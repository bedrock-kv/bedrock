defmodule Bedrock.Test.Chaos.Oracles do
  @moduledoc """
  The checks that decide whether a run is a pass.

  Three of them, each answering a different question about the same run:

    * **Conservation** — the sum of all balances at a snapshot version equals the
      total the run started with. Transfers move money, so a total that has
      moved means a write was lost, half-applied, or read at the wrong version.

    * **Reconciliation** — the balances at a snapshot version are exactly what
      you get by replaying the receipts visible *at that same version* against
      the starting balances. Conservation is blind to a transfer that debited
      and credited the wrong pair, or to a receipt that became visible without
      its balance changes; this is not.

    * **Acked-commit durability** — every commit the client was told had
      committed is readable afterwards. The journal is the claim, the database
      is the evidence.

  Every check takes a node and reads through that node, so they can be run at
  any moment against a cluster that is still under load, not just against a
  settled one. That is what the fault-injection ticket needs: a check that only
  works after the workload has drained cannot catch a violation that heals.

  `check_acked_durability/2` reads the journal *before* it reads the database,
  which is what makes it safe to run mid-flight: an entry that appears in the
  journal after the read is one whose commit also happened after the read, so a
  concurrent worker can never make the check spuriously fail.
  """

  alias Bedrock.Test.Chaos.Journal
  alias Bedrock.Test.Chaos.Ledger
  alias Bedrock.Test.Chaos.PeerCluster
  alias Bedrock.Test.Chaos.Workload

  @type failure :: {atom(), map()}

  @doc """
  Run every oracle against `node`, for the given workload run.
  """
  @spec check_all(node(), Workload.t()) :: :ok | {:error, [failure()]}
  def check_all(node, %Workload{config: config} = run) do
    durability = check_acked_durability(node, config.journal_dir)
    snapshot = snapshot!(node)

    [
      {:acked_commit_durability, durability},
      {:conservation, check_conservation(snapshot, Workload.expected_total(run))},
      {:reconciliation, check_reconciliation(snapshot, config)}
    ]
    |> Enum.flat_map(fn
      {_name, :ok} -> []
      {name, {:error, detail}} -> [{name, detail}]
    end)
    |> case do
      [] -> :ok
      failures -> {:error, failures}
    end
  end

  @doc """
  Read the balances and receipts at one version, through `node`.
  """
  @spec snapshot!(node()) :: Ledger.snapshot()
  def snapshot!(node) do
    case PeerCluster.call(node, Ledger, :snapshot, []) do
      {:ok, snapshot} -> snapshot
      {:error, reason} -> raise "Could not read a ledger snapshot from #{node}: #{inspect(reason)}"
    end
  end

  @doc """
  The total of all balances is the total the run started with.
  """
  @spec check_conservation(Ledger.snapshot(), integer()) :: :ok | {:error, map()}
  def check_conservation(%{balances: balances}, expected_total) do
    case balances |> Map.values() |> Enum.sum() do
      ^expected_total -> :ok
      total -> {:error, %{expected_total: expected_total, actual_total: total, accounts: map_size(balances)}}
    end
  end

  @doc """
  The balances are exactly the replay of the receipts visible at the same version.
  """
  @spec check_reconciliation(Ledger.snapshot(), Workload.config()) :: :ok | {:error, map()}
  def check_reconciliation(%{balances: balances, receipts: receipts}, config) do
    expected =
      Enum.reduce(receipts, starting_balances(config), fn {_id, {from, to, amount}}, acc ->
        acc |> Map.update(from, -amount, &(&1 - amount)) |> Map.update(to, amount, &(&1 + amount))
      end)

    case diff_balances(expected, balances) do
      [] -> :ok
      differences -> {:error, %{receipts: map_size(receipts), differences: Map.new(differences)}}
    end
  end

  @doc """
  Everything the journal says was acked is in the database, unchanged.

  Also reports commits the database has that the journal does not. Mid-run those
  are just workers that have not journalled yet; once a run has drained they are
  commits the client never learned about, which is worth knowing even though it
  is not by itself a durability violation.
  """
  @spec check_acked_durability(node(), Path.t()) :: :ok | {:error, map()}
  def check_acked_durability(node, journal_dir) do
    %{entries: entries, torn: torn} = Journal.read_all(journal_dir)

    receipts =
      case PeerCluster.call(node, Ledger, :receipts, []) do
        {:ok, receipts} -> receipts
        {:error, reason} -> raise "Could not read receipts from #{node}: #{inspect(reason)}"
      end

    {missing, mismatched} = compare_acks(entries, receipts)
    unjournalled = Map.drop(receipts, Enum.map(entries, & &1.transfer))

    if missing == [] and mismatched == [] do
      :ok
    else
      {:error,
       %{
         acked: length(entries),
         missing: missing,
         mismatched: mismatched,
         torn_journal_entries: torn,
         unjournalled_commits: map_size(unjournalled)
       }}
    end
  end

  defp compare_acks(entries, receipts) do
    Enum.reduce(entries, {[], []}, fn entry, {missing, mismatched} ->
      case Map.fetch(receipts, entry.transfer) do
        :error ->
          {[entry.transfer | missing], mismatched}

        {:ok, {from, to, amount}} when {from, to, amount} == {entry.from, entry.to, entry.amount} ->
          {missing, mismatched}

        {:ok, receipt} ->
          {missing, [{entry.transfer, %{journalled: entry, stored: receipt}} | mismatched]}
      end
    end)
  end

  defp starting_balances(%{accounts: accounts, starting_balance: starting_balance}),
    do: Map.new(0..(accounts - 1), &{&1, starting_balance})

  defp diff_balances(expected, actual) do
    expected
    |> Map.keys()
    |> Enum.concat(Map.keys(actual))
    |> Enum.uniq()
    |> Enum.flat_map(fn account ->
      case {Map.get(expected, account), Map.get(actual, account)} do
        {same, same} -> []
        {want, got} -> [{account, %{expected: want, actual: got}}]
      end
    end)
  end
end
