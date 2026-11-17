defmodule Bedrock.Test.Chaos.Workload do
  @moduledoc """
  Drives the conservation workload against a peer cluster.

  The driver runs in the primary VM, but the transactions do not: each worker is
  a process on one of the peer nodes, reached with a single `:erpc` call that
  runs the worker's whole batch. Peers are where `Bedrock.Repo` can find a
  gateway, and a named function is the only thing the primary can hand a peer
  (see `Bedrock.Test.Chaos.Ledger`).

  Workers are spread round-robin over the cluster's nodes, so a run exercises
  every node's gateway rather than funnelling through one.

  `start/2` returns while the workload is still running, and `await!/2` collects
  it. That split exists for the fault-injection ticket: oracles have to be
  runnable *during* a run, against a cluster that is still taking writes, not
  only against the settled state afterwards.
  """

  alias Bedrock.Test.Chaos.Journal
  alias Bedrock.Test.Chaos.Ledger
  alias Bedrock.Test.Chaos.PeerCluster

  @type config :: %{
          accounts: pos_integer(),
          starting_balance: pos_integer(),
          workers: pos_integer(),
          ops_per_worker: pos_integer(),
          max_amount: pos_integer(),
          distribution: :uniform | :hotspot,
          hot_accounts: pos_integer(),
          hot_probability: float(),
          seed: integer(),
          journal_dir: Path.t(),
          worker_timeout_ms: timeout()
        }

  @type spec :: %{
          worker_id: non_neg_integer(),
          node: node(),
          config: config()
        }

  @type stats :: %{
          committed: non_neg_integer(),
          rejected: non_neg_integer(),
          failed: non_neg_integer(),
          failures: %{term() => pos_integer()}
        }

  @type t :: %__MODULE__{config: config(), nodes: [node()], tasks: [Task.t()]}
  @enforce_keys [:config, :nodes, :tasks]
  defstruct [:config, :nodes, :tasks]

  @doc """
  Seed the accounts and start the workers. Returns as soon as they are running.
  """
  @spec start(PeerCluster.t(), keyword()) :: t()
  def start(cluster, opts \\ []) do
    config = config(opts)
    nodes = PeerCluster.node_names(cluster)

    :ok = PeerCluster.call(hd(nodes), Ledger, :seed, [config.accounts, config.starting_balance])

    tasks =
      Enum.map(0..(config.workers - 1), fn worker_id ->
        node = Enum.at(nodes, rem(worker_id, length(nodes)))
        spec = %{worker_id: worker_id, node: node, config: config}

        Task.async(fn -> call_worker(spec) end)
      end)

    %__MODULE__{config: config, nodes: nodes, tasks: tasks}
  end

  @doc """
  Wait for every worker to finish and aggregate what they did.

  Raises if a worker did not return, or returned an error: in a run with no
  faults injected, a worker that cannot complete is the finding.
  """
  @spec await!(t(), timeout()) :: stats()
  def await!(%__MODULE__{tasks: tasks, config: config}, timeout \\ :infinity) do
    timeout = if timeout == :infinity, do: config.worker_timeout_ms + 30_000, else: timeout

    tasks
    |> Task.await_many(timeout)
    |> Enum.map(fn
      {:ok, stats} -> stats
      {:error, reason} -> raise "Workload worker failed: #{inspect(reason)}"
    end)
    |> Enum.reduce(empty_stats(), &merge_stats/2)
  end

  @doc """
  The total the ledger must always hold, given this run's configuration.
  """
  @spec expected_total(t()) :: integer()
  def expected_total(%__MODULE__{config: config}), do: config.accounts * config.starting_balance

  @doc """
  Run one worker's whole batch. Called on a peer node, never in the primary.
  """
  @spec run_worker(spec()) :: {:ok, stats()} | {:error, term()}
  def run_worker(%{worker_id: worker_id, config: config}) do
    # Per-worker seeding keeps a run reproducible in `:seed` while still giving
    # each worker its own stream of choices.
    :rand.seed(:exsss, {config.seed, worker_id, 0})

    journal = Journal.open!(config.journal_dir, worker_id)

    try do
      {:ok, Enum.reduce(1..config.ops_per_worker, empty_stats(), &run_op(worker_id, &1, config, journal, &2))}
    after
      Journal.close(journal)
    end
  end

  defp call_worker(%{node: node, config: config} = spec) do
    PeerCluster.call(node, __MODULE__, :run_worker, [spec], config.worker_timeout_ms)
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  defp run_op(worker_id, seq, config, journal, stats) do
    id = {worker_id, seq}
    {from, to} = pick_pair(config)
    amount = :rand.uniform(config.max_amount)

    case attempt(id, from, to, amount) do
      # `:already_applied` means an earlier attempt of this same transfer did
      # commit and the client only found out on the retry. The commit is real,
      # so it is journalled like any other ack.
      result when result == :ok or result == {:error, :already_applied} ->
        journal_ack!(journal, id, from, to, amount, result)
        %{stats | committed: stats.committed + 1}

      {:error, :insufficient_funds} ->
        %{stats | rejected: stats.rejected + 1}

      {:error, reason} ->
        %{stats | failed: stats.failed + 1, failures: Map.update(stats.failures, reason, 1, &(&1 + 1))}
    end
  end

  defp attempt(id, from, to, amount) do
    Ledger.transfer(id, from, to, amount)
  rescue
    exception -> {:error, {:raised, Exception.message(exception)}}
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  # The journal entry is written *after* the ack and before the worker does
  # anything else, so every entry in it is a promise the cluster has already
  # made.
  defp journal_ack!(journal, id, from, to, amount, result) do
    Journal.append!(journal, %{
      transfer: id,
      from: from,
      to: to,
      amount: amount,
      result: result,
      node: node(),
      at: System.system_time(:microsecond)
    })
  end

  defp pick_pair(config) do
    from = pick_account(config)
    {from, pick_other_account(config, from)}
  end

  defp pick_other_account(config, from) do
    case pick_account(config) do
      ^from -> pick_other_account(config, from)
      other -> other
    end
  end

  defp pick_account(%{distribution: :uniform, accounts: accounts}), do: :rand.uniform(accounts) - 1

  defp pick_account(%{distribution: :hotspot} = config) do
    if :rand.uniform() < config.hot_probability do
      :rand.uniform(config.hot_accounts) - 1
    else
      :rand.uniform(config.accounts) - 1
    end
  end

  defp config(opts) do
    accounts = Keyword.get(opts, :accounts, 24)

    %{
      accounts: accounts,
      starting_balance: Keyword.get(opts, :starting_balance, 10_000),
      workers: Keyword.get(opts, :workers, 6),
      ops_per_worker: Keyword.get(opts, :ops_per_worker, 40),
      max_amount: Keyword.get(opts, :max_amount, 100),
      distribution: Keyword.get(opts, :distribution, :uniform),
      hot_accounts: Keyword.get(opts, :hot_accounts, max(div(accounts, 8), 2)),
      hot_probability: Keyword.get(opts, :hot_probability, 0.5),
      seed: Keyword.get(opts, :seed, 1),
      journal_dir: Keyword.fetch!(opts, :journal_dir),
      worker_timeout_ms: Keyword.get(opts, :worker_timeout_ms, 120_000)
    }
  end

  defp empty_stats, do: %{committed: 0, rejected: 0, failed: 0, failures: %{}}

  defp merge_stats(stats, acc) do
    %{
      committed: acc.committed + stats.committed,
      rejected: acc.rejected + stats.rejected,
      failed: acc.failed + stats.failed,
      failures: Map.merge(acc.failures, stats.failures, fn _reason, a, b -> a + b end)
    }
  end
end
