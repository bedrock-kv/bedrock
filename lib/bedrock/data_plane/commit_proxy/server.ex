defmodule Bedrock.DataPlane.CommitProxy.Server do
  alias Bedrock.DataPlane.CommitProxy.State

  import Bedrock.DataPlane.Resolver, only: [resolve_transactions: 4]

  import Bedrock.DataPlane.CommitProxy.Batching,
    only: [
      start_batch_if_needed: 1,
      add_transaction_to_batch: 3,
      apply_finalization_policy: 1
    ]

  use GenServer

  def child_spec(opts) do
    cluster = opts[:cluster] || raise "Missing :cluster option"

    transaction_system_layout =
      opts[:transaction_system_layout] || raise "Missing :transaction_system_layout option"

    epoch = opts[:epoch] || raise "Missing :epoch option"
    max_latency_in_ms = opts[:max_latency_in_ms] || 2
    max_per_batch = opts[:max_per_batch] || 10

    %{
      id: __MODULE__,
      start:
        {GenServer, :start_link,
         [
           __MODULE__,
           {cluster, transaction_system_layout, epoch, max_latency_in_ms, max_per_batch}
         ]},
      restart: :temporary
    }
  end

  def init({cluster, transaction_system_layout, epoch, max_latency_in_ms, max_per_batch}) do
    %State{
      cluster: cluster,
      transaction_system_layout: transaction_system_layout,
      epoch: epoch,
      max_latency_in_ms: max_latency_in_ms,
      max_per_batch: max_per_batch
    }
    |> then(&{:ok, &1})
  end

  # When a transaction is submitted, we check to see if we have a batch already
  # in progress. If we do, we add the transaction to the batch. If we don't, we
  # create a new batch and add the transaction to it. Once added, we check to
  # see if the batch meets the finalization policy. If it does, we finalize the
  # batch. If it doesn't, we wait for a short timeout to see if anything else
  # is submitted before re-considering finalizing the batch.
  def handle_call({:commit, transaction}, from, t) do
    t
    |> start_batch_if_needed()
    |> add_transaction_to_batch(transaction, from)
    |> apply_finalization_policy()
    |> case do
      {t, nil} -> t |> noreply(timeout: 0)
      {t, batch} -> t |> noreply(timeout: 0, continue: {:finalize, batch})
    end
  end

  # If we haven't seen any new commit requests come in for a few milliseconds,
  # we go ahead and finalize the batch -- No need to make everyone wait longer
  # than they absolutely need to!
  def handle_info(:timeout, %{batch: nil} = t),
    do: t |> noreply(continue: {:finalize, t.batch})

  def handle_continue({:finalize, batch}, t) do
    transactions_in_order = batch.buffer |> Enum.reverse()

    commit_version = batch.commit_version

    {:ok, aborted} =
      resolve_transactions(
        t.transaction_system_layout.resolver,
        batch.last_commit_version,
        commit_version,
        transactions_in_order |> Enum.map(&prepare_transaction_for_resolution/1)
      )

    {oks, aborts, writes} = prepare_transactions_for_log(transactions_in_order, aborted)

    :ok = reply_to_all_clients_with_aborted_transactions(aborts)

    combined_transaction = {{batch.last_commit_version, commit_version}, writes}

    :ok = write_transaction_to_logs(t, combined_transaction)
    :ok = reply_to_all_clients_with_successful_transactions(oks, commit_version)

    t |> noreply()
  end

  def write_transaction_to_logs(_t, _transaction) do
    :ok
  end

  @spec reply_to_all_clients_with_aborted_transactions([GenServer.from()]) :: :ok
  def reply_to_all_clients_with_aborted_transactions(aborts),
    do: Enum.each(aborts, &GenServer.reply(&1, {:error, :aborted}))

  @spec reply_to_all_clients_with_successful_transactions([GenServer.from()], Bedrock.version()) ::
          :ok
  def reply_to_all_clients_with_successful_transactions(oks, commit_version),
    do: Enum.each(oks, &GenServer.reply(&1, {:ok, commit_version}))

  @spec prepare_transactions_for_log(
          transactions :: [Bedrock.transaction()],
          aborts :: [integer()]
        ) :: {oks :: [pid()], aborts :: [pid()], writes :: %{Bedrock.key() => Bedrock.value()}}
  def prepare_transactions_for_log(transactions, []) do
    transactions
    |> Enum.reduce({[], %{}}, fn {from, _, _, writes}, {pids, writes} ->
      {[from | pids], Map.merge(writes, writes)}
    end)
    |> then(fn {pids, writes} -> {pids, [], writes} end)
  end

  def prepare_transactions_for_log(transactions, aborts) do
    aborted_set = MapSet.new(aborts)

    transactions
    |> Enum.with_index()
    |> Enum.reduce({[], [], %{}}, fn
      {{from, _, _, writes}, idx}, {oks, aborts, all_writes} ->
        if MapSet.member?(aborted_set, idx) do
          {oks, [from | aborts], writes}
        else
          {[from | oks], aborts, Map.merge(all_writes, writes)}
        end
    end)
  end

  def prepare_transaction_for_resolution({_from, read_version, reads, writes}),
    do: {read_version, reads, writes |> Map.keys()}

  defp noreply(t, opts \\ [])
  defp noreply(t, continue: continue, timeout: ms), do: {:noreply, t, ms, {:continue, continue}}
  defp noreply(t, continue: continue), do: {:noreply, t, {:continue, continue}}
  defp noreply(t, timeout: ms), do: {:noreply, t, ms}
  defp noreply(t, []), do: {:noreply, t}
end
