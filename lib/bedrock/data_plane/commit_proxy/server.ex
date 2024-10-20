defmodule Bedrock.DataPlane.CommitProxy.Server do
  alias Bedrock.DataPlane.CommitProxy.State
  alias Bedrock.DataPlane.CommitProxy.Commit

  import Bedrock.DataPlane.Resolver, only: [resolve_transactions: 4]
  import Bedrock.DataPlane.Sequencer, only: [next_commit_version: 1]
  import Bedrock.DataPlane.CommitProxy.Commit.Mutations, only: [add_transaction: 5]

  use GenServer

  def child_spec(opts) do
    cluster = opts[:cluster] || raise "Missing :cluster option"

    transaction_system_layout =
      opts[:transaction_system_layout] || raise "Missing :transaction_system_layout option"

    epoch = opts[:epoch] || raise "Missing :epoch option"

    %{
      id: __MODULE__,
      start:
        {GenServer, :start_link,
         [
           __MODULE__,
           {cluster, transaction_system_layout, epoch}
         ]},
      restart: :temporary
    }
  end

  def init({cluster, transaction_system_layout, epoch}) do
    %State{
      cluster: cluster,
      transaction_system_layout: transaction_system_layout,
      epoch: epoch,
      max_latency_in_ms: 2,
      max_per_batch: 10
    }
    |> then(&{:ok, &1})
  end

  def handle_call({:commit, read_version, reads, writes}, from, %{commit: nil} = t) do
    {:ok, last_commit_version, next_commit_version} =
      t.transaction_system_layout.sequencer |> next_commit_version()

    commit =
      timestamp()
      |> Commit.new(last_commit_version, next_commit_version)
      |> add_transaction(from, read_version, reads, writes)

    %{t | commit: commit}
    |> noreply(timeout: 1)
  end

  def handle_call({:commit, read_version, reads, writes}, from, t) do
    commit =
      t.commit
      |> add_transaction(from, read_version, reads, writes)

    if should_finalize?(commit, t.max_latency_in_ms, t.max_per_batch) do
      %{t | commit: nil}
      |> noreply(continue: {:finalize, commit})
    else
      %{t | commit: commit}
      t |> noreply(timeout: 1)
    end
  end

  def handle_info(:timeout, t),
    do:
      %{t | commit: nil}
      |> noreply(continue: {:finalize, t.commit})

  def handle_continue({:finalize, commit}, t) do
    transactions_in_order = commit.buffer |> Enum.reverse()

    {:ok, aborted} =
      resolve_transactions(
        t.transaction_system_layout.resolver,
        commit.last_commit_version,
        commit.next_commit_version,
        transactions_in_order |> Enum.map(&prepare_transaction_for_resolution/1)
      )

    {oks, aborts, writes} = prepare_transactions_for_commit(transactions_in_order, aborted)

    :ok = reply_to_all_clients_with_aborted_transactions(aborts)

    next_commit_version = commit.next_commit_version
    combined_transaction = {{commit.last_commit_version, next_commit_version}, writes}

    :ok = write_transaction_to_logs(t, combined_transaction)
    :ok = reply_to_all_clients_with_successful_transactions(oks, next_commit_version)

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
  def reply_to_all_clients_with_successful_transactions(oks, next_commit_version),
    do: Enum.each(oks, &GenServer.reply(&1, {:ok, next_commit_version}))

  @spec prepare_transactions_for_commit(
          transactions :: [Commit.transaction_info()],
          aborts :: [integer()]
        ) :: {oks :: [pid()], aborts :: [pid()], writes :: %{Bedrock.key() => Bedrock.value()}}
  def prepare_transactions_for_commit(transactions, []) do
    transactions
    |> Enum.reduce({[], %{}}, fn {from, _, _, writes}, {pids, writes} ->
      {[from | pids], Map.merge(writes, writes)}
    end)
    |> then(fn {pids, writes} -> {pids, [], writes} end)
  end

  def prepare_transactions_for_commit(transactions, aborts) do
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

  defp timestamp, do: :erlang.monotonic_time(:millisecond)

  @spec should_finalize?(
          Commit.t(),
          max_latency_in_ms :: non_neg_integer(),
          max_per_batch :: pos_integer()
        ) :: boolean()
  defp should_finalize?(commit, _max_latency_in_ms, max_per_batch)
       when commit.n_transactions >= max_per_batch,
       do: true

  defp should_finalize?(commit, max_latency_in_ms, _max_per_batch) do
    commit.started_at + max_latency_in_ms < timestamp()
  end

  #  defp reply(t, result), do: {:ok, result, t}

  defp noreply(t, opts \\ [])
  defp noreply(t, continue: continue), do: {:noreply, t, {:continue, continue}}
  defp noreply(t, timeout: ms), do: {:noreply, t, ms}
  defp noreply(t, []), do: {:noreply, t}
end
