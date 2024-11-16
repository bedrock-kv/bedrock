defmodule Bedrock.DataPlane.CommitProxy.Server do
  alias Bedrock.DataPlane.CommitProxy.State
  alias Bedrock.DataPlane.CommitProxy.Batch
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.ControlPlane.Director
  alias Bedrock.Internal.Time

  import Bedrock.DataPlane.CommitProxy.Batching,
    only: [
      start_batch_if_needed: 1,
      add_transaction_to_batch: 3,
      apply_finalization_policy: 1
    ]

  import Bedrock.DataPlane.CommitProxy.Finalization, only: [finalize_batch: 2]

  import Bedrock.DataPlane.CommitProxy.Telemetry,
    only: [
      trace_metadata: 1,
      trace_commit_proxy_batch_started: 3,
      trace_commit_proxy_batch_finished: 4,
      trace_commit_proxy_batch_failed: 3
    ]

  use GenServer
  import Bedrock.Internal.GenServer.Replies

  @spec child_spec(
          opts :: [
            cluster: module(),
            director: pid(),
            transaction_system_layout: TransactionSystemLayout.t(),
            epoch: Bedrock.epoch(),
            max_latency_in_ms: non_neg_integer(),
            max_per_batch: pos_integer()
          ]
        ) :: Supervisor.child_spec()
  def child_spec(opts) do
    cluster = opts[:cluster] || raise "Missing :cluster option"
    director = opts[:director] || raise "Missing :director option"
    epoch = opts[:epoch] || raise "Missing :epoch option"
    max_latency_in_ms = opts[:max_latency_in_ms] || 1
    max_per_batch = opts[:max_per_batch] || 10

    %{
      id: __MODULE__,
      start:
        {GenServer, :start_link,
         [
           __MODULE__,
           {cluster, director, epoch, max_latency_in_ms, max_per_batch}
         ]},
      restart: :temporary
    }
  end

  def init({cluster, director, epoch, max_latency_in_ms, max_per_batch}) do
    trace_metadata(cluster: cluster, pid: self())

    %State{
      cluster: cluster,
      director: director,
      epoch: epoch,
      max_latency_in_ms: max_latency_in_ms,
      max_per_batch: max_per_batch
    }
    |> then(&{:ok, &1})
  end

  def terminate(_reason, _t) do
    :ok
  end

  # When a transaction is submitted, we check to see if we have a batch already
  # in progress. If we do, we add the transaction to the batch. If we don't, we
  # create a new batch and add the transaction to it. Once added, we check to
  # see if the batch meets the finalization policy. If it does, we finalize the
  # batch. If it doesn't, we wait for a short timeout to see if anything else
  # is submitted before re-considering finalizing the batch.
  def handle_call({:commit, transaction}, from, t) do
    t
    |> ask_for_transaction_system_layout_if_needed()
    |> start_batch_if_needed()
    |> add_transaction_to_batch(transaction, reply_fn(from))
    |> apply_finalization_policy()
    |> case do
      {t, nil} -> t |> noreply(timeout: 0)
      {t, batch} -> t |> noreply(continue: {:finalize, batch})
    end
  end

  # If we haven't seen any new commit requests come in for a few milliseconds,
  # we go ahead and finalize the batch -- No need to make everyone wait longer
  # than they absolutely need to!
  def handle_info(:timeout, %{batch: nil} = t),
    do: t |> noreply()

  def handle_info(:timeout, %{batch: batch} = t),
    do: %{t | batch: nil} |> noreply(continue: {:finalize, batch})

  def handle_continue({:finalize, batch}, t) do
    trace_commit_proxy_batch_started(batch.commit_version, length(batch.buffer), Time.now_in_ms())

    case :timer.tc(fn -> finalize_batch(batch, t.transaction_system_layout) end) do
      {n_usec, {:ok, n_aborts, n_oks}} ->
        trace_commit_proxy_batch_finished(batch.commit_version, n_aborts, n_oks, n_usec)
        t |> noreply()

      {n_usec, {:error, reason}} ->
        trace_commit_proxy_batch_failed(batch, reason, n_usec)
        t |> noreply()
    end
  end

  def ask_for_transaction_system_layout_if_needed(t) when t.transaction_system_layout != nil,
    do: t

  def ask_for_transaction_system_layout_if_needed(t) do
    t.director
    |> Director.fetch_transaction_system_layout(50)
    |> case do
      {:ok, transaction_system_layout} ->
        %{t | transaction_system_layout: transaction_system_layout}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @spec reply_fn(GenServer.from()) :: Batch.reply_fn()
  def reply_fn(from), do: &GenServer.reply(from, &1)
end
