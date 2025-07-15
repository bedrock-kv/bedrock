defmodule Bedrock.DataPlane.CommitProxy.Server do
  alias Bedrock.DataPlane.CommitProxy.State
  alias Bedrock.DataPlane.CommitProxy.Batch
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

  require Logger

  @spec child_spec(
          opts :: [
            cluster: module(),
            director: pid(),
            epoch: Bedrock.epoch(),
            lock_token: binary(),
            max_latency_in_ms: non_neg_integer(),
            max_per_batch: pos_integer()
          ]
        ) :: Supervisor.child_spec()
  def child_spec(opts) do
    cluster = opts[:cluster] || raise "Missing :cluster option"
    director = opts[:director] || raise "Missing :director option"
    epoch = opts[:epoch] || raise "Missing :epoch option"
    lock_token = opts[:lock_token] || raise "Missing :lock_token option"
    max_latency_in_ms = opts[:max_latency_in_ms] || 1
    max_per_batch = opts[:max_per_batch] || 10

    %{
      id: __MODULE__,
      start:
        {GenServer, :start_link,
         [
           __MODULE__,
           {cluster, director, epoch, max_latency_in_ms, max_per_batch, lock_token}
         ]},
      restart: :temporary
    }
  end

  def init({cluster, director, epoch, max_latency_in_ms, max_per_batch, lock_token}) do
    trace_metadata(%{cluster: cluster, pid: self()})

    %State{
      cluster: cluster,
      director: director,
      epoch: epoch,
      max_latency_in_ms: max_latency_in_ms,
      max_per_batch: max_per_batch,
      lock_token: lock_token
    }
    |> then(&{:ok, &1})
  end

  def terminate(_reason, _t) do
    :ok
  end

  def handle_call(
        {:recover_from, lock_token, transaction_system_layout},
        _from,
        %{mode: :locked} = t
      ) do
    if lock_token == t.lock_token do
      %{t | transaction_system_layout: transaction_system_layout, mode: :running}
      |> reply(:ok)
    else
      t |> reply({:error, :unauthorized})
    end
  end

  # When a transaction is submitted, we check to see if we have a batch already
  # in progress. If we do, we add the transaction to the batch. If we don't, we
  # create a new batch and add the transaction to it. Once added, we check to
  # see if the batch meets the finalization policy. If it does, we finalize the
  # batch. If it doesn't, we wait for a short timeout to see if anything else
  # is submitted before re-considering finalizing the batch.
  def handle_call({:commit, transaction}, from, %{mode: :running} = t) do
    case t.transaction_system_layout do
      nil ->
        t |> reply({:error, :no_transaction_system_layout})

      _layout ->
        t
        |> start_batch_if_needed()
        |> add_transaction_to_batch(transaction, reply_fn(from))
        |> apply_finalization_policy()
        |> case do
          {t, nil} -> t |> noreply(timeout: 0)
          {t, batch} -> t |> noreply(continue: {:finalize, batch})
        end
    end
  end

  # Reject commit calls when locked (except from director with system layout)
  def handle_call({:commit, _transaction}, _from, %{mode: :locked} = t) do
    t |> reply({:error, :locked})
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

      {n_usec, {:error, {:log_failures, errors}}} ->
        trace_commit_proxy_batch_failed(batch, {:log_failures, errors}, n_usec)
        # Exit to trigger recovery since logs are failing
        exit({:log_failures, errors})

      {n_usec, {:error, {:insufficient_acknowledgments, count, required, errors}}} ->
        trace_commit_proxy_batch_failed(
          batch,
          {:insufficient_acknowledgments, count, required, errors},
          n_usec
        )

        # Exit to trigger recovery since not all logs are available
        exit({:insufficient_acknowledgments, count, required, errors})

      {n_usec, {:error, {:resolver_unavailable, reason}}} ->
        trace_commit_proxy_batch_failed(batch, {:resolver_unavailable, reason}, n_usec)
        # Exit to trigger recovery since resolver is unavailable
        exit({:resolver_unavailable, reason})

      {n_usec, {:error, {:storage_team_coverage_error, key}}} ->
        trace_commit_proxy_batch_failed(batch, {:storage_team_coverage_error, key}, n_usec)
        # Exit to trigger recovery since storage team configuration is invalid
        exit({:storage_team_coverage_error, key})

      {n_usec, {:error, reason}} ->
        trace_commit_proxy_batch_failed(batch, reason, n_usec)
        t |> noreply()
    end
  end

  @spec reply_fn(GenServer.from()) :: Batch.reply_fn()
  def reply_fn(from), do: &GenServer.reply(from, &1)

  @doc """
  Updates the transaction system layout if the transaction contains system layout information.

  This function extracts transaction system layout from system transactions that contain
  the layout key and updates the commit proxy's state accordingly.
  """
  @spec maybe_update_layout_from_transaction(State.t(), Bedrock.transaction()) :: State.t()
  def maybe_update_layout_from_transaction(state, {_reads, writes}) when is_map(writes) do
    layout_key = "\xff/system/transaction_system_layout"

    case Map.get(writes, layout_key) do
      nil ->
        state

      encoded_layout when is_binary(encoded_layout) ->
        try do
          layout = :erlang.binary_to_term(encoded_layout)
          %{state | transaction_system_layout: layout}
        rescue
          _ ->
            # If decoding fails, return state unchanged
            state
        end

      _ ->
        state
    end
  end

  def maybe_update_layout_from_transaction(state, _transaction) do
    # For non-standard transaction formats, return state unchanged
    state
  end
end
