defmodule Bedrock.DataPlane.CommitProxy.Server do
  @moduledoc """
  GenServer implementation of the Commit Proxy.

  ## Overview

  The Commit Proxy batches transaction requests from clients to optimize throughput while
  maintaining strict consistency guarantees. It coordinates with resolvers for conflict
  detection and logs for durable persistence.

  ## Lifecycle

  1. **Initialization**: Starts in `:locked` mode, waiting for recovery completion
  2. **Recovery**: Director calls `recover_from/3` to provide transaction system layout and unlock
  3. **Transaction Processing**: Accepts `:commit` calls, batches transactions, and finalizes
  4. **Empty Transaction Timeout**: Creates empty transactions during quiet periods to advance read versions

  ## Batching Strategy

  - **Size-based**: Batches finalize when reaching `max_per_batch` transactions
  - **Time-based**: Batches finalize after `max_latency_in_ms` milliseconds
  - **Immediate**: Single transactions may bypass batching for low-latency processing

  ## Timeout Mechanisms

  - **Fast timeout (0ms)**: Allows GenServer to process any queued `:commit` messages before
    finalizing the current batch, ensuring optimal batching efficiency
  - **Empty transaction timeout**: Creates empty `{nil, %{}}` transactions during quiet periods
    to keep read versions advancing and provide system health checking

  ## Error Handling

  Uses fail-fast recovery model where unrecoverable errors (sequencer unavailable, log failures)
  trigger process exit and Director-coordinated cluster recovery.
  """

  use GenServer

  import Bedrock.DataPlane.CommitProxy.Batching,
    only: [
      start_batch_if_needed: 1,
      apply_finalization_policy: 1,
      add_transaction_to_batch: 4,
      single_transaction_batch: 2
    ]

  import Bedrock.DataPlane.CommitProxy.Finalization, only: [finalize_batch: 3]

  import Bedrock.DataPlane.CommitProxy.Telemetry,
    only: [trace_metadata: 1]

  import Bedrock.Internal.GenServer.Replies

  alias Bedrock.Cluster
  alias Bedrock.DataPlane.CommitProxy.Batch
  # ConflictSharding moved to Batching module
  alias Bedrock.DataPlane.CommitProxy.LayoutOptimization
  alias Bedrock.DataPlane.CommitProxy.State
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.Internal.Time

  @spec child_spec(
          opts :: [
            cluster: Cluster.t(),
            director: pid(),
            epoch: Bedrock.epoch(),
            lock_token: Bedrock.lock_token(),
            instance: non_neg_integer(),
            max_latency_in_ms: non_neg_integer(),
            max_per_batch: pos_integer(),
            empty_transaction_timeout_ms: non_neg_integer()
          ]
        ) :: Supervisor.child_spec() | no_return()
  def child_spec(opts) do
    cluster = opts[:cluster] || raise "Missing :cluster option"
    director = opts[:director] || raise "Missing :director option"
    epoch = opts[:epoch] || raise "Missing :epoch option"
    lock_token = opts[:lock_token] || raise "Missing :lock_token option"
    instance = opts[:instance] || raise "Missing :instance option"
    max_latency_in_ms = opts[:max_latency_in_ms] || 4
    max_per_batch = opts[:max_per_batch] || 32
    empty_transaction_timeout_ms = opts[:empty_transaction_timeout_ms] || 1_000

    %{
      id: {__MODULE__, cluster, epoch, instance},
      start:
        {GenServer, :start_link,
         [
           __MODULE__,
           {cluster, director, epoch, max_latency_in_ms, max_per_batch, empty_transaction_timeout_ms, lock_token}
         ]},
      restart: :temporary
    }
  end

  @impl true
  @spec init({module(), pid(), Bedrock.epoch(), non_neg_integer(), pos_integer(), non_neg_integer(), binary()}) ::
          {:ok, State.t(), timeout()}
  def init({cluster, director, epoch, max_latency_in_ms, max_per_batch, empty_transaction_timeout_ms, lock_token}) do
    # Monitor the Director - if it dies, this commit proxy should terminate
    Process.monitor(director)

    trace_metadata(%{cluster: cluster, pid: self()})

    then(
      %State{
        cluster: cluster,
        director: director,
        epoch: epoch,
        max_latency_in_ms: max_latency_in_ms,
        max_per_batch: max_per_batch,
        empty_transaction_timeout_ms: empty_transaction_timeout_ms,
        lock_token: lock_token
      },
      &{:ok, &1, empty_transaction_timeout_ms}
    )
  end

  @impl true
  @spec terminate(term(), State.t()) :: :ok
  def terminate(_reason, %State{} = t) do
    abort_current_batch(t)
    :ok
  end

  @impl true
  @spec handle_call(
          {:recover_from, binary(), map()} | {:commit, Bedrock.transaction()},
          GenServer.from(),
          State.t()
        ) ::
          {:reply, term(), State.t()} | {:noreply, State.t(), timeout() | {:continue, term()}}
  def handle_call({:recover_from, lock_token, transaction_system_layout}, _from, %{mode: :locked} = t) do
    if lock_token == t.lock_token do
      precomputed_layout = LayoutOptimization.precompute_from_layout(transaction_system_layout)

      reply(
        %{
          t
          | transaction_system_layout: transaction_system_layout,
            precomputed_layout: precomputed_layout,
            mode: :running
        },
        :ok
      )
    else
      reply(t, {:error, :unauthorized})
    end
  end

  def handle_call({:commit, transaction}, _from, %{mode: :running, transaction_system_layout: nil} = t)
      when is_binary(transaction) do
    reply(t, {:error, :no_transaction_system_layout})
  end

  def handle_call({:commit, transaction}, from, %{mode: :running} = t) when is_binary(transaction) do
    case start_batch_if_needed(t) do
      {:error, reason} ->
        GenServer.reply(from, {:error, :abort})
        exit(reason)

      updated_t ->
        updated_t
        |> add_transaction_to_batch(transaction, reply_fn(from), nil)
        |> apply_finalization_policy()
        |> case do
          {t, nil} ->
            # Use zero timeout to process any pending messages first
            noreply(t, timeout: 0)

          {t, batch} ->
            # Finalize asynchronously and reset for next batch
            finalize_batch_async(batch, t.transaction_system_layout, t.epoch, t.precomputed_layout)
            maybe_set_empty_transaction_timeout(t)
        end
    end
  end

  def handle_call({:commit, _transaction}, _from, %{mode: :locked} = t), do: reply(t, {:error, :locked})

  @impl true
  @spec handle_info(:timeout, State.t()) :: {:noreply, State.t(), timeout()}
  def handle_info(:timeout, %{batch: nil, mode: :running} = t) do
    empty_transaction = Transaction.empty_transaction()

    case single_transaction_batch(t, empty_transaction) do
      {:ok, batch} ->
        # Send empty batch asynchronously
        finalize_batch_async(batch, t.transaction_system_layout, t.epoch, t.precomputed_layout)
        maybe_set_empty_transaction_timeout(t)

      {:error, :sequencer_unavailable} ->
        exit({:sequencer_unavailable, :timeout_empty_transaction})
    end
  end

  def handle_info(:timeout, %{batch: nil} = t) do
    noreply(t, timeout: t.empty_transaction_timeout_ms)
  end

  def handle_info(:timeout, %{batch: batch} = t) do
    # Timeout reached - finalize current batch asynchronously
    finalize_batch_async(batch, t.transaction_system_layout, t.epoch, t.precomputed_layout)
    maybe_set_empty_transaction_timeout(%{t | batch: nil})
  end

  def handle_info({:DOWN, _ref, :process, director_pid, _reason}, %{director: director_pid} = t) do
    # Director has died - this commit proxy should terminate gracefully
    {:stop, :normal, t}
  end

  def handle_info(_msg, t) do
    {:noreply, t}
  end

  # Asynchronous batch finalization - creates its own resolver tasks
  defp finalize_batch_async(batch, transaction_system_layout, epoch, precomputed_layout) do
    Task.start_link(fn ->
      case finalize_batch(batch, transaction_system_layout, epoch: epoch, precomputed: precomputed_layout) do
        {:ok, _n_aborts, _n_oks} ->
          :ok

        {:error, reason} ->
          exit(reason)
      end
    end)
  end

  @spec reply_fn(GenServer.from()) :: Batch.reply_fn()
  def reply_fn(from), do: &GenServer.reply(from, &1)

  # Moved to Batching module to avoid duplication

  @spec maybe_set_empty_transaction_timeout(State.t()) :: {:noreply, State.t(), timeout()}
  defp maybe_set_empty_transaction_timeout(%{mode: :running} = t),
    do: noreply(t, timeout: t.empty_transaction_timeout_ms)

  defp maybe_set_empty_transaction_timeout(t), do: noreply(t)

  @spec abort_current_batch(State.t()) :: :ok
  defp abort_current_batch(%{batch: nil}), do: :ok

  defp abort_current_batch(%{batch: batch}) do
    batch
    |> Batch.all_callers()
    |> Enum.each(fn reply_fn -> reply_fn.({:error, :abort}) end)
  end
end
