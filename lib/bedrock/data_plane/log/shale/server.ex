defmodule Bedrock.DataPlane.Log.Shale.Server do
  @moduledoc false
  use GenServer

  import Bedrock.DataPlane.Log.Shale.ColdStarting, only: [reload_segments_at_path: 1]
  import Bedrock.DataPlane.Log.Shale.Facts, only: [info: 2]
  import Bedrock.DataPlane.Log.Shale.Locking, only: [lock_for_recovery: 3]

  import Bedrock.DataPlane.Log.Shale.LongPulls,
    only: [
      process_expired_deadlines_for_waiting_pullers: 2,
      try_to_add_to_waiting_pullers: 5,
      determine_timeout_for_next_puller_deadline: 2,
      notify_waiting_pullers: 3
    ]

  import Bedrock.DataPlane.Log.Shale.Pulling, only: [pull: 3]
  import Bedrock.DataPlane.Log.Shale.Pushing, only: [push: 4]
  import Bedrock.DataPlane.Log.Shale.Recovery, only: [recover_from: 4]
  import Bedrock.DataPlane.Log.Telemetry
  import Bedrock.Internal.GenServer.Replies

  alias Bedrock.Cluster
  alias Bedrock.DataPlane.Demux
  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.Log.Shale.Segment
  alias Bedrock.DataPlane.Log.Shale.SegmentRecycler
  alias Bedrock.DataPlane.Log.Shale.State
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version

  require Logger

  # Retry backoff configuration for resource exhaustion
  @initial_retry_delay_ms 1_000
  @max_retry_delay_ms 30_000
  @max_retry_attempts 10

  @doc false
  @spec child_spec(
          opts :: [
            cluster: Cluster.t(),
            otp_name: atom(),
            id: Log.id(),
            foreman: pid(),
            path: Path.t(),
            object_storage: module(),
            start_unlocked: boolean()
          ]
        ) :: Supervisor.child_spec()
  def child_spec(opts) do
    cluster = opts[:cluster] || raise "Missing :cluster option"
    otp_name = opts[:otp_name] || raise "Missing :otp_name option"
    id = Keyword.fetch!(opts, :id) || raise "Missing :id option"
    foreman = Keyword.fetch!(opts, :foreman)
    path = Keyword.fetch!(opts, :path)
    object_storage = Keyword.fetch!(opts, :object_storage)
    start_unlocked = Keyword.get(opts, :start_unlocked, false)

    %{
      id: {__MODULE__, id},
      start:
        {GenServer, :start_link,
         [
           __MODULE__,
           {
             cluster,
             otp_name,
             id,
             foreman,
             path,
             object_storage,
             start_unlocked
           },
           [name: otp_name]
         ]}
    }
  end

  @impl true
  @spec init({module(), atom(), Log.id(), pid(), Path.t(), module(), boolean()}) ::
          {:ok, State.t(), {:continue, :initialization}}
  def init({cluster, otp_name, id, foreman, path, object_storage, start_unlocked}) do
    initial_mode = if start_unlocked, do: :running, else: :locked

    {:ok,
     %State{
       path: path,
       cluster: cluster,
       mode: initial_mode,
       init_state: {:retrying, 1},
       id: id,
       otp_name: otp_name,
       foreman: foreman,
       object_storage: object_storage,
       oldest_version: Version.zero(),
       last_version: Version.zero()
     }, {:continue, :initialization}}
  end

  @impl true
  @spec handle_continue(
          :initialization
          | {:notify_waiting_pullers, Bedrock.version(), Transaction.encoded()}
          | :check_for_expired_pullers
          | :wait_for_next_puller_deadline,
          State.t()
        ) ::
          {:noreply, State.t()} | {:noreply, State.t(), timeout()}
  def handle_continue(:initialization, t) do
    trace_metadata(%{cluster: t.cluster, id: t.id, otp_name: t.otp_name})
    trace_started()

    case do_initialization(t) do
      {:ok, t} ->
        t
        |> Map.put(:init_state, :initialized)
        |> noreply()

      {:error, {:resource_exhausted, reason}} ->
        handle_resource_exhaustion(t, reason)
    end
  end

  @impl true
  def handle_continue({:notify_waiting_pullers, version, transaction}, t) do
    t
    |> Map.update!(
      :waiting_pullers,
      &notify_waiting_pullers(&1, version, transaction)
    )
    |> noreply(continue: :check_for_expired_pullers)
  end

  @impl true
  def handle_continue(:check_for_expired_pullers, t) do
    t
    |> Map.update!(
      :waiting_pullers,
      &process_expired_deadlines_for_waiting_pullers(&1, monotonic_now())
    )
    |> noreply(continue: :wait_for_next_puller_deadline)
  end

  @impl true
  def handle_continue(:wait_for_next_puller_deadline, t) do
    t
    |> Map.get(:waiting_pullers)
    |> determine_timeout_for_next_puller_deadline(monotonic_now())
    |> case do
      nil -> noreply(t)
      timeout -> {:noreply, t, timeout}
    end
  end

  @impl true
  @spec handle_info(:timeout | :retry_initialization | {:min_durable_version, Bedrock.version()}, State.t()) ::
          {:noreply, State.t()} | {:noreply, State.t(), {:continue, :check_for_expired_pullers}}
  def handle_info(:timeout, t), do: noreply(t, continue: :check_for_expired_pullers)

  def handle_info(:retry_initialization, t) do
    case do_initialization(t) do
      {:ok, t} ->
        Logger.info("Shale initialization succeeded after retry",
          log_id: t.id,
          path: t.path
        )

        t
        |> Map.put(:init_state, :initialized)
        |> noreply()

      {:error, {:resource_exhausted, reason}} ->
        handle_resource_exhaustion(t, reason)
    end
  end

  def handle_info({:min_durable_version, version}, t) do
    noreply(%{t | min_durable_version: version})
  end

  @impl true
  @spec handle_call(
          {:info, [atom()]}
          | {:lock_for_recovery, Bedrock.epoch()}
          | {:recover_from, pid(), Bedrock.version(), Bedrock.version()}
          | {:push, binary(), Bedrock.version()}
          | {:pull, Bedrock.version(), keyword()}
          | :ping,
          GenServer.from(),
          State.t()
        ) ::
          {:reply, term(), State.t()} | {:noreply, State.t(), {:continue, atom()}}
  def handle_call({:info, fact_names}, _, t), do: t |> info(fact_names) |> then(&reply(t, &1))

  @impl true
  def handle_call({:lock_for_recovery, epoch}, {director, _}, t) do
    trace_lock_for_recovery(epoch)

    with {:ok, t} <- lock_for_recovery(t, epoch, director),
         {:ok, info} <- info(t, Log.recovery_info()) do
      reply(t, {:ok, self(), info})
    else
      error -> reply(t, error)
    end
  end

  @impl true
  def handle_call({:recover_from, source_log, first_version, last_version}, {_director, _}, t) do
    trace_recover_from(source_log, first_version, last_version)

    case recover_from(t, source_log, first_version, last_version) do
      {:ok, t} -> reply(t, {:ok, self()})
      {:error, reason} -> reply(t, {:error, {:failed_to_recover, reason}})
    end
  end

  @impl true
  def handle_call({:push, transaction_bytes, expected_version}, from, %State{} = t) do
    with {:ok, transaction} <- Transaction.validate(transaction_bytes),
         :ok <- validate_has_shard_index(transaction),
         {:ok, t} <- push(t, expected_version, transaction, ack_fn(from)) do
      # Push to Demux for distribution to ShardServers (async)
      Demux.Server.push(t.demux, expected_version, transaction)
      noreply(t, continue: {:notify_waiting_pullers, expected_version, transaction})
    else
      {:wait, t} -> noreply(t, continue: :check_for_expired_pullers)
      {:error, _reason} = error -> reply(t, error, continue: :check_for_expired_pullers)
    end
  end

  @impl true
  def handle_call({:pull, from_version, opts}, from, t) do
    trace_pull_transactions(from_version, opts)

    case pull(t, from_version, opts) do
      {:ok, t, transactions} ->
        reply(t, {:ok, transactions})

      {:waiting_for, from_version} ->
        t.waiting_pullers
        |> try_to_add_to_waiting_pullers(
          monotonic_now(),
          reply_to_fn(from),
          from_version,
          opts
        )
        |> case do
          {:error, _reason} = error ->
            reply(t, error, continue: :check_for_expired_pullers)

          {:ok, waiting_pullers} ->
            t
            |> Map.put(:waiting_pullers, waiting_pullers)
            |> noreply(continue: :check_for_expired_pullers)
        end

      {:error, _reason} = error ->
        reply(t, error)
    end
  end

  @impl true
  def handle_call(:ping, _from, t), do: reply(t, :pong)

  @impl true
  def handle_call({:get_shard_server, shard_id}, _from, t) do
    result = Demux.Server.get_shard_server(t.demux, shard_id)
    reply(t, result)
  end

  defp validate_has_shard_index(transaction) do
    case Transaction.shard_index(transaction) do
      {:ok, [_ | _]} ->
        # Non-empty shard_index is valid
        :ok

      {:ok, []} ->
        # Empty shard_index is valid for empty/heartbeat transactions
        # that advance the Lamport clock without mutations
        :ok

      {:ok, nil} ->
        # No shard_index section at all is valid for empty/heartbeat transactions
        :ok

      {:error, _} ->
        {:error, :missing_shard_index}
    end
  end

  @spec check_running(term()) :: {:error, :unavailable}
  def check_running(_t), do: {:error, :unavailable}

  @spec ack_fn(GenServer.from()) :: (:ok | {:error, term()} -> :ok)
  def ack_fn(from), do: fn result -> GenServer.reply(from, result) end

  @spec reply_to_fn(GenServer.from()) :: (term() -> :ok)
  def reply_to_fn(from), do: &GenServer.reply(from, &1)

  @spec monotonic_now() :: integer()
  def monotonic_now, do: :erlang.monotonic_time(:millisecond)

  # Initialization with resource exhaustion detection
  defp do_initialization(t) do
    with {:ok, recycler_pid} <- start_segment_recycler(t.path),
         {:ok, {oldest_version, last_version, active_segment, segments}} <-
           load_or_create_segments(t.path, recycler_pid),
         {:ok, demux} <- start_demux(t) do
      {:ok,
       t
       |> Map.put(:oldest_version, oldest_version)
       |> Map.put(:last_version, last_version)
       |> Map.put(:active_segment, active_segment)
       |> Map.put(:segments, segments)
       |> Map.put(:segment_recycler, recycler_pid)
       |> Map.put(:demux, demux)}
    end
  end

  defp start_segment_recycler(path) do
    case SegmentRecycler.start_link(
           path: path,
           min_available: 2,
           max_available: 3,
           segment_size: 64 * 1024 * 1024
         ) do
      {:ok, pid} ->
        {:ok, pid}

      {:error, reason} when reason in [:emfile, :enfile, :enomem] ->
        {:error, {:resource_exhausted, reason}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp load_or_create_segments(path, recycler_pid) do
    case reload_segments_at_path(path) do
      {:ok, []} ->
        case Segment.allocate_from_recycler(recycler_pid, path, Version.zero()) do
          {:ok, new_segment} ->
            {:ok, {Version.zero(), Version.zero(), new_segment, []}}

          {:error, :allocation_failed} ->
            # Segment allocation can fail due to resource exhaustion (emfile/enfile)
            # or because the recycler is unavailable. Treat as recoverable.
            {:error, {:resource_exhausted, :allocation_failed}}
        end

      {:ok, [active_segment | segments]} ->
        active_segment = Segment.ensure_transactions_are_loaded(active_segment)
        last_version = Segment.last_version(active_segment)
        oldest_version = Enum.min([active_segment.min_version | Enum.map(segments, & &1.min_version)])
        {:ok, {oldest_version, last_version, active_segment, segments}}

      {:error, {:unable_to_list_segments, reason}} when reason in [:emfile, :enfile, :enomem] ->
        {:error, {:resource_exhausted, reason}}

      {:error, {:unable_to_list_segments, reason}} ->
        raise "Unable to read WAL segment files from path: #{path}. Error: #{inspect(reason)}. Check directory permissions and filesystem health."
    end
  end

  defp start_demux(t) do
    case Demux.Server.start_link(
           cluster: t.cluster,
           object_storage: t.object_storage,
           log: self()
         ) do
      {:ok, demux} -> {:ok, demux}
      {:error, reason} -> {:error, reason}
    end
  end

  defp handle_resource_exhaustion(t, reason) do
    attempt =
      case t.init_state do
        {:retrying, n} -> n
        :initialized -> 1
      end

    if attempt >= @max_retry_attempts do
      Logger.error("Shale initialization failed: resource exhaustion after #{attempt} attempts",
        log_id: t.id,
        path: t.path,
        reason: reason
      )

      raise "Shale initialization failed after #{attempt} attempts due to #{reason}. Check system resource limits (file descriptors, memory)."
    end

    delay = calculate_retry_delay(attempt)

    Logger.warning("Shale initialization delayed due to resource exhaustion",
      log_id: t.id,
      path: t.path,
      reason: reason,
      attempt: attempt,
      retry_in_ms: delay
    )

    Process.send_after(self(), :retry_initialization, delay)

    t
    |> Map.put(:init_state, {:retrying, attempt + 1})
    |> noreply()
  end

  defp calculate_retry_delay(attempt) do
    # Exponential backoff: 1s, 2s, 4s, 8s, ... capped at 30s
    delay = trunc(@initial_retry_delay_ms * :math.pow(2, attempt - 1))
    min(delay, @max_retry_delay_ms)
  end
end
