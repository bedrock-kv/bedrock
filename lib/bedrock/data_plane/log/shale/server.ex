defmodule Bedrock.DataPlane.Log.Shale.Server do
  @moduledoc false
  use GenServer

  import Bedrock.DataPlane.Log.Shale.ColdStarting, only: [reload_segments_at_path: 1]
  import Bedrock.DataPlane.Log.Shale.Facts, only: [info: 2]
  import Bedrock.DataPlane.Log.Shale.Locking, only: [lock_for_recovery: 2]

  import Bedrock.DataPlane.Log.Shale.LongPulls,
    only: [
      process_expired_deadlines_for_waiting_pullers: 2,
      try_to_add_to_waiting_pullers: 5,
      determine_timeout_for_next_puller_deadline: 2,
      notify_waiting_pullers: 3
    ]

  import Bedrock.DataPlane.Log.Shale.Pulling, only: [pull: 3]
  import Bedrock.DataPlane.Log.Shale.Pushing, only: [push: 5]

  import Bedrock.DataPlane.Log.Shale.Recovery,
    only: [apply_replay_page: 3, prepare_replay_state: 2, recover_from: 5, stream_transactions_from_sources: 6]

  import Bedrock.DataPlane.Log.Telemetry
  import Bedrock.Internal.GenServer.Replies

  alias Bedrock.Cluster
  alias Bedrock.DataPlane.Demux
  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.Log.Shale.DemuxControl
  alias Bedrock.DataPlane.Log.Shale.Locking
  alias Bedrock.DataPlane.Log.Shale.Recovery
  alias Bedrock.DataPlane.Log.Shale.Segment
  alias Bedrock.DataPlane.Log.Shale.SegmentRecycler
  alias Bedrock.DataPlane.Log.Shale.State
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version
  alias Bedrock.Service.Foreman
  alias Bedrock.Service.RecoveryAuthority
  alias Bedrock.Service.RecoveryControl

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
            reject_pushes_above_lag_us: non_neg_integer()
          ]
        ) :: Supervisor.child_spec()
  def child_spec(opts) do
    if Keyword.has_key?(opts, :start_unlocked),
      do: raise(ArgumentError, "start_unlocked bypasses recovery authority")

    cluster = opts[:cluster] || raise "Missing :cluster option"
    otp_name = opts[:otp_name] || raise "Missing :otp_name option"
    id = Keyword.fetch!(opts, :id) || raise "Missing :id option"
    foreman = Keyword.fetch!(opts, :foreman)
    path = Keyword.fetch!(opts, :path)
    object_storage = Keyword.fetch!(opts, :object_storage)
    reject_pushes_above_lag_us = Keyword.get(opts, :reject_pushes_above_lag_us)
    cut_interval_us = opts |> Keyword.get(:params, %{}) |> cut_interval_us_from_params()

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
             reject_pushes_above_lag_us,
             cut_interval_us
           },
           [name: otp_name]
         ]}
    }
  end

  # The cut interval rides the manifest, which is what makes it survive a
  # restart: the Foreman rebuilds a crashed worker from its manifest
  # alone, so an interval held only in State would revert to the default
  # and the log would resume rolling on boundaries the chunks it already
  # wrote were not cut on. Anything but a positive integer is "unset" —
  # manifest params are JSON, so a string or a zero is a config mistake,
  # not an instruction.
  @spec cut_interval_us_from_params(map()) :: pos_integer() | nil
  defp cut_interval_us_from_params(%{"cut_interval_us" => us}) when is_integer(us) and us > 0, do: us
  defp cut_interval_us_from_params(_params), do: nil

  @impl true
  @spec init({module(), atom(), Log.id(), pid(), Path.t(), module(), non_neg_integer() | nil, pos_integer() | nil}) ::
          {:ok, State.t(), {:continue, :initialization}} | {:stop, term()}
  def init({cluster, otp_name, id, foreman, path, object_storage, reject_pushes_above_lag_us, cut_interval_us}) do
    case validate_startup_control(path, cluster, id) do
      {:ok, control} ->
        initial_mode = if control.phase == :running, do: :running, else: :locked

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
           reject_pushes_above_lag_us: reject_pushes_above_lag_us,
           cut_interval_us: cut_interval_us,
           recovery_control: control,
           recovery_authority: control.authority,
           available_after: Version.zero(),
           oldest_version: Version.zero(),
           last_version: Version.zero()
         }, {:continue, :initialization}}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl true
  def terminate(_reason, %State{} = t) do
    Locking.cancel_replay(t.replay_operation, :not_lock_owner)
    :ok
  end

  defp validate_startup_control(path, cluster, id) do
    with {:ok, control} <- RecoveryControl.validate_prepared(path, cluster, id, Bedrock.DataPlane.Log.Shale),
         :ok <- validate_running_wal(path, control) do
      {:ok, control}
    end
  end

  defp validate_running_wal(path, %{phase: phase, last_inclusive: last, wal_identity: expected})
       when phase in [:replay_complete, :running] do
    allow_suffix? = phase == :running

    case RecoveryControl.wal_identity(path, last, allow_suffix: allow_suffix?) do
      {:ok, ^expected} -> :ok
      {:ok, _} -> {:error, {:recovery_authority, :wal_identity_mismatch}}
      {:error, reason} -> {:error, {:recovery_authority, {:wal_identity_unavailable, reason}}}
    end
  end

  defp validate_running_wal(_path, _control), do: :ok

  @impl true
  @spec handle_continue(
          :initialization
          | {:notify_appended, [{Bedrock.version(), Transaction.encoded()}]}
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
  def handle_continue({:notify_appended, events}, t) do
    # Wake pullers from the authoritative append events themselves — each
    # event carries its own version and bytes, in predecessor-chain order.
    t
    |> Map.update!(:waiting_pullers, fn waiting_pullers ->
      Enum.reduce(events, waiting_pullers, fn {version, transaction}, acc ->
        notify_waiting_pullers(acc, version, transaction)
      end)
    end)
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
  @spec handle_info(:timeout | :retry_initialization | {:min_durable_version, pid(), Bedrock.version()}, State.t()) ::
          {:noreply, State.t()} | {:noreply, State.t(), {:continue, :check_for_expired_pullers}}
  def handle_info(:timeout, t), do: noreply(t, continue: :check_for_expired_pullers)

  def handle_info({:replay_fetched, operation_id, result}, %{replay_operation: %{id: operation_id} = op} = t) do
    Process.demonitor(op.monitor, [:flush])
    Process.demonitor(op.guardian_monitor, [:flush])
    Process.demonitor(op.owner_monitor, [:flush])
    t = %{t | replay_operation: nil}

    with :ok <- validate_owner(t, op.authority),
         {:ok, t} <- finish_streamed_replay(t, result, op.last_inclusive),
         {:ok, t} <- complete_replay(t) do
      Enum.each(op.waiters, &GenServer.reply(&1, {:ok, self()}))
      noreply(t)
    else
      {:error, reason, failed_t} ->
        Enum.each(op.waiters, &GenServer.reply(&1, {:error, {:failed_to_recover, reason}}))
        noreply(%{failed_t | mode: :locked})

      {:error, reason} ->
        Enum.each(op.waiters, &GenServer.reply(&1, {:error, reason}))
        noreply(t)
    end
  end

  def handle_info({:replay_fetched, _operation_id, _result}, t), do: noreply(t)

  def handle_info(
        {:replay_page, operation_id, authority, transactions, task_pid},
        %{replay_operation: %{id: operation_id, pid: task_pid} = op} = t
      ) do
    with :ok <- validate_owner(t, authority),
         true <- RecoveryAuthority.compare(authority, op.authority) == :same || {:error, :not_lock_owner},
         t = if(op.started?, do: t, else: prepare_replay_state(t, op.replay_after)),
         {:ok, t} <- apply_replay_page(t, transactions, op.last_inclusive),
         :ok <- validate_owner(t, authority) do
      send(task_pid, {:replay_page_ack, operation_id, :ok})
      noreply(%{t | replay_operation: %{op | started?: true}})
    else
      {:error, reason, failed_t} ->
        send(task_pid, {:replay_page_ack, operation_id, {:error, reason}})
        noreply(%{failed_t | replay_operation: op})

      {:error, reason} ->
        send(task_pid, {:replay_page_ack, operation_id, {:error, reason}})
        noreply(t)
    end
  end

  def handle_info({:replay_page, operation_id, _authority, _transactions, task_pid}, t) do
    send(task_pid, {:replay_page_ack, operation_id, {:error, :not_lock_owner}})
    noreply(t)
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, %{replay_operation: %{monitor: ref} = op} = t) do
    Process.demonitor(op.owner_monitor, [:flush])
    Process.demonitor(op.guardian_monitor, [:flush])
    Enum.each(op.waiters, &GenServer.reply(&1, {:error, {:failed_to_recover, {:replay_task_exit, reason}}}))
    noreply(%{t | replay_operation: nil, mode: :locked})
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, %{replay_operation: %{owner_monitor: ref} = op} = t) do
    Locking.cancel_replay(op, :not_lock_owner)
    noreply(%{t | replay_operation: nil, mode: :locked})
  end

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

  def handle_info({:min_durable_version, demux, version}, %{mode: :running, demux: demux} = t) do
    t
    |> advance_min_durable_version(version)
    |> noreply()
  end

  # Watermarks from a stale Demux incarnation (pid mismatch after a recovery
  # reset) or outside :running are dropped; confirmations are re-derived from
  # fresh flushes, so losing them is always safe.
  def handle_info({:min_durable_version, _demux, _version}, t), do: noreply(t)

  # A newly durable layout, relayed by this node's foreman. Log topology is
  # epoch-constant, so membership is decided by the push itself: if the
  # layout omits us, we are displaced — our WAL was already replayed into
  # the new generation before the layout became durable. FDB's TLog
  # computes the same verdict from ServerDBInfo (isDisplaced /
  # 'DBInfoDoesNotContain') and throws worker_removed on itself; nobody
  # else decides. A layout may judge every worker it had the chance to
  # include (pushed epoch >= ours — the locking phase locks old-layout
  # logs into the judging epoch, so the displacing push carries OUR
  # epoch); only a push older than our lock is off-limits: that is an
  # in-flight recovery's past, and absence there is not a death sentence.
  def handle_info({:tsl_updated, %{epoch: pushed_epoch, logs: logs}}, t) do
    if displaced?(t, pushed_epoch, logs) do
      Logger.info("Bedrock log #{t.id}: displaced by epoch #{pushed_epoch} layout; retiring")
      Foreman.worker_retired(t.foreman, t.id)
      {:stop, {:shutdown, :displaced}, t}
    else
      noreply(t)
    end
  end

  def handle_info({:tsl_updated, _}, t), do: noreply(t)

  # Displacement verdict: the pushed layout had the chance to include us
  # (its epoch is at or past the one we were locked into; nil means never
  # locked — a cold-boot resurrection any completed layout may judge) and
  # its log set does not name us.
  @spec displaced?(State.t(), Bedrock.epoch(), %{Log.id() => term()}) :: boolean()
  defp displaced?(%{epoch: my_epoch, id: id}, pushed_epoch, logs) do
    may_judge? = is_nil(my_epoch) or pushed_epoch >= my_epoch
    may_judge? and not Map.has_key?(logs, id)
  end

  @impl true
  @spec handle_call(
          {:info, [atom()]}
          | {:lock_for_recovery, Bedrock.epoch()}
          | {:recover_from, [pid()], Bedrock.version(), Bedrock.version()}
          | {:push, binary(), Bedrock.version()}
          | {:push, binary(), Bedrock.version(), Bedrock.version() | nil}
          | {:pull, Bedrock.version(), keyword()}
          | :ping,
          GenServer.from(),
          State.t()
        ) ::
          {:reply, term(), State.t()} | {:noreply, State.t(), {:continue, atom()}}
  def handle_call({:info, fact_names}, _, t), do: t |> info(fact_names) |> then(&reply(t, &1))

  @impl true
  def handle_call({:lock_for_recovery, authority}, _from, t) do
    with {:ok, authority} <- RecoveryAuthority.new(authority),
         {:ok, t} <- lock_for_recovery(t, authority),
         {:ok, info} <- info(t, Log.recovery_info()) do
      trace_lock_for_recovery(authority.generation)
      reply(t, {:ok, self(), info})
    else
      error -> reply(t, error)
    end
  end

  def handle_call({:push, authority, transaction_bytes, expected_version, known_committed_version}, from, %State{} = t) do
    with {:ok, authority} <- RecoveryAuthority.new(authority),
         :ok <- validate_owner(t, authority),
         {:ok, transaction} <- Transaction.validate(transaction_bytes),
         :ok <- validate_has_shard_index(transaction) do
      t
      |> push(RecoveryAuthority.external(authority), expected_version, transaction, from)
      |> apply_push_transition(known_committed_version)
    else
      {:error, _reason} = error -> reply(t, error, continue: :check_for_expired_pullers)
    end
  end

  @impl true
  def handle_call({:recover_from, authority, source_logs, replay_after, last_inclusive}, from, t) do
    trace_recover_from(source_logs, replay_after, last_inclusive)

    with {:ok, authority} <- RecoveryAuthority.new(authority),
         :ok <- validate_owner(t, authority) do
      case existing_replay(t, authority, replay_after, last_inclusive, from) do
        {:joined, t} ->
          noreply(t)

        :none ->
          case begin_replay(t, authority, replay_after, last_inclusive) do
            {:continue, t} -> start_replay(t, authority, source_logs, replay_after, last_inclusive, from)
            {:already_complete, t} -> reply(t, {:ok, self()})
            {:error, reason} -> reply(t, {:error, reason})
          end

        {:error, reason} ->
          reply(t, {:error, reason})
      end
    else
      {:error, reason} -> reply(t, {:error, reason})
    end
  end

  def handle_call({:recover_from, _sources, _after, _last}, _from, t),
    do: reply(t, {:error, :invalid_recovery_authority})

  def handle_call({:unlock_after_recovery, authority}, _from, t) do
    with {:ok, authority} <- RecoveryAuthority.new(authority),
         :ok <- validate_owner(t, authority),
         {:ok, t} <- unlock_replay(t) do
      reply(t, :ok)
    else
      {:error, reason} -> reply(t, {:error, reason})
    end
  end

  @impl true
  def handle_call({:push, _transaction_bytes, _expected_version, _known_committed_version}, _from, t),
    do: reply(t, {:error, :invalid_recovery_authority})

  # Pushes without a known-committed watermark (older callers, tests): the
  # WAL still appends, but cuts stay gated until a watermark arrives.
  @impl true
  def handle_call({:push, transaction_bytes, expected_version}, from, %State{} = t),
    do: handle_call({:push, transaction_bytes, expected_version, nil}, from, t)

  @impl true
  def handle_call({:pull, from_version, opts}, from, t) do
    trace_pull_transactions(from_version, opts)

    case validate_recovery_pull(t, opts) do
      :ok -> do_pull(t, from_version, opts, from)
      {:error, reason} -> reply(t, {:error, reason})
    end
  end

  @impl true
  def handle_call(:ping, _from, t), do: reply(t, :pong)

  @impl true
  def handle_call({:get_shard_server, shard_id}, _from, t) do
    result = Demux.Server.get_shard_server(t.demux, shard_id)
    reply(t, result)
  end

  defp do_pull(t, from_version, opts, from) do
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

  defp validate_recovery_pull(t, opts) do
    if opts[:recovery] do
      with {:ok, authority} <- RecoveryAuthority.new(opts[:recovery_authority]) do
        validate_owner(t, authority)
      end
    else
      :ok
    end
  end

  defp validate_owner(%{recovery_authority: nil}, _authority), do: {:error, :not_lock_owner}

  defp validate_owner(%{recovery_authority: current}, authority) do
    if RecoveryAuthority.compare(authority, current) == :same, do: :ok, else: {:error, :not_lock_owner}
  end

  defp begin_replay(_t, _authority, replay_after, last_inclusive) when replay_after > last_inclusive,
    do: {:error, :invalid_version_range}

  defp begin_replay(%{recovery_control: control} = t, authority, replay_after, last_inclusive) do
    same_range = control.replay_after == replay_after and control.last_inclusive == last_inclusive

    begin_replay_phase(control.phase, same_range, t, authority, replay_after, last_inclusive)
  end

  defp begin_replay_phase(phase, true, t, _authority, _replay_after, _last_inclusive)
       when phase in [:replay_complete, :running] do
    case validate_running_wal(t.path, t.recovery_control) do
      :ok -> {:already_complete, t}
      {:error, {:recovery_authority, reason}} -> {:error, reason}
    end
  end

  defp begin_replay_phase(:replay_started, false, _t, _authority, _replay_after, _last_inclusive),
    do: {:error, :replay_range_mismatch}

  defp begin_replay_phase(:replay_started, true, t, authority, replay_after, last_inclusive) do
    if Recovery.replay_complete_on_disk?(t.path, replay_after, last_inclusive) do
      case complete_replay(t) do
        {:ok, t} -> {:already_complete, t}
        {:error, reason, _t} -> {:error, reason}
      end
    else
      persist_replay_started(t, authority, replay_after, last_inclusive)
    end
  end

  defp begin_replay_phase(:locked, _same_range, t, authority, replay_after, last_inclusive),
    do: persist_replay_started(t, authority, replay_after, last_inclusive)

  defp begin_replay_phase(_phase, _same_range, _t, _authority, _replay_after, _last_inclusive),
    do: {:error, :lock_required}

  defp persist_replay_started(t, authority, replay_after, last_inclusive) do
    record = RecoveryControl.replay_started(t.recovery_control, authority, replay_after, last_inclusive)

    case write_control(t.path, record) do
      :ok -> {:continue, %{t | recovery_control: record}}
      {:error, reason} -> {:error, {:unable_to_persist_recovery_state, reason}}
    end
  end

  defp complete_replay(t) do
    with {:ok, wal_identity} <- RecoveryControl.wal_identity(t.path, t.last_version, allow_suffix: false),
         record = RecoveryControl.replay_complete(t.recovery_control, wal_identity),
         :ok <- write_control(t.path, record) do
      {:ok, %{t | recovery_control: record, mode: :locked}}
    else
      {:error, reason} -> {:error, {:unable_to_persist_replay_complete, reason}, t}
    end
  end

  defp existing_replay(%{replay_operation: nil}, _authority, _after, _last, _from), do: :none

  defp existing_replay(%{replay_operation: op} = t, authority, replay_after, last_inclusive, from) do
    if RecoveryAuthority.compare(authority, op.authority) == :same and op.replay_after == replay_after and
         op.last_inclusive == last_inclusive do
      {:joined, %{t | replay_operation: %{op | waiters: op.waiters ++ [from]}}}
    else
      {:error, :replay_in_progress}
    end
  end

  defp start_replay(t, authority, sources, replay_after, last_inclusive, _from) when replay_after == last_inclusive do
    case recover_from(t, authority, sources, replay_after, last_inclusive) do
      {:ok, t} ->
        case complete_replay(t) do
          {:ok, t} -> reply(t, {:ok, self()})
          {:error, reason, t} -> reply(t, {:error, {:failed_to_recover, reason}})
        end

      {:error, reason, t} ->
        reply(t, {:error, {:failed_to_recover, reason}})
    end
  end

  defp start_replay(t, authority, sources, replay_after, last_inclusive, from) do
    operation_id = make_ref()
    server = self()

    {pid, monitor} =
      spawn_monitor(fn ->
        result =
          stream_transactions_from_sources(server, operation_id, authority, sources, replay_after, last_inclusive)

        send(server, {:replay_fetched, operation_id, result})
      end)

    {guardian, guardian_monitor} = spawn_monitor(fn -> guard_replay_lifetime(server, pid) end)

    {owner, _tag} = from
    owner_monitor = Process.monitor(owner)

    operation = %{
      id: operation_id,
      authority: RecoveryAuthority.external(authority),
      replay_after: replay_after,
      last_inclusive: last_inclusive,
      waiters: [from],
      pid: pid,
      monitor: monitor,
      guardian: guardian,
      guardian_monitor: guardian_monitor,
      owner_monitor: owner_monitor,
      started?: false
    }

    noreply(%{t | replay_operation: operation})
  end

  defp guard_replay_lifetime(server, replay_pid) do
    server_ref = Process.monitor(server)
    replay_ref = Process.monitor(replay_pid)

    receive do
      {:DOWN, ^server_ref, :process, ^server, _reason} ->
        if Process.alive?(replay_pid), do: Process.exit(replay_pid, :kill)

      {:DOWN, ^replay_ref, :process, ^replay_pid, _reason} ->
        :ok
    end
  end

  defp finish_streamed_replay(%{last_version: last} = t, :ok, last), do: {:ok, %{t | mode: :locked}}
  defp finish_streamed_replay(t, :ok, last), do: {:error, {:incomplete_replay, t.last_version, last}, t}
  defp finish_streamed_replay(t, {:error, reason}, _last), do: {:error, reason, t}

  defp unlock_replay(%{recovery_control: %{phase: :running}} = t), do: {:ok, %{t | mode: :running}}

  defp unlock_replay(%{recovery_control: %{phase: :replay_complete} = control} = t) do
    record = RecoveryControl.running(control)

    with :ok <- validate_running_wal(t.path, control),
         :ok <- write_control(t.path, record) do
      {:ok, %{t | recovery_control: record, mode: :running}}
    else
      {:error, {:recovery_authority, reason}} -> {:error, reason}
      {:error, reason} -> {:error, {:unable_to_persist_recovery_state, reason}}
    end
  end

  defp unlock_replay(_t), do: {:error, :replay_not_complete}

  defp write_control(path, record) do
    case RecoveryControl.write(path, record) do
      {:error, {:post_publish_sync_failed, reason}} -> exit({:recovery_authority_durability_uncertain, reason})
      result -> result
    end
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

  # The Server is the sole owner of live effects, applied exactly once
  # from the transition Pushing returned: every caller reply is issued
  # here (tokens are the callers' `from`s), every append event forwards
  # to the Demux in predecessor-chain order, and puller notification runs
  # from those same authoritative events. The binaries in the events are
  # the exact ref-counted inputs written to the WAL, so forwarding
  # neither slices nor copies them.
  defp apply_push_transition(
         %{state: t, appended: appended, replies: replies, parked?: parked?},
         known_committed_version
       ) do
    Enum.each(replies, fn {from, result} -> GenServer.reply(from, result) end)
    forward_appended_transactions(t.demux, appended, known_committed_version)

    # A parked push carries commit evidence even though nothing appended:
    # the known-committed version still advances the Demux's cut gate.
    if parked?, do: advance_demux_kcv(t.demux, known_committed_version)

    if appended == [] do
      noreply(t, continue: :check_for_expired_pullers)
    else
      noreply(t, continue: {:notify_appended, appended})
    end
  end

  defp forward_appended_transactions(nil, _events, _known_committed_version), do: :ok

  defp forward_appended_transactions(demux, events, known_committed_version) do
    Enum.each(events, fn {version, transaction} ->
      Demux.Server.push(demux, version, transaction, known_committed_version)
    end)
  end

  defp advance_demux_kcv(_demux, nil), do: :ok
  defp advance_demux_kcv(nil, _known_committed_version), do: :ok

  defp advance_demux_kcv(demux, known_committed_version) do
    Demux.Server.advance_known_committed_version(demux, known_committed_version)
  end

  @spec check_running(term()) :: {:error, :unavailable}
  def check_running(_t), do: {:error, :unavailable}

  @spec reply_to_fn(GenServer.from()) :: (term() -> :ok)
  def reply_to_fn(from), do: &GenServer.reply(from, &1)

  @spec monotonic_now() :: integer()
  def monotonic_now, do: :erlang.monotonic_time(:millisecond)

  # Transactional cold start: validate the existing WAL set FIRST — it
  # needs no started resources — then acquire the recycler and demux.
  # Every attempt owns what it starts: a failure after acquisition tears
  # the acquired resources down synchronously before returning, so a
  # retrying server never accumulates stray recyclers or demuxes.
  defp do_initialization(t) do
    with {:ok, wal_snapshot} <- load_or_create_segments(t),
         {:ok, recycler_pid} <- start_segment_recycler(t.path),
         {:ok, demux} <- start_demux_or_release_recycler(t, recycler_pid) do
      {available_after, oldest_version, last_version, active_segment, segments} = wal_snapshot

      {:ok,
       t
       |> Map.put(:available_after, available_after)
       |> Map.put(:oldest_version, oldest_version)
       |> Map.put(:last_version, last_version)
       |> Map.put(:active_segment, active_segment)
       |> Map.put(:segments, segments)
       |> Map.put(:segment_recycler, recycler_pid)
       |> Map.put(:demux, demux)}
    end
  end

  # Normalize demux acquisition into one transactional step for the caller:
  # success transfers both resources into State; failure releases the staged
  # recycler before returning the classified error.
  defp start_demux_or_release_recycler(t, recycler_pid) do
    case start_demux(t) do
      {:ok, demux} ->
        {:ok, demux}

      {:error, reason} ->
        :ok = stop_owned_recycler(recycler_pid)
        classify_resource_error(reason)
    end
  end

  defp start_segment_recycler(path) do
    case SegmentRecycler.start_link(
           path: path,
           min_available: 2,
           max_available: 3,
           segment_size: 64 * 1024 * 1024
         ) do
      {:ok, pid} -> {:ok, pid}
      {:error, reason} -> classify_resource_error(reason)
    end
  end

  defp classify_resource_error(reason) when reason in [:emfile, :enfile, :enomem],
    do: {:error, {:resource_exhausted, reason}}

  defp classify_resource_error(reason), do: {:error, reason}

  defp stop_owned_recycler(pid) do
    Process.unlink(pid)
    GenServer.stop(pid, :shutdown, 5_000)
    :ok
  catch
    :exit, _ -> :ok
  end

  defp load_or_create_segments(t) do
    loader = t.segment_loader || (&reload_segments_at_path/1)

    case loader.(t.path) do
      {:ok, []} ->
        {:ok, {Version.zero(), Version.zero(), Version.zero(), nil, []}}

      {:ok, [active_segment | segments]} ->
        active_segment = Segment.ensure_transactions_are_loaded(active_segment)
        last_version = Segment.last_version(active_segment) || active_segment.previous_version
        available_after = List.last([active_segment | segments]).previous_version
        oldest_version = determine_oldest_transaction_version([active_segment | segments], available_after)
        {:ok, {available_after, oldest_version, last_version, active_segment, segments}}

      {:error, {:wal_io, _path, reason}} when reason in [:emfile, :enfile, :enomem] ->
        # Transient resource exhaustion: the caller retries with backoff.
        {:error, {:resource_exhausted, reason}}

      {:error, {:wal_io, path, reason}} ->
        raise "WAL I/O failure at #{path}: #{inspect(reason)}. Check directory permissions and filesystem health."

      {:error, {:wal_format, path, reason}} ->
        raise "Unable to establish WAL replay cursor for #{path}: #{reason}"
    end
  end

  # The optional WAL backpressure hard limit is enforced in Pushing,
  # against each transaction's own prospective commit version — including
  # entries drained from the pending queue.

  defp start_demux(t), do: DemuxControl.start(t)

  defp advance_min_durable_version(t, incoming_version) do
    # The floor can never pass the WAL tip, whatever a confirmation claims.
    incoming_version = min(incoming_version, t.last_version)

    min_durable_version =
      case t.min_durable_version do
        nil -> incoming_version
        existing when incoming_version > existing -> incoming_version
        existing -> existing
      end

    if min_durable_version == t.min_durable_version do
      t
    else
      t
      |> Map.put(:min_durable_version, min_durable_version)
      |> trim_durable_segments()
    end
  end

  defp trim_durable_segments(%{segment_recycler: nil} = t), do: t
  defp trim_durable_segments(%{segments: []} = t), do: t
  defp trim_durable_segments(%{min_durable_version: nil} = t), do: t

  # Only reachable in :running mode (the watermark handler is the sole
  # caller and it gates on mode). Materializers never hold the WAL back:
  # they catch up from object-storage chunks and snapshots, so the trim
  # floor is object-storage confirmation alone (the Demux watermark).
  defp trim_durable_segments(t) do
    trim_floor = t.min_durable_version
    segments_oldest_first = Enum.reverse(t.segments)

    {segments_to_trim_oldest_first, remaining_segments_oldest_first} =
      Enum.split_while(segments_oldest_first, &segment_fully_durable?(&1, trim_floor))

    Enum.each(segments_to_trim_oldest_first, fn segment ->
      :ok = Segment.return_to_recycler(segment, t.segment_recycler)
    end)

    remaining_segments = Enum.reverse(remaining_segments_oldest_first)
    lag_us = Version.distance(t.last_version, trim_floor)

    trace_trim(
      trim_floor,
      t.last_version,
      lag_us,
      length(segments_to_trim_oldest_first),
      length(remaining_segments) + 1
    )

    available_after = determine_available_after(t.active_segment, remaining_segments)

    updated =
      t
      |> Map.put(:segments, remaining_segments)
      |> Map.put(:available_after, available_after)
      |> Map.put(
        :oldest_version,
        determine_oldest_transaction_version([t.active_segment | remaining_segments], available_after)
      )

    if segments_to_trim_oldest_first == [], do: updated, else: refresh_running_control(updated)
  end

  defp refresh_running_control(%{recovery_control: %{phase: :running}, recovery_authority: authority} = t) do
    started = RecoveryControl.replay_started(t.recovery_control, authority, t.available_after, t.last_version)
    {:ok, identity} = RecoveryControl.wal_identity(t.path, t.last_version)
    record = started |> RecoveryControl.replay_complete(identity) |> RecoveryControl.running()

    case RecoveryControl.write(t.path, record) do
      :ok -> %{t | recovery_control: record}
      {:error, {:post_publish_sync_failed, reason}} -> exit({:recovery_authority_durability_uncertain, reason})
      {:error, reason} -> exit({:recovery_authority_checkpoint_failed, reason})
    end
  end

  defp refresh_running_control(t), do: t

  defp segment_fully_durable?(segment, min_durable_version) do
    loaded_segment = Segment.ensure_transactions_are_loaded(segment)

    case Segment.last_version(loaded_segment) do
      nil -> true
      last_version -> last_version <= min_durable_version
    end
  end

  defp determine_available_after(nil, []), do: Version.zero()

  defp determine_available_after(active_segment, segments) do
    [active_segment | segments]
    |> Enum.reject(&is_nil/1)
    |> List.last()
    |> Map.fetch!(:previous_version)
  end

  defp determine_oldest_transaction_version(segments, available_after) do
    segments
    |> Enum.reverse()
    |> Enum.find_value(fn segment ->
      segment
      |> Segment.transactions()
      |> List.last()
      |> case do
        nil -> nil
        transaction -> Transaction.commit_version!(transaction)
      end
    end)
    |> Kernel.||(available_after)
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
