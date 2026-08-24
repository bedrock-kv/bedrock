defmodule Bedrock.DataPlane.Materializer.Olivine.Server do
  @moduledoc false
  use GenServer

  import Bedrock.Internal.GenServer.Replies

  alias Bedrock.DataPlane.CommitProxy
  alias Bedrock.DataPlane.Materializer
  alias Bedrock.DataPlane.Materializer.Olivine.DataDatabase
  alias Bedrock.DataPlane.Materializer.Olivine.Index
  alias Bedrock.DataPlane.Materializer.Olivine.Index.Page
  alias Bedrock.DataPlane.Materializer.Olivine.IndexDatabase
  alias Bedrock.DataPlane.Materializer.Olivine.IndexManager
  alias Bedrock.DataPlane.Materializer.Olivine.IntakeQueue
  alias Bedrock.DataPlane.Materializer.Olivine.Logic
  alias Bedrock.DataPlane.Materializer.Olivine.Reading
  alias Bedrock.DataPlane.Materializer.Olivine.State
  alias Bedrock.DataPlane.Materializer.Olivine.Telemetry, as: OlivineTelemetry
  alias Bedrock.DataPlane.Materializer.Telemetry
  alias Bedrock.Service.Foreman

  require Logger

  # Transaction count limits for adaptive batching
  # Small batches for responsiveness during normal operation
  @continuation_batch_count 5
  # Larger batches during lulls when no reads are waiting
  @timeout_batch_count 50

  # Ingest backpressure: above the high-water the ingest reply is withheld
  # (the puller blocks); it is released once the queue drains below the
  # release mark.
  @ingest_high_water_count 1_000
  @ingest_release_count 500

  @spec child_spec(opts :: keyword()) :: map()
  def child_spec(opts) do
    otp_name = opts[:otp_name] || raise "Missing :otp_name option"
    foreman = opts[:foreman] || raise "Missing :foreman option"
    id = opts[:id] || raise "Missing :id option"
    path = opts[:path] || raise "Missing :path option"
    startup_opts = startup_opts(opts[:cluster], opts[:params] || %{})

    %{
      id: {__MODULE__, id},
      start:
        {GenServer, :start_link,
         [
           __MODULE__,
           {otp_name, foreman, id, path, startup_opts},
           [name: otp_name]
         ]}
    }
  end

  # Builds the opts handed to Logic.startup/5 from the worker's manifest
  # params. shard_id is ALWAYS threaded through (it identifies the
  # worker's shard assignment for info facts and re-adoption, and must
  # never be gated on cluster presence); the ObjectStorage snapshot
  # handle additionally requires a cluster, which Logic guards on. Idle
  # spin-down is opt-in per worker (bedrock-q67.21.5): without an
  # explicit positive idle_timeout the worker never spins down, which is
  # what exempts the system shard (its bootstrap never sets the param).
  @spec startup_opts(cluster :: module() | nil, params :: map()) :: keyword()
  defp startup_opts(cluster, params) do
    base = [cluster: cluster, shard_id: params["shard_id"]]

    case params["idle_timeout"] do
      idle_timeout when is_integer(idle_timeout) and idle_timeout > 0 ->
        Keyword.put(base, :idle_timeout, idle_timeout)

      _ ->
        base
    end
  end

  @impl true
  def init(args), do: {:ok, args, {:continue, :finish_startup}}

  @impl true

  # A worker that cannot account for its shard's full history refuses,
  # rather than answering from an incomplete database. Absence and
  # ignorance are different facts and must never share a reply: {:ok,
  # nil} here would report "no such key" for keys that exist below a
  # retention floor this worker cannot reach. :unavailable is retryable
  # AND routing-invalidating for clients (Internal.Repo), so the caller
  # re-asks a proxy and may land on a member of the set that CAN answer —
  # FDB's wrong_shard_server, whose whole purpose is to send the client
  # somewhere else.
  def handle_call({:get, _key, _version, _opts}, _from, %State{unreadable_below: floor} = t) when floor != nil,
    do: reply(t, {:error, :unavailable})

  def handle_call({:get_range, _s, _e, _version, _opts}, _from, %State{unreadable_below: floor} = t) when floor != nil,
    do: reply(t, {:error, :unavailable})

  def handle_call({:get, key, version, opts}, from, %State{} = t) do
    t = touch_read_activity(t)

    # Set operation context metadata for this request
    Telemetry.trace_metadata(%{operation: :get, key: key})

    fetch_opts = opts |> Keyword.put(:reply_fn, reply_fn_for(from)) |> Keyword.put_new(:wait_ms, 1_000)
    context = Reading.ReadingContext.new(t.index_manager, t.database)

    {updated_manager, result} =
      Reading.handle_get(
        t.read_request_manager,
        context,
        key,
        version,
        fetch_opts
      )

    updated_state = %{t | read_request_manager: updated_manager}
    schedule_waiter_expiration(t.read_request_manager, updated_manager, fetch_opts[:wait_ms])

    case result do
      :ok -> noreply(updated_state, continue: :maybe_process_transactions)
      {:error, _reason} = error -> reply(updated_state, error)
    end
  end

  def handle_call({:get_range, start_key, end_key, version, opts}, from, %State{} = t) do
    t = touch_read_activity(t)

    # Set operation context metadata for this request
    Telemetry.trace_metadata(%{operation: :get_range, key: {start_key, end_key}})

    fetch_opts = opts |> Keyword.put(:reply_fn, reply_fn_for(from)) |> Keyword.put_new(:wait_ms, 1_000)
    context = Reading.ReadingContext.new(t.index_manager, t.database)

    {updated_manager, result} =
      Reading.handle_get_range(
        t.read_request_manager,
        context,
        start_key,
        end_key,
        version,
        fetch_opts
      )

    updated_state = %{t | read_request_manager: updated_manager}
    schedule_waiter_expiration(t.read_request_manager, updated_manager, fetch_opts[:wait_ms])

    case result do
      :ok -> noreply(updated_state, continue: :maybe_process_transactions)
      {:error, _reason} = error -> reply(updated_state, error)
    end
  end

  # The puller hands over a batch and waits for :ok. While locked, the
  # puller is being torn down: acknowledge and discard.
  @impl true
  def handle_call({:ingest, _encoded_transactions, _kcv}, _from, %State{mode: :locked} = t), do: reply(t, :ok)

  # Only the current puller may feed the stream. A superseded puller —
  # torn down at compaction cutover or recovery unlock — may have died
  # with an ingest call already in this mailbox; applying that batch
  # would graft a stale suffix, with a gap beneath it, onto the rewound
  # index. Acknowledge and discard. (With no puller at all, direct
  # ingest is the static unit-test configuration and is accepted.)
  def handle_call({:ingest, _encoded_transactions, _kcv}, {caller, _}, %State{pull_task: %Task{pid: pid}} = t)
      when caller !== pid, do: reply(t, :ok)

  def handle_call({:ingest, encoded_transactions, kcv}, from, %State{} = t) do
    updated_intake_queue = IntakeQueue.add_transactions(t.intake_queue, encoded_transactions)
    queue_size = IntakeQueue.size(updated_intake_queue)

    t = %{
      t
      | intake_queue: updated_intake_queue,
        known_committed_version: max_version(t.known_committed_version, kcv)
    }

    Telemetry.trace_transactions_queued(length(encoded_transactions), queue_size)

    if queue_size >= @ingest_high_water_count do
      # Backpressure: hold the reply until the queue drains. The puller
      # cannot outrun the applier because the applier holds the reply.
      noreply(%{t | pending_ingest: from}, continue: :process_transactions)
    else
      reply(t, :ok, continue: :process_transactions)
    end
  end

  @impl true
  def handle_call({:info, fact_names}, _from, %State{} = t), do: t |> Logic.info(fact_names) |> then(&reply(t, &1))

  @impl true
  def handle_call({:lock_for_recovery, epoch}, {director, _}, t) do
    with {:ok, t} <- Logic.lock_for_recovery(t, director, epoch),
         {:ok, info} <- Logic.info(t, Materializer.recovery_info()) do
      reply(t, {:ok, self(), info})
    else
      error -> reply(t, error)
    end
  end

  # The lock's taker is the unlock's only authority: adoption
  # (bedrock-q67.21.5) means family-named workers can now be locked by
  # racing epochs — an equal-or-newer lock replaces `director`, and a
  # superseded locker's late unlock must not flip the worker to
  # :running with the loser's pull sources mid-recovery. A worker with
  # no lock owner has no authority to violate (static configurations
  # unlock directly).
  @impl true
  def handle_call(
        {:unlock_after_recovery, _durable_version, _pull_sources},
        {caller, _},
        %State{director: director} = t
      )
      when director != nil and caller != director do
    reply(t, {:error, :not_lock_owner})
  end

  def handle_call({:unlock_after_recovery, durable_version, pull_sources}, {_director, _}, t) do
    {:ok, updated_state} = Logic.unlock_after_recovery(t, durable_version, pull_sources)
    # Service starts now: a worker that spent its whole idle window locked
    # (a long recovery) must not spin down on its first post-unlock check.
    reply(touch_read_activity(updated_state), :ok)
  end

  @impl true
  def handle_call(:compact, _from, %State{compaction_task: task} = t) when not is_nil(task) do
    # Compaction already in progress
    reply(t, {:error, :compaction_in_progress})
  end

  @impl true
  def handle_call(:compact, _from, %State{} = t) do
    {:ok, task} = Logic.start_compaction(t)
    updated_state = %{t | compaction_task: task, allow_window_advancement: false}
    reply(updated_state, :ok)
  end

  @impl true
  def handle_call(_, _from, t), do: reply(t, {:error, :not_ready})

  @impl true
  # Handle new 5-tuple format with opts
  def handle_continue(:finish_startup, {otp_name, foreman, id, path, opts}) when is_list(opts) do
    do_finish_startup(otp_name, foreman, id, path, opts)
  end

  # Backward compatibility: handle old 4-tuple format (for tests that bypass child_spec)
  def handle_continue(:finish_startup, {otp_name, foreman, id, path}) do
    do_finish_startup(otp_name, foreman, id, path, [])
  end

  def handle_continue(:report_health_to_foreman, %State{} = t) do
    :ok = Foreman.report_health(t.foreman, t.id, {:ok, self()})
    noreply(t, continue: :process_transactions)
  end

  def handle_continue(:process_transactions, %State{} = t) do
    case IntakeQueue.take_batch_by_count(t.intake_queue, @continuation_batch_count) do
      {[], nil, updated_intake_queue} ->
        # Queue empty, just wait for new transactions or timeout
        updated_state = maybe_release_ingest(%{t | intake_queue: updated_intake_queue})
        noreply(updated_state)

      {batch, _batch_last_version, updated_intake_queue} ->
        updated_state = maybe_release_ingest(%{t | intake_queue: updated_intake_queue})
        # Process small batch for responsiveness
        {:ok, state_with_txns, version} = Logic.apply_transactions(updated_state, batch)
        final_state = notify_waiting_fetches(state_with_txns, version)

        # Check for more transactions to process
        noreply(final_state, continue: :maybe_process_transactions)
    end
  end

  def handle_continue(:maybe_process_transactions, %State{} = t) do
    if IntakeQueue.empty?(t.intake_queue) do
      noreply(t, timeout: 0)
    else
      noreply(t, continue: :process_transactions)
    end
  end

  def handle_continue(:advance_window, %State{} = t) do
    if t.allow_window_advancement do
      {:ok, state_after_window} = Logic.advance_window(t)
      noreply(state_after_window)
    else
      # Compaction in progress - skip window advancement
      noreply(t)
    end
  end

  defp do_finish_startup(otp_name, foreman, id, path, opts) do
    # Set persistent telemetry metadata for this server
    Telemetry.trace_metadata(%{otp_name: otp_name, storage_id: id})

    Telemetry.trace_startup_start()

    case Logic.startup(otp_name, foreman, id, path, opts) do
      {:ok, state} ->
        Telemetry.trace_startup_complete()
        schedule_idle_check(state)
        noreply(state, continue: :report_health_to_foreman)

      {:error, reason} ->
        Telemetry.trace_startup_failed(reason)
        stop(:no_state, reason)
    end
  end

  defp notify_waiting_fetches(state, version) do
    context = Reading.ReadingContext.new(state.index_manager, state.database)
    updated_manager = Reading.notify_waiting_fetches(state.read_request_manager, context, version)
    %{state | read_request_manager: updated_manager}
  end

  defp maybe_release_ingest(%State{pending_ingest: nil} = t), do: t

  defp maybe_release_ingest(%State{pending_ingest: from} = t) do
    if IntakeQueue.size(t.intake_queue) < @ingest_release_count do
      GenServer.reply(from, :ok)
      %{t | pending_ingest: nil}
    else
      t
    end
  end

  defp max_version(nil, version), do: version
  defp max_version(version, nil), do: version
  defp max_version(a, b), do: max(a, b)

  # Periodic idle check (bedrock-q67.21.5): only client reads count as
  # activity - transaction application and pulls keep a shard fresh, not
  # hot. When the read-inactivity window expires the worker best-effort
  # uploads a snapshot (when configured), arranges for its foreman entry
  # (and on-disk working directory) to be removed once it has exited,
  # and stops with {:shutdown, :idle} so the distributor swaps the
  # placeholder in WITHOUT eager re-recruitment: demand revives the
  # shard on the next read.
  @impl true
  # The puller found a hole: the cluster's retention floor is above where
  # our data ends, so the span between exists nowhere we can reach. This
  # is not transient and not a fault of any one log — every replica
  # answers identically — so it is recorded once and the worker stops
  # answering reads. It stays unreadable for its lifetime: Bedrock has no
  # equivalent of FDB's fetchKeys, so nothing here can close the gap.
  # Making it VISIBLE is the point; a worker serving an incomplete shard
  # silently is the failure this prevents.
  def handle_info({:shard_hole, _floor}, %State{unreadable_below: existing} = t) when existing != nil, do: noreply(t)

  def handle_info({:shard_hole, floor}, %State{} = t) do
    Logger.error(
      "Bedrock materializer #{t.id} (shard #{inspect(t.shard_id)}): log retention floor #{inspect(floor)} is above " <>
        "this worker's data; it cannot serve this shard and will refuse reads"
    )

    OlivineTelemetry.trace_shard_unreadable(t.id, t.shard_id, floor)
    noreply(%{t | unreadable_below: floor})
  end

  def handle_info(:idle_check, %State{idle_timeout: :infinity} = t), do: noreply(t)

  # A locked worker is mid-recovery: the director is counting on it, and
  # reads legitimately pause while the lock is held — not idleness.
  # An active compaction owns the scratch files and the window; spinning
  # down mid-compaction would upload from a moving target and then let
  # the deferred removal delete the directory under the task. Both defer:
  # re-arm and check again.
  def handle_info(:idle_check, %State{mode: :locked} = t), do: t |> schedule_idle_check() |> noreply()

  def handle_info(:idle_check, %State{compaction_task: task} = t) when not is_nil(task),
    do: t |> schedule_idle_check() |> noreply()

  def handle_info(:idle_check, %State{} = t) do
    idle_ms = System.monotonic_time(:millisecond) - t.last_read_at

    if idle_ms >= t.idle_timeout do
      initiate_idle_spindown(t, idle_ms)
    else
      schedule_idle_check(t)
      noreply(t, continue: :maybe_process_transactions)
    end
  end

  # Discard transactions when locked
  def handle_info({:apply_transactions, _encoded_transactions}, %State{mode: :locked} = t), do: noreply(t)

  @impl true
  def handle_info({:apply_transactions, encoded_transactions}, %State{} = t) do
    # Queue the transactions and start processing
    updated_intake_queue = IntakeQueue.add_transactions(t.intake_queue, encoded_transactions)
    updated_state = %{t | intake_queue: updated_intake_queue}
    queue_size = IntakeQueue.size(updated_intake_queue)
    Telemetry.trace_transactions_queued(length(encoded_transactions), queue_size)
    Telemetry.trace_transaction_timeout_scheduled()
    noreply(updated_state, continue: :process_transactions)
  end

  @impl true
  def handle_info(:timeout, %State{} = t) do
    # First, process a larger batch of transactions for throughput
    case IntakeQueue.take_batch_by_count(t.intake_queue, @timeout_batch_count) do
      {[], nil, updated_intake_queue} ->
        # No transactions to process, advance window during this lull
        updated_state = maybe_release_ingest(%{t | intake_queue: updated_intake_queue})
        noreply(updated_state, continue: :advance_window)

      {batch, _batch_last_version, updated_intake_queue} ->
        updated_state = maybe_release_ingest(%{t | intake_queue: updated_intake_queue})
        # Process larger batch for throughput
        {:ok, state_with_txns, version} = Logic.apply_transactions(updated_state, batch)
        state_after_txns = notify_waiting_fetches(state_with_txns, version)

        # Now advance window after processing transactions
        {:ok, final_state} = Logic.advance_window(state_after_txns)
        noreply(final_state, continue: :maybe_process_transactions)
    end
  end

  @impl true
  def handle_info(:expire_waiting_fetches, %State{} = t) do
    updated_manager = Reading.expire_waiting_fetches(t.read_request_manager)
    noreply(%{t | read_request_manager: updated_manager})
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, _reason}, %State{} = t) do
    updated_manager = Reading.remove_active_task(t.read_request_manager, pid)
    updated_state = %{t | read_request_manager: updated_manager}
    noreply(updated_state)
  end

  @impl true
  def handle_info(
        {:compaction_ready, compact_data_fd, compact_idx_fd, compact_data_path, compact_idx_path, new_data_offset,
         index_offset, compacted_pages, durable_version, duration, data_size_before, index_size_before},
        %State{} = t
      ) do
    # Atomic cutover to compacted files
    # The cutover rewinds the index to the durable snapshot, so the
    # running puller's position (and any batch it has in flight) is
    # meaningless. Stop it first — releasing a backpressure-parked ingest
    # reply on the way — and rejoin the stream at the durable boundary
    # once the new state is built. The stream re-delivers everything
    # ingested during compaction; nothing is lost and nothing special
    # remembers it.
    t = Logic.stop_pulling(t)

    {data_db, index_db} = t.database
    data_path = data_db.file_name
    idx_path = index_db.file_name

    # Note: We don't explicitly close the old files - on Unix, we can rename open files,
    # and they'll be closed automatically when no longer referenced. Attempting to close
    # them can fail with :not_on_controlling_process due to file descriptor ownership.

    # Rename files atomically

    # Create .old backup names
    old_data_path = data_path ++ ~c".old"
    old_idx_path = idx_path ++ ~c".old"

    :ok = :file.rename(data_path, old_data_path)
    :ok = :file.rename(idx_path, old_idx_path)
    :ok = :file.rename(compact_data_path, data_path)
    :ok = :file.rename(compact_idx_path, idx_path)

    # Clean up .old backup files after successful rename
    :ok = :file.delete(old_data_path)
    :ok = :file.delete(old_idx_path)

    # Build new database structures from compacted files
    # File name is now the original path (we renamed compact to replace it)
    new_data_db = %DataDatabase{
      file: compact_data_fd,
      file_offset: new_data_offset,
      file_name: data_path,
      window_size_in_microseconds: 5_000_000,
      buffer: :ets.new(:buffer, [:ordered_set, :protected, {:read_concurrency, true}])
    }

    new_index_db = %IndexDatabase{
      file: compact_idx_fd,
      file_offset: index_offset,
      file_name: idx_path,
      durable_version: durable_version,
      last_block_empty: false,
      last_block_offset: 0,
      last_block_previous_version: nil
    }

    new_database = {new_data_db, new_index_db}

    # Build index structures from in-memory compacted pages
    new_tree = Index.Tree.from_page_map(compacted_pages)
    {min_key, max_key} = calculate_key_bounds_from_pages(compacted_pages)

    # Get max_keys_per_page from the durable version's index
    {^durable_version, {durable_index, _modified}} =
      Enum.find(t.index_manager.versions, fn {v, _} -> v == durable_version end)

    new_index = %Index{
      tree: new_tree,
      page_map: compacted_pages,
      min_key: min_key,
      max_key: max_key,
      max_keys_per_page: durable_index.max_keys_per_page,
      target_keys_per_page: durable_index.target_keys_per_page
    }

    new_index_manager = %IndexManager{
      versions: [{durable_version, {new_index, %{}}}],
      current_version: durable_version,
      window_size_in_microseconds: 5_000_000,
      id_allocator: t.index_manager.id_allocator,
      output_queue: :queue.new(),
      last_version_ended_at_offset: 0,
      window_lag_time_μs: 5_000_000,
      n_keys: IndexManager.info(t.index_manager, :n_keys)
    }

    # Reset state for replay
    new_state = %{
      t
      | database: new_database,
        index_manager: new_index_manager,
        intake_queue: IntakeQueue.new(),
        compaction_task: nil,
        allow_window_advancement: true
    }

    # Emit completion telemetry
    values_compacted = Enum.sum(Enum.map(compacted_pages, fn {_, {page, _}} -> Page.key_count(page) end))

    OlivineTelemetry.trace_compaction_complete(durable_version,
      duration_μs: duration,
      data_size_before: data_size_before,
      data_size_after: new_data_offset,
      index_size_before: index_size_before,
      index_size_after: index_offset,
      values_compacted: values_compacted
    )

    # Optionally upload snapshot to ObjectStorage (async, fire-and-forget)
    Logic.maybe_upload_snapshot(new_state, data_path, idx_path, durable_version)

    # Resume: a fresh puller joins the stream at the durable boundary and
    # re-delivers everything after it through the normal apply path.
    noreply(Logic.resume_pulling_from(new_state, durable_version))
  end

  @impl true
  def handle_info({:compaction_failed, reason}, %State{} = t) do
    # Log error and resume normal operation
    require Logger

    Logger.error("Compaction failed: #{inspect(reason)}")

    # Clean up any partial .compact files
    {data_db, index_db} = t.database

    try do
      :file.delete(data_db.file_name ++ ~c".compact")
      :file.delete(index_db.file_name ++ ~c".compact")
    catch
      _, _ -> :ok
    end

    # Resume normal operation
    updated_state = %{t | compaction_task: nil, allow_window_advancement: true}
    noreply(updated_state)
  end

  # A newly durable layout, relayed by this node's foreman: rejoin
  # validation. Unlike logs, materializer membership is not in the wiring
  # push — it lives in the committed keyspace (materializers/<tag>) — so
  # the worker asks a commit proxy for its tag's entry (FDB's
  # storage-server rejoin against the proxy's txnStateStore). Absence or
  # a different worker id is authoritative displacement: dispose and
  # exit; nobody else decides. Errors (locked, unavailable, timeout) are
  # not verdicts — revalidate on the next push. A layout may judge every
  # worker it had the chance to include (pushed epoch >= ours — every
  # recovery locks every advertised materializer into its epoch, so the
  # completing push carries the stray's own epoch); only a push older
  # than our lock — an in-flight recovery's past — is off-limits.
  @impl true
  def handle_info({:tsl_updated, %{epoch: pushed_epoch} = tsl}, %State{} = t) do
    if validation_due?(t, pushed_epoch) do
      case rejoin_verdict(t, Map.get(tsl, :proxies, [])) do
        :keep ->
          noreply(t)

        :displaced ->
          require Logger

          Logger.info("Bedrock materializer #{t.id}: keyspace no longer names it for shard #{t.shard_num}; retiring")
          Foreman.worker_retired(t.foreman, t.id)
          {:stop, {:shutdown, :displaced}, t}
      end
    else
      noreply(t)
    end
  end

  @impl true
  def handle_info(_msg, state), do: {:noreply, state}

  # Only a layout that had the chance to include us may judge us: pushed
  # epoch at or past the one we were locked into (nil means never locked —
  # a cold-boot resurrection any completed layout may judge). Static
  # materializers (no shard assignment) are outside the cluster layout
  # entirely.
  @spec validation_due?(State.t(), Bedrock.epoch()) :: boolean()
  defp validation_due?(%State{shard_num: nil}, _pushed_epoch), do: false
  defp validation_due?(%State{epoch: nil}, _pushed_epoch), do: true
  defp validation_due?(%State{epoch: my_epoch}, pushed_epoch), do: pushed_epoch >= my_epoch

  @spec rejoin_verdict(State.t(), [CommitProxy.ref()]) :: :keep | :displaced
  defp rejoin_verdict(_t, []), do: :keep

  defp rejoin_verdict(%State{} = t, proxies) do
    # Membership, not resolution: a shard may have several materializers,
    # so the question is whether the committed set still contains ME —
    # another member's presence is not my displacement (FDB's
    # matchesThisServer, asked of the set).
    case CommitProxy.materializer_members(Enum.random(proxies), t.shard_num) do
      {:ok, members} -> if Map.has_key?(members, t.id), do: :keep, else: :displaced
      {:error, :not_found} -> :displaced
      {:error, _not_a_verdict} -> :keep
    end
  end

  @spec touch_read_activity(State.t()) :: State.t()
  defp touch_read_activity(%State{} = t), do: %{t | last_read_at: System.monotonic_time(:millisecond)}

  # Checks run at a quarter of the timeout, so per-read cost stays a
  # single timestamp write instead of per-request timer churn on the hot
  # path.
  @spec schedule_idle_check(State.t()) :: State.t()
  defp schedule_idle_check(%State{idle_timeout: :infinity} = t), do: t

  defp schedule_idle_check(%State{idle_timeout: idle_timeout} = t) do
    Process.send_after(self(), :idle_check, max(div(idle_timeout, 4), 10))
    t
  end

  # The snapshot upload gates the spin-down: it is the only durable
  # artifact bridging spin-down to demand-driven revival, so on failure
  # the worker stays up and retries at the next check — cheaper and
  # louder than a full log replay at revival.
  @spec initiate_idle_spindown(State.t(), non_neg_integer()) ::
          {:stop, {:shutdown, :idle}, State.t()} | {:noreply, State.t()}
  defp initiate_idle_spindown(%State{} = t, idle_ms) do
    case Logic.upload_snapshot_before_spindown(t) do
      :ok ->
        OlivineTelemetry.trace_idle_spindown(idle_ms,
          n_keys: IndexManager.info(t.index_manager, :n_keys),
          size_in_bytes: IndexManager.info(t.index_manager, :size_in_bytes)
        )

        remove_worker_after_exit(t)
        stop(t, {:shutdown, :idle})

      {:error, reason} ->
        Logger.warning("Idle spin-down aborted: snapshot upload failed (#{inspect(reason)}); staying up")
        t |> schedule_idle_check() |> noreply()
    end
  end

  # The foreman entry (and with it the on-disk working directory) is
  # reclaimed by Foreman.remove_worker/3 - but only after this worker
  # has actually exited, so the {:shutdown, :idle} exit reason reaches
  # the distributor's monitor untainted by the supervisor's
  # terminate_child. Calling the foreman inline would also deadlock: it
  # calls back into this worker's supervisor.
  @spec remove_worker_after_exit(State.t()) :: :ok
  defp remove_worker_after_exit(%State{foreman: foreman, id: id}) do
    worker = self()

    spawn(fn ->
      ref = Process.monitor(worker)

      receive do
        {:DOWN, ^ref, :process, ^worker, _reason} ->
          try do
            Foreman.remove_worker(foreman, id, timeout: 5_000)
          catch
            kind, reason ->
              Logger.warning(
                "Failed to remove idle materializer worker #{inspect(id)} from foreman: #{inspect({kind, reason})}"
              )
          end
      after
        30_000 -> :ok
      end
    end)

    :ok
  end

  @impl true
  def terminate(reason, %State{} = t) do
    Telemetry.trace_shutdown_start(reason)
    Reading.shutdown(t.read_request_manager)
    Logic.shutdown(t)
    Telemetry.trace_shutdown_complete()
    :ok
  end

  @impl true
  def terminate(_reason, _state), do: :ok

  defp reply_fn_for(from), do: fn result -> GenServer.reply(from, result) end

  defp schedule_waiter_expiration(previous_manager, updated_manager, wait_ms)
       when is_integer(wait_ms) and wait_ms > 0 do
    if previous_manager.waiting_fetches != updated_manager.waiting_fetches do
      Process.send_after(self(), :expire_waiting_fetches, wait_ms)
    end

    :ok
  end

  defp schedule_waiter_expiration(_previous_manager, _updated_manager, _wait_ms), do: :ok

  # Calculate min/max key bounds from page_map
  defp calculate_key_bounds_from_pages(page_map) when map_size(page_map) == 0, do: {<<0xFF, 0xFF>>, <<>>}

  defp calculate_key_bounds_from_pages(page_map) do
    min_key =
      page_map
      |> Enum.map(fn {_id, {page, _next}} -> Page.left_key(page) end)
      |> Enum.reject(&is_nil/1)
      |> case do
        [] -> <<0xFF, 0xFF>>
        keys -> Enum.min(keys)
      end

    max_key =
      page_map
      |> Enum.map(fn {_id, {page, _next}} -> Page.right_key(page) end)
      |> Enum.reject(&is_nil/1)
      |> case do
        [] -> <<>>
        keys -> Enum.max(keys)
      end

    {min_key, max_key}
  end
end
