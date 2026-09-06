defmodule Bedrock.DataPlane.Materializer.Olivine.Logic do
  @moduledoc false

  import Bedrock.DataPlane.Materializer.Olivine.State,
    only: [update_mode: 2, update_director_and_epoch: 3, reset_puller: 1, put_puller: 2]

  alias Bedrock.ControlPlane.Director
  alias Bedrock.DataPlane.Materializer
  alias Bedrock.DataPlane.Materializer.Olivine.CompactionWriter.SplitFile, as: SplitFileWriter
  alias Bedrock.DataPlane.Materializer.Olivine.Database
  alias Bedrock.DataPlane.Materializer.Olivine.IndexManager
  alias Bedrock.DataPlane.Materializer.Olivine.IntakeQueue
  alias Bedrock.DataPlane.Materializer.Olivine.SnapshotPolicy
  alias Bedrock.DataPlane.Materializer.Olivine.State
  alias Bedrock.DataPlane.Materializer.Olivine.Streaming
  alias Bedrock.DataPlane.Materializer.Olivine.Telemetry, as: OlivineTelemetry
  alias Bedrock.DataPlane.Materializer.Telemetry
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version
  alias Bedrock.ObjectStorage.Config, as: ObjectStorageConfig
  alias Bedrock.ObjectStorage.Keys
  alias Bedrock.ObjectStorage.Snapshot
  alias Bedrock.ObjectStorage.SnapshotBundle
  alias Bedrock.Service.Worker

  require Logger

  @spec startup(otp_name :: atom(), foreman :: pid(), id :: Worker.id(), Path.t()) ::
          {:ok, State.t()} | {:error, File.posix()} | {:error, term()}
  @spec startup(otp_name :: atom(), foreman :: pid(), id :: Worker.id(), Path.t(), opts :: keyword()) ::
          {:ok, State.t()} | {:error, File.posix()} | {:error, term()}
  def startup(otp_name, foreman, id, path, opts \\ []) do
    cluster = Keyword.get(opts, :cluster)
    {shard_tag, shard_num} = normalize_shard(Keyword.get(opts, :shard_id))
    idle_timeout = Keyword.get(opts, :idle_timeout, :infinity)
    snapshot_policy = Keyword.get(opts, :snapshot_policy, %SnapshotPolicy{})
    snapshot = build_snapshot_handle(cluster, shard_tag)

    with :ok <- ensure_directory_exists(path),
         :ok <- maybe_load_snapshot(path, snapshot),
         {:ok, database} <- Database.open(:"#{otp_name}_db", Path.join(path, "dets"), opts),
         {:ok, index_manager} <- IndexManager.recover_from_database(database) do
      {:ok,
       %State{
         path: path,
         otp_name: otp_name,
         id: id,
         shard_id: shard_tag,
         shard_num: shard_num,
         foreman: foreman,
         database: database,
         index_manager: index_manager,
         snapshot: snapshot,
         snapshot_policy: snapshot_policy,
         idle_timeout: idle_timeout,
         last_read_at: System.monotonic_time(:millisecond)
       }}
    end
  end

  # The shard assignment arrives as either the numeric shard id (the form the
  # SHARD_INDEX and ShardServers use) or its base-36 tag (the form object
  # storage paths use). Keep both: the tag for the snapshot handle, the
  # number for ShardServer discovery.
  @spec normalize_shard(non_neg_integer() | String.t() | nil) ::
          {tag :: String.t() | nil, num :: non_neg_integer() | nil}
  defp normalize_shard(nil), do: {nil, nil}
  defp normalize_shard(shard_num) when is_integer(shard_num), do: {Keys.shard_tag(shard_num), shard_num}

  defp normalize_shard(shard_tag) when is_binary(shard_tag) do
    case Keys.parse_shard_tag(shard_tag) do
      {:ok, shard_num} -> {shard_tag, shard_num}
      {:error, _} -> {shard_tag, nil}
    end
  end

  @spec build_snapshot_handle(cluster :: module() | nil, shard_id :: String.t() | nil) :: Snapshot.t() | nil
  defp build_snapshot_handle(nil, _shard_id), do: nil
  defp build_snapshot_handle(_cluster, nil), do: nil

  defp build_snapshot_handle(_cluster, shard_id) do
    backend = ObjectStorageConfig.backend()
    Snapshot.new(backend, shard_id)
  end

  @doc """
  Checks if local database files exist. If not, attempts to discover and
  restore from the latest snapshot in ObjectStorage.
  """
  @spec maybe_load_snapshot(Path.t(), Snapshot.t() | nil) :: :ok | {:error, term()}
  def maybe_load_snapshot(_path, nil), do: :ok

  def maybe_load_snapshot(path, %Snapshot{} = snapshot) do
    # Database.open uses Path.dirname(file_path) and creates data/idx files there
    # We pass Path.join(path, "dets") to Database.open, so files are at path/data, path/idx
    data_path = Path.join(path, "data")
    idx_path = Path.join(path, "idx")

    if File.exists?(data_path) and File.exists?(idx_path) do
      # Local files exist - use them (warm start)
      :ok
    else
      # Cold start - discover and download from ObjectStorage
      load_snapshot_from_object_storage(path, snapshot)
    end
  end

  @spec load_snapshot_from_object_storage(Path.t(), Snapshot.t()) :: :ok | {:error, term()}
  defp load_snapshot_from_object_storage(path, snapshot) do
    bundle_path = Path.join(path, "snapshot.bundle")
    data_path = Path.join(path, "data")
    idx_path = Path.join(path, "idx")

    # Discovery: find latest snapshot in ObjectStorage
    with {:ok, version, data} <- Snapshot.read_latest(snapshot),
         :ok <- File.write(bundle_path, data),
         {:ok, _, _} <- SnapshotBundle.split_in_place(bundle_path, data_path, idx_path) do
      Logger.info("Discovered and loaded snapshot from ObjectStorage", version: version)
      :ok
    else
      {:error, :not_found} ->
        # No snapshot discovered - proceed with empty state
        Logger.info("No snapshot discovered in ObjectStorage, starting fresh")
        :ok

      {:error, reason} ->
        {:error, {:snapshot_load_failed, reason}}
    end
  end

  @spec ensure_directory_exists(Path.t()) :: :ok | {:error, File.posix()}
  defp ensure_directory_exists(path), do: File.mkdir_p(path)

  @doc """
  This worker's retirement notice key, or nil if it has no assignment.

  The worker id is IN the key, so the question a private mutation poses
  is FDB's own — `startsWith(data->sk)`, "is this about me?"
  (`storageserver.actor.cpp:11523`) — not "does this value name someone
  else". Set-valued membership is what collapses it: a sibling joining or
  leaving is not this worker's business.
  """
  @spec retirement_notice_key(State.t()) :: Bedrock.key() | nil
  def retirement_notice_key(%State{id: id, shard_num: shard_num}) when is_binary(id) and is_integer(shard_num),
    do: Bedrock.end_of_keyspace() <> Bedrock.SystemKeys.materializer_key(shard_num, id)

  def retirement_notice_key(_unassigned), do: nil

  @doc """
  Whether a batch carries this worker's retirement notice.

  Asked at the single point where stream data becomes durable state, so
  the worker retires at exactly the version its assignment ends — the
  notice rides the same commit that removed the membership entry.
  """
  @spec retirement_notice?([Transaction.encoded()], Bedrock.key() | nil) :: boolean()
  def retirement_notice?(_batch, nil), do: false

  def retirement_notice?(batch, notice_key) do
    Enum.any?(batch, fn transaction ->
      case Transaction.mutations(transaction) do
        {:ok, mutations} -> Enum.any?(mutations, &match?({:clear, ^notice_key}, &1))
        _no_mutations -> false
      end
    end)
  end

  @spec shutdown(State.t()) :: :ok
  def shutdown(%State{} = t) do
    stop_pulling(t)
    :ok = Database.close(t.database)
  end

  @spec lock_for_recovery(State.t(), Director.ref(), Bedrock.epoch()) ::
          {:ok, State.t()} | {:error, :newer_epoch_exists | String.t()}
  def lock_for_recovery(t, _, epoch) when not is_nil(t.epoch) and epoch < t.epoch, do: {:error, :newer_epoch_exists}

  def lock_for_recovery(t, director, epoch) do
    t
    |> update_mode(:locked)
    |> update_director_and_epoch(director, epoch)
    |> stop_pulling()
    |> then(&{:ok, &1})
  end

  @spec stop_pulling(State.t()) :: State.t()
  def stop_pulling(%{pull_task: nil} = t), do: release_pending_ingest(t)

  def stop_pulling(%{pull_task: puller} = t) do
    Streaming.stop(puller)

    t
    |> reset_puller()
    |> release_pending_ingest()
  end

  # A puller parked in a backpressured ingest call must not be left waiting
  # on a reply that will never come; replying to an already-dead caller is a
  # harmless no-op.
  defp release_pending_ingest(%{pending_ingest: nil} = t), do: t

  defp release_pending_ingest(%{pending_ingest: from} = t) do
    GenServer.reply(from, :ok)
    %{t | pending_ingest: nil}
  end

  @spec unlock_after_recovery(State.t(), Bedrock.version(), Materializer.pull_sources()) ::
          {:ok, State.t()}
  def unlock_after_recovery(t, durable_version, pull_sources) when is_list(pull_sources) do
    t =
      t
      |> stop_pulling()
      |> rollback_uncommitted(durable_version)
      |> Map.put(:pull_sources, pull_sources)

    t
    |> start_pulling_from(resume_position(t))
    |> update_mode(:running)
    |> then(&{:ok, &1})
  end

  # The recovery version is the cluster's rollback point: everything this
  # materializer applied above it was an uncommitted suffix, and it lives
  # only in memory (eviction is clamped to the known-committed version,
  # which the recovery version can never undercut). Discarding it is pure
  # pointer manipulation. Queued-but-unapplied transactions are dropped
  # too: the resumed stream re-delivers everything after the rollback point.
  defp rollback_uncommitted(t, recovery_version) do
    %{
      t
      | index_manager: IndexManager.rollback_to(t.index_manager, recovery_version),
        intake_queue: IntakeQueue.new(),
        known_committed_version: nil
    }
  end

  # A streaming materializer resumes from its own applied position — the
  # stream serves any starting point (chunks reach arbitrarily far back),
  # so a materializer restored from an old snapshot simply has more stream
  # to drink.
  defp resume_position(t), do: t.index_manager.current_version

  @doc """
  Restarts the stream from a rewind point.

  Compaction cutover rewinds the index to the durable snapshot, which makes
  the running puller's position meaningless — the same situation recovery
  handles at unlock, resolved the same way: stop the puller and rejoin the
  stream at the boundary. The stream re-delivers everything after it; no
  suffix bookkeeping, no special path.
  """
  @spec resume_pulling_from(State.t(), Bedrock.version()) :: State.t()
  def resume_pulling_from(t, start_after) do
    t
    |> stop_pulling()
    |> start_pulling_from(start_after)
  end

  # Without a shard assignment there is no stream to join: the materializer
  # is static, fed only through direct ingest (unit-test configurations).
  # Production materializers always receive their shard from the director.
  # Without pull sources (never unlocked into a layout), likewise.
  @spec start_pulling_from(State.t(), Bedrock.version()) :: State.t()
  defp start_pulling_from(%{shard_num: nil} = t, _start_after), do: t
  defp start_pulling_from(%{pull_sources: nil} = t, _start_after), do: t

  defp start_pulling_from(%{shard_num: shard_num, pull_sources: sources} = t, start_after) when is_list(sources) do
    # The stream puller: everything — history, recent data, and version
    # currency — comes from this shard's ShardServer. Batches are handed
    # over synchronously; the server withholds the reply for backpressure.
    server = self()
    ingest_fn = fn transactions, kcv -> GenServer.call(server, {:ingest, transactions, kcv}, :infinity) end

    puller = Streaming.start_pulling(shard_num, start_after, sources, ingest_fn)
    put_puller(t, puller)
  end

  @spec info(State.t(), Materializer.fact_name() | [Materializer.fact_name()]) ::
          {:ok, term() | %{Materializer.fact_name() => term()}} | {:error, :unsupported_info}
  def info(%State{} = t, fact_name) when is_atom(fact_name), do: {:ok, gather_info(fact_name, t)}

  def info(%State{} = t, fact_names) when is_list(fact_names) do
    {:ok,
     fact_names
     |> Enum.reduce([], fn
       fact_name, acc -> [{fact_name, gather_info(fact_name, t)} | acc]
     end)
     |> Map.new()}
  end

  defp supported_info, do: ~w[
      current_version
      durable_version
      epoch
      oldest_durable_version
      id
      pid
      path
      key_ranges
      kind
      n_keys
      otp_name
      shard_id
      size_in_bytes
      supported_info
      utilization
    ]a

  defp gather_info(:oldest_durable_version, t), do: Database.durable_version(t.database)
  defp gather_info(:durable_version, t), do: Database.durable_version(t.database)
  # The applied (in-memory) position — what reads can be served through.
  # Distinct from :durable_version, which eviction clamps to the
  # known-committed version and which therefore trails by design.
  defp gather_info(:current_version, t), do: t.index_manager.current_version
  defp gather_info(:shard_id, t), do: t.shard_num
  # The epoch this worker was last locked into (nil: never locked). The
  # distributor's assignment verification reads it to distinguish a
  # worker that is IN the epoch from one the epoch never embraced — a
  # node that missed recovery's roll call and rejoined later.
  defp gather_info(:epoch, t), do: t.epoch
  defp gather_info(:id, t), do: t.id
  defp gather_info(:key_ranges, t), do: IndexManager.info(t.index_manager, :key_ranges)
  defp gather_info(:kind, _t), do: :materializer
  defp gather_info(:n_keys, t), do: IndexManager.info(t.index_manager, :n_keys)
  defp gather_info(:otp_name, t), do: t.otp_name
  defp gather_info(:path, t), do: t.path
  defp gather_info(:pid, _t), do: self()
  defp gather_info(:size_in_bytes, t), do: IndexManager.info(t.index_manager, :size_in_bytes)
  defp gather_info(:supported_info, _t), do: supported_info()
  defp gather_info(:utilization, t), do: IndexManager.info(t.index_manager, :utilization)
  defp gather_info(_unsupported, _t), do: {:error, :unsupported_info}

  defp max_eviction_size, do: 10 * 1024 * 1024

  @doc """
  Performs window advancement by delegating policy decisions to IndexManager and handling persistence.
  IndexManager determines what to evict based on buffer tracking and hot set management.
  Logic handles database persistence and telemetry for the eviction.
  """
  @spec advance_window(State.t()) :: {:ok, State.t()} | {:error, term()}
  def advance_window(%State{} = state) do
    start_time = System.monotonic_time(:microsecond)

    # The eviction cap is the known-committed version: nothing above it may
    # become durable, so a recovery rollback never has to touch disk.
    case IndexManager.advance_window(state.index_manager, max_eviction_size(), state.known_committed_version) do
      {:no_eviction, updated_index_manager} ->
        updated_state = %{state | index_manager: updated_index_manager}
        {:ok, updated_state}

      {:evict, evicted_count, updated_index_manager, collected_pages, eviction_version} ->
        window_edge = calculate_window_edge_for_telemetry(eviction_version, state.window_lag_time_μs)
        {data_db, _index_db} = state.database

        current_durable_version = Database.durable_version(state.database)

        case Database.advance_durable_version(
               state.database,
               eviction_version,
               current_durable_version,
               data_db.file_offset,
               collected_pages
             ) do
          {:ok, updated_database, db_pipeline} ->
            updated_state = %{state | index_manager: updated_index_manager, database: updated_database}

            duration = System.monotonic_time(:microsecond) - start_time
            lag_time_μs = calculate_lag_time_μs(window_edge, eviction_version)

            OlivineTelemetry.trace_window_advanced(:evicted, eviction_version,
              duration_μs: duration,
              evicted_count: evicted_count,
              lag_time_μs: lag_time_μs,
              window_target_version: window_edge,
              data_size_in_bytes: data_db.file_offset,
              durable_version_duration_μs: db_pipeline.total_duration_μs,
              db_insert_time_μs: db_pipeline.insert_time_μs,
              db_write_time_μs: db_pipeline.write_time_μs
            )

            {:ok, updated_state}

          {:error, reason} ->
            {:error, {:durable_version_advance_failed, reason}}
        end
    end
  end

  # Helper to calculate window edge for telemetry purposes.
  # Uses eviction version directly instead of extracting from batch for efficiency.
  defp calculate_window_edge_for_telemetry(eviction_version, window_lag_time_μs) do
    Version.subtract(eviction_version, window_lag_time_μs)
  rescue
    ArgumentError ->
      # Underflow - return zero version
      Version.zero()
  end

  defp calculate_lag_time_μs(window_edge_version, eviction_version) do
    max(0, Version.distance(window_edge_version, eviction_version))
  rescue
    _ -> 0
  end

  @doc """
  Apply a batch of transactions to the storage state.
  This is used for incremental processing, so that durable writes happen in
  bounded steps as the window advances rather than as one large flush.
  Buffer tracking is handled directly by IndexManager.apply_transactions.
  """
  @spec apply_transactions(State.t(), [binary()]) :: {:ok, State.t(), Bedrock.version()}
  def apply_transactions(%State{} = t, encoded_transactions) do
    batch_size = length(encoded_transactions)
    batch_size_bytes = Enum.sum(Enum.map(encoded_transactions, &byte_size/1))
    start_time = System.monotonic_time(:microsecond)

    {updated_index_manager, updated_database} =
      IndexManager.apply_transactions(t.index_manager, encoded_transactions, t.database)

    version = updated_index_manager.current_version

    duration = System.monotonic_time(:microsecond) - start_time
    Telemetry.trace_transaction_processing_complete(batch_size, duration, batch_size_bytes)

    # Update state with both updated index manager and database
    updated_state = %{
      t
      | index_manager: updated_index_manager,
        database: updated_database,
        snapshot_policy: SnapshotPolicy.observe(t.snapshot_policy, batch_size, batch_size_bytes)
    }

    {:ok, updated_state, version}
  end

  @doc """
  Initiates background compaction of database files.

  Returns a Task that will build compacted files. The task sends a message to the
  calling process when complete with the compacted file handles and page_map.

  This function does not block - compaction happens in the background.
  """
  @spec start_compaction(State.t()) :: {:ok, Task.t()}
  def start_compaction(%State{} = state) do
    database = state.database
    # Get complete current page_map from index
    complete_page_map = IndexManager.get_complete_page_map(state.index_manager)
    caller = self()

    durable_version = Database.durable_version(database)
    {data_db, index_db} = database

    # Emit start telemetry
    OlivineTelemetry.trace_compaction_started(durable_version,
      data_size_before: data_db.file_offset,
      index_size_before: index_db.file_offset
    )

    # Prepare compact file paths
    compact_data_path = data_db.file_name ++ ~c".compact"
    compact_idx_path = index_db.file_name ++ ~c".compact"

    task =
      Task.async(fn ->
        start_time = System.monotonic_time(:microsecond)

        with {:ok, writer} <- SplitFileWriter.new(compact_data_path, compact_idx_path),
             {:ok, result, compacted_pages, durable_version} <-
               Database.compact(database, complete_page_map, SplitFileWriter, writer) do
          duration = System.monotonic_time(:microsecond) - start_time

          send(caller, {
            :compaction_ready,
            result.data_fd,
            result.idx_fd,
            result.data_path,
            result.idx_path,
            result.data_offset,
            result.idx_offset,
            compacted_pages,
            durable_version,
            duration,
            data_db.file_offset,
            index_db.file_offset
          })

          :ok
        else
          {:error, reason} ->
            OlivineTelemetry.trace_compaction_failed(reason)
            send(caller, {:compaction_failed, reason})
            {:error, reason}
        end
      end)

    {:ok, task}
  end

  @doc """
  Synchronous snapshot upload before an idle spin-down.

  Runs in the caller (unlike `maybe_upload_snapshot/4`): the worker's
  removal reclaims its working directory right after it exits, so a
  fire-and-forget task would race the deletion of the very files it
  reads.

  The upload NEVER ships the live files: the live idx file is a delta
  chain of per-window-advance records, while the bundle restore path
  (`SnapshotBundle.split_in_place/3`) expects a single self-terminating
  index record — a raw multi-record upload restores at best partially
  and at worst as a silently EMPTY shard, and `Snapshot.write`'s
  put-if-not-exists makes the poisoned bundle permanent. Instead the
  durable state is compacted to scratch files (the same
  `Database.compact/4` the compaction path uploads from, whose output
  is exactly the bundle format) and those are uploaded.

  A no-op when no snapshot is configured. A failure is returned, not
  swallowed: the caller must abort the spin-down (stay up, retry at the
  next idle check) — this snapshot is the only durable artifact
  bridging spin-down to demand-driven revival, and staying up is
  cheaper and louder than a full log replay at revival.
  """
  @spec upload_snapshot_before_spindown(State.t()) :: :ok | {:error, term()}
  def upload_snapshot_before_spindown(%State{snapshot: nil}), do: :ok

  def upload_snapshot_before_spindown(%State{snapshot: snapshot} = t) do
    {data_db, index_db} = t.database
    complete_page_map = IndexManager.get_complete_page_map(t.index_manager)
    spindown_data_path = data_db.file_name ++ ~c".spindown"
    spindown_idx_path = index_db.file_name ++ ~c".spindown"

    result =
      with {:ok, writer} <- SplitFileWriter.new(spindown_data_path, spindown_idx_path),
           {:ok, files, _compacted_pages, durable_version} <-
             Database.compact(t.database, complete_page_map, SplitFileWriter, writer),
           _ = :file.close(files.data_fd),
           _ = :file.close(files.idx_fd),
           version_int = Version.to_integer(durable_version),
           {:ok, data} <- File.read(to_string(spindown_data_path)),
           {:ok, idx} <- File.read(to_string(spindown_idx_path)),
           :ok <- Snapshot.write(snapshot, version_int, [data, idx]) do
        Logger.info("Snapshot uploaded to ObjectStorage before idle spin-down", version: version_int)
        :ok
      else
        {:error, reason} ->
          Logger.warning("Snapshot upload before idle spin-down failed", reason: inspect(reason))
          {:error, reason}
      end

    File.rm(to_string(spindown_data_path))
    File.rm(to_string(spindown_idx_path))
    result
  end

  @doc """
  Optionally uploads a snapshot to ObjectStorage after compaction.

  This is a fire-and-forget operation. If snapshot is not configured,
  this is a no-op. If configured, spawns an async task to read the data and
  index files and upload them directly as a bundle (iodata).

  The task logs success or failure but does not affect the caller.

  Compaction only makes the upload CHEAP — the bundle-shaped files are
  already on disk at that instant — it is not itself a reason to pay for
  one. `SnapshotPolicy` is what decides, from the interval and the work
  applied since the last upload; unconfigured, it says yes every time.
  The spin-down upload is deliberately NOT policed: it is the only
  durable artifact bridging spin-down to revival, so it is unconditional.
  """
  @spec maybe_upload_snapshot(
          State.t(),
          data_path :: charlist(),
          idx_path :: charlist(),
          durable_version :: Bedrock.version()
        ) ::
          State.t()
  def maybe_upload_snapshot(%State{snapshot: nil} = t, _data_path, _idx_path, _durable_version), do: t

  def maybe_upload_snapshot(%State{snapshot: snapshot} = t, data_path, idx_path, durable_version) do
    now_in_ms = System.monotonic_time(:millisecond)

    case SnapshotPolicy.decide(t.snapshot_policy, now_in_ms) do
      :wait ->
        t

      :upload ->
        version_int = Version.to_integer(durable_version)

        Task.start(fn ->
          # Read files and upload as iodata (no intermediate bundle file)
          with {:ok, data} <- File.read(to_string(data_path)),
               {:ok, idx} <- File.read(to_string(idx_path)),
               :ok <- Snapshot.write(snapshot, version_int, [data, idx]) do
            Logger.info("Snapshot uploaded to ObjectStorage", version: version_int)
          else
            {:error, reason} ->
              Logger.warning("Snapshot upload failed", version: version_int, reason: inspect(reason))
          end
        end)

        %{t | snapshot_policy: SnapshotPolicy.uploaded(t.snapshot_policy, now_in_ms)}
    end
  end
end
