defmodule Bedrock.DataPlane.Log.Shale.ExplicitAuthorityProtocolTest do
  use ExUnit.Case, async: false

  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.Log.Shale
  alias Bedrock.DataPlane.Log.Shale.Segment
  alias Bedrock.DataPlane.Log.Shale.Server
  alias Bedrock.DataPlane.Materializer.Olivine
  alias Bedrock.DataPlane.Version
  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.LocalFilesystem
  alias Bedrock.Service.Foreman.StartingWorkers
  alias Bedrock.Service.Foreman.WorkerInfo
  alias Bedrock.Service.Manifest
  alias Bedrock.Service.RecoveryControl
  alias Bedrock.Test.DataPlane.TransactionTestSupport

  @moduletag :tmp_dir
  @a %{generation: 7, recovery_id: "owner-a"}
  @foreign %{generation: 8, recovery_id: "owner-b"}
  @new %{generation: 8, recovery_id: "owner-new"}
  @lower %{generation: 6, recovery_id: "owner-old"}

  defmodule TestCluster do
    @moduledoc false
    def name, do: "explicit-authority-test"
    def otp_name(:worker_supervisor), do: :explicit_authority_worker_supervisor
    def otp_name(:foreman), do: :explicit_authority_foreman
    def otp_name_for_worker(id), do: :"explicit_authority_#{id}"
  end

  defmodule OtherCluster do
    @moduledoc false
    def name, do: "other-explicit-authority-test"
    def otp_name_for_worker(id), do: :"other_explicit_authority_#{id}"
  end

  setup %{tmp_dir: root} do
    id = "explicit-authority-log"
    worker = StartingWorkers.initialize_new_worker(id, Shale, %{}, root, TestCluster)
    path = worker.path

    opts = [
      cluster: TestCluster,
      otp_name: TestCluster.otp_name_for_worker(id),
      id: id,
      foreman: self(),
      path: path,
      object_storage: ObjectStorage.backend(LocalFilesystem, root: Path.join(root, "objects"))
    ]

    log = start_supervised!(Server.child_spec(opts))
    eventually(fn -> assert :sys.get_state(log).segment_recycler end)
    {:ok, log: log, path: path, opts: opts}
  end

  test "real Foreman creation publishes marked manifest with no-grant first", %{path: path} do
    assert {File.exists?(control_path(path)), manifest_requires_authority?(path), control_phase(path)} ==
             {true, true, :no_grant}
  end

  test "marked manifest with a deleted control record fails startup", %{log: log, path: path, opts: opts} do
    stop_log(log, opts)
    write_manifest!(path, %{"recovery_authority_protocol" => 1})
    File.rm(control_path(path))
    assert {:error, {:recovery_authority, :missing_after_migration}} = start_server(opts)
  end

  test "Foreman opt-in completes record-before-marker migration and starts the child", %{
    log: log,
    path: path,
    opts: opts
  } do
    stop_log(log, opts)
    legacy = Manifest.new(TestCluster.name(), opts[:id], Shale, %{"kept" => "value"})
    :ok = Manifest.write_to_file(legacy, Path.join(path, "manifest.json"))
    File.write!(control_path(path), encoded_control(:no_grant, creation_identity(opts[:id], Shale)))

    worker = foreman_start(path, opts, allow_recovery_authority_migration: true)

    assert {worker.health, healthy_worker?(worker), manifest_requires_authority?(path), manifest_identity(path)} ==
             {{:ok, healthy_pid(worker)}, true, true, {legacy.cluster, legacy.id, legacy.worker, "value"}}
  end

  test "Foreman rejects an unmarked missing-record worker without migration opt-in", %{
    log: log,
    path: path,
    opts: opts
  } do
    stop_log(log, opts)
    legacy = Manifest.new(TestCluster.name(), opts[:id], Shale, %{"kept" => "value"})
    :ok = Manifest.write_to_file(legacy, Path.join(path, "manifest.json"))
    File.rm(control_path(path))
    assert_foreman_rejects_unchanged(path, opts, [], :migration_required)
  end

  test "Foreman rejects an unmarked no-grant worker without migration opt-in", %{log: log, path: path, opts: opts} do
    stop_log(log, opts)
    write_manifest!(path, %{"kept" => "value"})
    File.write!(control_path(path), encoded_control(:no_grant, creation_identity(opts[:id], Shale)))
    assert_foreman_rejects_unchanged(path, opts, [], :migration_required)
  end

  test "Foreman opt-in migrates unmarked missing-record worker before child start", %{log: log, path: path, opts: opts} do
    stop_log(log, opts)
    write_manifest!(path, %{"kept" => "value"})
    File.rm(control_path(path))
    worker = foreman_start(path, opts, allow_recovery_authority_migration: true)

    assert {{:ok, _pid}, true, true, "value"} =
             {worker.health, Process.alive?(elem(worker.health, 1)), manifest_requires_authority?(path),
              manifest_param(path, "kept")}
  end

  for phase <- [:granted, :replay_started, :replay_complete, :running] do
    test "Foreman rejects unmarked #{phase} record without mutation", %{log: log, path: path, opts: opts} do
      stop_log(log, opts)
      write_manifest!(path, %{"kept" => "value"})
      File.write!(control_path(path), encoded_control(unquote(phase), creation_identity(opts[:id], Shale)))
      assert_foreman_rejects_unchanged(path, opts, [allow_recovery_authority_migration: true], :unsafe_legacy_state)
    end
  end

  for {label, control, reason} <- [
        {:missing, :missing, :missing_after_migration},
        {:corrupt, :corrupt, :corrupt},
        {:future, :future, :future_version}
      ] do
    test "Foreman rejects marked manifest with #{label} record without mutation", %{log: log, path: path, opts: opts} do
      stop_log(log, opts)
      write_manifest!(path, %{"recovery_authority_protocol" => 1, "kept" => "value"})
      write_control_fixture(path, unquote(control), creation_identity(opts[:id], Shale))

      assert_foreman_rejects_unchanged(
        path,
        opts,
        [allow_recovery_authority_migration: true],
        unquote(reason)
      )
    end
  end

  test "Foreman starts a marked worker with a valid supported record", %{log: log, path: path, opts: opts} do
    stop_log(log, opts)
    write_manifest!(path, %{"recovery_authority_protocol" => 1, "kept" => "value"})
    File.write!(control_path(path), encoded_control(:no_grant, creation_identity(opts[:id], Shale)))
    worker = foreman_start(path, opts, [])

    assert {{:ok, _pid}, true, "value"} =
             {worker.health, Process.alive?(elem(worker.health, 1)), manifest_param(path, "kept")}
  end

  test "Foreman rejects a future manifest marker without mutation", %{log: log, path: path, opts: opts} do
    stop_log(log, opts)
    write_manifest!(path, %{"recovery_authority_protocol" => 2, "kept" => "value"})
    File.write!(control_path(path), encoded_control(:no_grant, creation_identity(opts[:id], Shale)))
    assert_foreman_rejects_unchanged(path, opts, [allow_recovery_authority_migration: true], :future_protocol)
  end

  test "direct Shale startup cannot use the Foreman migration option as a bypass", %{log: log, path: path, opts: opts} do
    stop_log(log, opts)
    write_manifest!(path, %{"kept" => "value"})
    File.rm(control_path(path))

    assert {:error, {:recovery_authority, :unprepared_worker_directory}} =
             start_server(Keyword.put(opts, :allow_recovery_authority_migration, true))
  end

  test "direct Olivine startup cannot migrate an unmarked initialized directory", %{tmp_dir: root} do
    {path, opts} = initialized_olivine_fixture(root, marked?: false)

    assert_direct_olivine_rejected_unchanged(
      path,
      Keyword.put(opts, :allow_recovery_authority_migration, true),
      :unprepared_worker_directory
    )
  end

  test "direct Olivine startup rejects a marked directory whose authority record is missing", %{tmp_dir: root} do
    {path, opts} = initialized_olivine_fixture(root, marked?: true)
    assert_direct_olivine_rejected_unchanged(path, opts, :missing_after_migration)
  end

  test "record-before-marker creation cut is resumed only by the same explicit Foreman creation", %{tmp_dir: root} do
    id = "creation-cut-log"
    path = Path.join(root, id)
    File.mkdir_p!(path)
    :ok = Shale.one_time_initialization(path)
    File.write!(Path.join(path, "initialized.sentinel"), "original-worker-data")
    File.write!(control_path(path), encoded_control(:no_grant, creation_identity(id, Shale)))
    initialized_before = worker_data_files(path)

    worker =
      dynamic(StartingWorkers, :initialize_new_worker, [
        id,
        Shale,
        %{},
        root,
        TestCluster,
        [resume_incomplete_creation: id]
      ])

    result = {creation_result(worker), manifest_requires_authority?(path), control_phase(path), worker_data_files(path)}
    assert result == {{:worker, id, path}, true, :no_grant, initialized_before}
  end

  test "creation retry rejects the same id from a different cluster without mutation", %{tmp_dir: root} do
    assert_creation_identity_rejected(root, OtherCluster, Shale)
  end

  test "creation retry rejects the same id for a different worker without mutation", %{tmp_dir: root} do
    assert_creation_identity_rejected(root, TestCluster, Olivine)
  end

  test "same grant lock works from a different task PID", %{log: log} do
    assert {:ok, ^log, _} = in_task(fn -> Log.lock_for_recovery(log, @a) end)
    assert {:ok, ^log, _} = in_task(fn -> Log.lock_for_recovery(log, @a) end)
    assert installed_authority(log) == @a
  end

  test "same grant replay works from a different task PID", %{log: log} do
    assert {:ok, ^log, _} = Log.lock_for_recovery(log, @a)
    assert {:ok, ^log} = in_task(fn -> recover(log, @a, [], Version.zero(), Version.zero()) end)
  end

  test "same grant unlock works from a different task PID", %{log: log} do
    assert {:ok, ^log, _} = Log.lock_for_recovery(log, @a)
    prepare_replay_complete(log, @a)
    assert :ok = in_task(fn -> unlock(log, @a) end)
  end

  for {label, authority, lock_error} <- [
        {"equal foreign", @foreign, :not_lock_owner},
        {"lower", @lower, :newer_epoch_exists}
      ] do
    test "#{label} lock reaches its own rejection", %{log: log} do
      assert {:ok, ^log, _} = Log.lock_for_recovery(log, @new)
      assert {:error, unquote(lock_error)} = Log.lock_for_recovery(log, unquote(Macro.escape(authority)))
    end

    test "#{label} replay reaches its own rejection", %{log: log} do
      assert {:ok, ^log, _} = Log.lock_for_recovery(log, @new)

      assert {:error, :not_lock_owner} =
               recover(log, unquote(Macro.escape(authority)), [], Version.zero(), Version.zero())
    end

    test "#{label} unlock reaches its own rejection", %{log: log} do
      assert {:ok, ^log, _} = Log.lock_for_recovery(log, @new)
      assert {:error, :not_lock_owner} = unlock(log, unquote(Macro.escape(authority)))
    end

    test "#{label} immediate push reaches its own rejection", %{log: log} do
      assert {:ok, ^log, _} = Log.lock_for_recovery(log, @new)

      assert {:error, :not_lock_owner} =
               push(log, unquote(Macro.escape(authority)), tx(100), Version.zero(), Version.zero())
    end

    test "#{label} recovery pull reaches its own rejection", %{log: log} do
      assert {:ok, ^log, _} = Log.lock_for_recovery(log, @new)

      assert {:error, :not_lock_owner} =
               Log.pull(log, Version.zero(),
                 recovery: true,
                 recovery_authority: unquote(Macro.escape(authority)),
                 last_version: Version.zero()
               )
    end
  end

  test("old lock form independently fails closed", %{log: log},
    do: assert({:error, :invalid_recovery_authority} = Log.lock_for_recovery(log, 7))
  )

  test("old replay form independently fails closed", %{log: log},
    do: assert({:error, :invalid_recovery_authority} = Log.recover_from(log, [], Version.zero(), Version.zero()))
  )

  test("old push form independently fails closed", %{log: log},
    do: assert({:error, :invalid_recovery_authority} = Log.push(log, tx(100), Version.zero()))
  )

  test("old recovery pull independently fails closed", %{log: log},
    do:
      assert(
        {:error, :invalid_recovery_authority} =
          Log.pull(log, Version.zero(), recovery: true, last_version: Version.zero())
      )
  )

  test "start_unlocked is rejected", %{opts: opts} do
    assert_raise ArgumentError, ~r/start_unlocked.*recovery authority/, fn ->
      Server.child_spec(Keyword.put(opts, :start_unlocked, true))
    end
  end

  test "an actually parked request retains explicit authority", %{log: log} do
    prepare_running(log, @a)
    v1 = Version.from_integer(100)
    waiter = Task.async(fn -> probe_push(log, @a, tx(200), v1, v1) end)
    eventually(fn -> assert map_size(:sys.get_state(log).pending_pushes) == 1 end)
    assert pending_authorities(log) == [@a]
    Task.shutdown(waiter, :brutal_kill)
  end

  test "higher lock clears an actually parked old request", %{log: log} do
    prepare_running(log, @a)
    v1 = Version.from_integer(100)
    waiter = Task.async(fn -> probe_push(log, @a, tx(200), v1, v1) end)
    eventually(fn -> assert map_size(:sys.get_state(log).pending_pushes) == 1 end)
    assert {:ok, ^log, _} = Log.lock_for_recovery(log, @new)
    assert :sys.get_state(log).pending_pushes == %{}
    Task.shutdown(waiter, :brutal_kill)
  end

  test "equal-generation foreign parked push is rejected at admission", %{log: log} do
    prepare_running(log, @new)
    v1 = Version.from_integer(100)
    waiter = Task.async(fn -> probe_push(log, @foreign, tx(200), v1, v1) end)
    assert {:error, :not_lock_owner} = Task.await(waiter)
    assert :sys.get_state(log).pending_pushes == %{}
  end

  test "equal-generation foreign parked push is revalidated at drain", %{log: log} do
    prepare_running(log, @new)
    v1 = Version.from_integer(100)
    waiter = Task.async(fn -> probe_push(log, @new, tx(200), v1, v1) end)
    eventually(fn -> assert map_size(:sys.get_state(log).pending_pushes) == 1 end)

    :sys.replace_state(log, fn t ->
      entry = Map.fetch!(t.pending_pushes, v1)
      %{t | pending_pushes: Map.put(t.pending_pushes, v1, %{entry | authority: @foreign})}
    end)

    assert :ok = probe_push(log, @new, tx(100), Version.zero(), Version.zero())
    assert {:error, :not_lock_owner} = Task.await(waiter)
  end

  test "identical parked pushes retain two waiters", %{log: log} do
    prepare_running(log, @a)
    v1 = Version.from_integer(100)
    body = tx(200, %{"same" => "bytes"})
    tasks = for _ <- 1..2, do: Task.async(fn -> probe_push(log, @a, body, v1, v1) end)
    eventually(fn -> assert map_size(:sys.get_state(log).pending_pushes) == 1 end)
    assert pending_waiter_count(log, v1) == 2
    Enum.each(tasks, &Task.shutdown(&1, :brutal_kill))
  end

  test "conflicting parked payload cannot replace the first", %{log: log} do
    prepare_running(log, @a)
    v1 = Version.from_integer(100)
    first_tx = tx(200, %{"value" => "first"})
    first = Task.async(fn -> probe_push(log, @a, first_tx, v1, v1) end)
    eventually(fn -> assert map_size(:sys.get_state(log).pending_pushes) == 1 end)
    second = Task.async(fn -> probe_push(log, @a, tx(200, %{"value" => "second"}), v1, v1) end)
    assert {:error, :conflicting_pending_push} = Task.await(second)
    assert pending_transaction(log, v1) == first_tx
    Task.shutdown(first, :brutal_kill)
  end

  test "durable higher fence survives restart", %{log: log, opts: opts} do
    assert {:ok, ^log, _} = Log.lock_for_recovery(log, @new)
    stop_log(log, opts)
    assert {:ok, restarted} = start_server(opts)
    assert {:error, :newer_epoch_exists} = Log.lock_for_recovery(restarted, @lower)
    assert installed_authority(restarted) == @new
  end

  test "replay-complete restart is idempotent", %{log: log, opts: opts} do
    assert {:ok, ^log, _} = Log.lock_for_recovery(log, @a)
    assert {:ok, ^log} = recover(log, @a, [], Version.zero(), Version.zero())
    before = persistent_snapshot(log)
    stop_log(log, opts)
    assert {:ok, restarted} = start_server(opts)
    assert {:ok, ^restarted} = recover(restarted, @a, [], Version.zero(), Version.zero())
    assert persistent_snapshot(restarted) == before
  end

  test "running restart accepts only exact duplicate unlock", %{log: log, opts: opts} do
    prepare_running(log, @a)
    stop_log(log, opts)
    assert {:ok, restarted} = start_server(opts)
    assert :ok = unlock(restarted, @a)
    assert {:error, :not_lock_owner} = unlock(restarted, @foreign)
  end

  test "raw staged replay reaches source and exposes missing source authority", %{log: log} do
    {source, release} = gated_source(self(), [tx(100)])
    assert {:ok, ^log, _} = Log.lock_for_recovery(log, @a)
    replay = Task.async(fn -> probe_recover(log, @a, [source], Version.zero(), Version.from_integer(100)) end)
    assert_receive {:source_pull_entered, ^source, _from, opts}
    release.()
    assert {:ok, ^log} = Task.await(replay)
    assert opts[:recovery_authority] == @a
  end

  test "blocked replay yields to takeover and discards the delayed stale result", %{log: log} do
    {source, release} = gated_source(self(), [tx(100)])
    assert {:ok, ^log, _} = Log.lock_for_recovery(log, @a)
    replay = Task.async(fn -> probe_recover(log, @a, [source], Version.zero(), Version.from_integer(100)) end)
    assert_receive {:source_pull_entered, ^source, _from, _opts}
    replay_pid = :sys.get_state(log).replay_operation.pid
    replay_ref = Process.monitor(replay_pid)
    before = persistent_snapshot(log)
    takeover = Task.async(fn -> Log.lock_for_recovery(log, @new) end)
    assert_receive {:DOWN, ^replay_ref, :process, ^replay_pid, :killed}, 1_000
    assert {:ok, ^log, _} = Task.await(takeover)
    assert {:error, :not_lock_owner} = Task.await(replay)
    after_takeover = persistent_snapshot(log)
    release.()
    Process.sleep(25)
    assert persistent_snapshot(log) == after_takeover
    assert after_takeover.last == before.last
  end

  test "log shutdown synchronously kills a blocked replay", %{log: log, opts: opts} do
    {source, release} = gated_source(self(), [tx(100)])
    assert {:ok, ^log, _} = Log.lock_for_recovery(log, @a)
    caller = Task.async(fn -> probe_recover(log, @a, [source], Version.zero(), Version.from_integer(100)) end)
    assert_receive {:source_pull_entered, ^source, _from, _opts}
    replay_pid = :sys.get_state(log).replay_operation.pid
    replay_ref = Process.monitor(replay_pid)

    assert :ok = GenServer.stop(log)
    assert_receive {:DOWN, ^replay_ref, :process, ^replay_pid, :killed}, 1_000
    assert {:error, :not_lock_owner} = Task.await(caller)
    release.()
    stop_supervised({Server, opts[:id]})
  end

  test "exact durable tail retry is idempotent and conflicting bytes fail", %{log: log} do
    prepare_running(log, @a)
    body = tx(100, %{"value" => "first"})
    assert :ok = push(log, @a, body, Version.zero(), Version.zero())
    before = persistent_snapshot(log)
    assert :ok = push(log, @a, body, Version.zero(), Version.zero())
    assert persistent_snapshot(log) == before

    assert {:error, :conflicting_durable_tail} =
             push(log, @a, tx(100, %{"value" => "other"}), Version.zero(), Version.zero())

    assert persistent_snapshot(log) == before
  end

  test "same owner relock from running durably returns to locked", %{log: log, path: path} do
    prepare_running(log, @a)
    assert :sys.get_state(log).mode == :running
    assert {:ok, ^log, _} = Log.lock_for_recovery(log, @a)
    assert {:locked, :locked} == {:sys.get_state(log).mode, control_phase(path)}
  end

  test "stale required pending predecessor fences every successor", %{log: log} do
    prepare_running(log, @new)
    v1 = Version.from_integer(100)
    v2 = Version.from_integer(200)
    first = Task.async(fn -> push(log, @new, tx(200), v1, v1) end)
    second = Task.async(fn -> push(log, @new, tx(300), v2, v2) end)
    eventually(fn -> assert map_size(:sys.get_state(log).pending_pushes) == 2 end)

    :sys.replace_state(log, fn t ->
      entry = Map.fetch!(t.pending_pushes, v1)
      %{t | pending_pushes: Map.put(t.pending_pushes, v1, %{entry | authority: @foreign})}
    end)

    assert :ok = push(log, @new, tx(100), Version.zero(), Version.zero())
    assert {:error, :not_lock_owner} = Task.await(first)
    assert {:error, :not_lock_owner} = Task.await(second)
    assert :sys.get_state(log).pending_pushes == %{}
  end

  test "replay-complete WAL corruption rejects retry and restart", %{log: log, opts: opts, path: path} do
    assert {:ok, ^log, _} = Log.lock_for_recovery(log, @a)
    assert {:ok, ^log} = recover(log, @a, [], Version.zero(), Version.zero())
    [wal | _] = Path.wildcard(Path.join(path, "wal_*"))
    bytes = File.read!(wal)
    # Corrupt the recovery prefix itself. Bytes after the EOF marker are
    # preallocation and are deliberately outside the logical WAL identity.
    <<prefix::binary-size(4), byte, suffix::binary>> = bytes
    File.write!(wal, prefix <> <<Bitwise.bxor(byte, 1)>> <> suffix)
    assert {:error, :wal_identity_mismatch} = recover(log, @a, [], Version.zero(), Version.zero())
    stop_log(log, opts)
    assert {:error, {:recovery_authority, :wal_identity_mismatch}} = start_server(opts)
  end

  test "replay-complete restart rejects an unexpected higher WAL file", %{log: log, opts: opts, path: path} do
    assert {:ok, ^log, _} = Log.lock_for_recovery(log, @a)
    assert {:ok, ^log} = recover(log, @a, [], Version.zero(), Version.zero())
    [wal | _] = Path.wildcard(Path.join(path, "wal_*"))
    stop_log(log, opts)

    higher = Path.join(path, Segment.encode_file_name(100))
    File.cp!(wal, higher)

    assert {:error, {:recovery_authority, {:wal_identity_unavailable, :unexpected_wal_suffix}}} =
             start_server(opts)
  end

  test "restart rejects a WAL symlink before reading its target", %{log: log, opts: opts, path: path} do
    assert {:ok, ^log, _} = Log.lock_for_recovery(log, @a)
    assert {:ok, ^log} = recover(log, @a, [], Version.zero(), Version.zero())
    [wal | _] = Path.wildcard(Path.join(path, "wal_*"))
    stop_log(log, opts)

    target = Path.join(path, "saved-wal")
    File.rename!(wal, target)
    File.ln_s!(target, wal)

    assert {:error, {:recovery_authority, {:wal_identity_unavailable, :unsafe_wal_file}}} =
             start_server(opts)
  end

  test "running restart accepts and loads legitimate appends beyond its recovery checkpoint", %{
    log: log,
    opts: opts
  } do
    prepare_running(log, @a)
    v1 = Version.from_integer(100)
    assert :ok = push(log, @a, tx(100), Version.zero(), v1)
    stop_log(log, opts)

    assert {:ok, restarted} = start_server(opts)
    assert :sys.get_state(restarted).last_version == v1
  end

  test "complete replay_started cut promotes without rewriting WAL", %{log: log, opts: opts, path: path} do
    assert {:ok, ^log, _} = Log.lock_for_recovery(log, @a)
    assert {:ok, ^log} = recover(log, @a, [], Version.zero(), Version.zero())
    {:ok, complete} = RecoveryControl.load(path)
    started = RecoveryControl.replay_started(complete, @a, Version.zero(), Version.zero())
    :ok = RecoveryControl.write(path, started)
    before = wal_files(path)
    stop_log(log, opts)
    assert {:ok, restarted} = start_server(opts)
    assert {:ok, ^restarted} = recover(restarted, @a, [], Version.zero(), Version.zero())
    assert wal_files(path) == before
    assert control_phase(path) == :replay_complete
  end

  # Test-only staged probes keep deeper REDs executable before new arities exist.
  defp recover(log, auth, sources, after_v, last_v),
    do: dynamic(Log, :recover_from, [log, auth, sources, after_v, last_v])

  defp unlock(log, auth), do: dynamic(Log, :unlock_after_recovery, [log, auth])

  defp push(log, auth, body, predecessor, kcv),
    do: dynamic(Log, :push, [log, auth, body, predecessor, [known_committed_version: kcv]])

  defp probe_recover(log, auth, sources, after_v, last_v) do
    if function_exported?(Log, :recover_from, 5),
      do: recover(log, auth, sources, after_v, last_v),
      else: Log.recover_from(log, sources, after_v, last_v)
  end

  defp probe_push(log, auth, body, predecessor, kcv) do
    if function_exported?(Log, :push, 5),
      do: push(log, auth, body, predecessor, kcv),
      else: Log.push(log, body, predecessor, known_committed_version: kcv)
  end

  defp prepare_replay_complete(log, auth) do
    assert {:ok, ^log} = probe_recover(log, auth, [], Version.zero(), Version.zero())
    :ok
  end

  defp prepare_running(log, auth) do
    assert {:ok, ^log, _} = Log.lock_for_recovery(log, auth)
    prepare_replay_complete(log, auth)
    if function_exported?(Log, :unlock_after_recovery, 2), do: assert(:ok = unlock(log, auth))
    :ok
  end

  defp dynamic(module, function, args),
    do:
      if(function_exported?(module, function, length(args)),
        do: apply(module, function, args),
        else: {:error, :explicit_authority_protocol_missing}
      )

  defp gated_source(test, transactions) do
    source =
      spawn_link(fn ->
        receive do
          {:"$gen_call", from, {:pull, _cursor, opts}} ->
            send(test, {:source_pull_entered, self(), from, opts})
            receive do: (:release -> GenServer.reply(from, {:ok, transactions}))
        end
      end)

    {source, fn -> send(source, :release) end}
  end

  defp tx(version, data \\ %{}), do: TransactionTestSupport.new_log_transaction(version, data)
  defp control_path(path), do: Path.join(path, ".recovery-authority-v1")

  defp creation_identity(id, worker, cluster \\ TestCluster),
    do: %{cluster: cluster.name(), service_id: id, worker: worker |> Module.split() |> Enum.join(".")}

  defp encoded_control(phase, identity) do
    base = %RecoveryControl{creation: identity, phase: :no_grant}

    record =
      case phase do
        :no_grant ->
          base

        :granted ->
          RecoveryControl.locked(base, @a)

        :replay_started ->
          RecoveryControl.replay_started(base, @a, Version.zero(), Version.zero())

        :replay_complete ->
          base
          |> RecoveryControl.replay_started(@a, Version.zero(), Version.zero())
          |> RecoveryControl.replay_complete(wal_identity(Version.zero()))

        :running ->
          base
          |> RecoveryControl.replay_started(@a, Version.zero(), Version.zero())
          |> RecoveryControl.replay_complete(wal_identity(Version.zero()))
          |> RecoveryControl.running()
      end

    RecoveryControl.encode(record)
  end

  defp control_phase(path) do
    case RecoveryControl.load(path) do
      {:ok, record} -> record.phase
      _ -> :missing
    end
  end

  defp wal_identity(last_version), do: %{last_version: last_version, files_digest: :crypto.hash(:sha256, "test-wal")}

  defp foreman_start(path, opts, migration_opts) do
    start_supervised!({DynamicSupervisor, strategy: :one_for_one, name: TestCluster.otp_name(:worker_supervisor)})

    worker_info = %WorkerInfo{
      id: opts[:id],
      path: path,
      otp_name: opts[:otp_name],
      health: :stopped
    }

    args = [worker_info, TestCluster, opts[:object_storage], migration_opts]

    if function_exported?(StartingWorkers, :try_to_start_worker, 4),
      do: apply(StartingWorkers, :try_to_start_worker, args),
      else: StartingWorkers.try_to_start_worker(worker_info, TestCluster, opts[:object_storage])
  end

  defp assert_foreman_rejects_unchanged(path, opts, migration_opts, reason) do
    before = all_files(path)
    worker = foreman_start(path, opts, migration_opts)

    assert {worker.health, Process.whereis(opts[:otp_name]), all_files(path)} ==
             {{:failed_to_start, {:recovery_authority, reason}}, nil, before}
  end

  defp assert_creation_identity_rejected(root, requested_cluster, requested_worker) do
    id = "creation-identity-log"
    path = Path.join(root, id)
    File.mkdir_p!(path)
    :ok = Shale.one_time_initialization(path)
    File.write!(Path.join(path, "initialized.sentinel"), "original-worker-data")
    File.write!(control_path(path), encoded_control(:no_grant, creation_identity(id, Shale)))
    before = all_files(path)

    result =
      dynamic(StartingWorkers, :initialize_new_worker, [
        id,
        requested_worker,
        %{},
        root,
        requested_cluster,
        [resume_incomplete_creation: id]
      ])

    health = if is_map(result), do: result.health, else: result

    assert {health, all_files(path)} ==
             {{:failed_to_start, {:recovery_authority, :creation_identity_mismatch}}, before}
  end

  defp initialized_olivine_fixture(root, marked?: marked?) do
    id = "direct-olivine-#{System.unique_integer([:positive])}"
    worker = StartingWorkers.initialize_new_worker(id, Olivine, %{}, root, TestCluster)

    opts = [
      cluster: TestCluster,
      otp_name: TestCluster.otp_name_for_worker(id),
      id: id,
      foreman: self(),
      path: worker.path,
      params: %{}
    ]

    assert {:ok, pid} = start_olivine_and_observe(opts)
    GenServer.stop(pid)
    eventually(fn -> assert Process.whereis(opts[:otp_name]) == nil end)
    assert File.regular?(Path.join(worker.path, "data"))
    assert File.regular?(Path.join(worker.path, "idx"))

    params = if marked?, do: %{"recovery_authority_protocol" => 1}, else: %{}
    write_manifest_for!(worker.path, id, Olivine, params)
    File.rm(control_path(worker.path))
    {worker.path, opts}
  end

  defp assert_direct_olivine_rejected_unchanged(path, opts, reason) do
    before = all_files(path)
    result = start_olivine_and_observe(opts)

    on_exit(fn ->
      if pid = Process.whereis(opts[:otp_name]), do: GenServer.stop(pid)
    end)

    assert {result, Process.whereis(opts[:otp_name]), all_files(path)} ==
             {{:error, {:recovery_authority, reason}}, nil, before}
  end

  defp start_olivine_and_observe(opts) do
    %{start: {GenServer, :start_link, [Olivine.Server, init_args, start_opts]}} = Olivine.child_spec(opts)
    id = opts[:id]

    case GenServer.start(Olivine.Server, init_args, start_opts) do
      {:ok, pid} ->
        ref = Process.monitor(pid)

        receive do
          {:"$gen_cast", {:worker_health, ^id, {:ok, ^pid}}} ->
            Process.demonitor(ref, [:flush])
            {:ok, pid}

          {:DOWN, ^ref, :process, ^pid, reason} ->
            {:error, reason}
        after
          2_000 ->
            {:error, :startup_observation_timeout}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp write_control_fixture(path, :missing, _identity), do: File.rm(control_path(path))
  defp write_control_fixture(path, :corrupt, _identity), do: File.write!(control_path(path), "corrupt")

  defp write_control_fixture(path, :future, identity) do
    record = %RecoveryControl{creation: identity, phase: :no_grant}
    File.write!(control_path(path), RecoveryControl.encode(record, 2))
  end

  defp manifest_param(path, key) do
    {:ok, manifest} = Manifest.load_from_file(Path.join(path, "manifest.json"))
    manifest.params[key]
  end

  defp manifest_identity(path) do
    {:ok, manifest} = Manifest.load_from_file(Path.join(path, "manifest.json"))
    {manifest.cluster, manifest.id, manifest.worker, manifest.params["kept"]}
  end

  defp healthy_pid(%{health: {:ok, pid}}), do: pid
  defp healthy_pid(_), do: nil
  defp healthy_worker?(worker), do: worker |> healthy_pid() |> is_pid() && Process.alive?(healthy_pid(worker))
  defp creation_result(%WorkerInfo{id: id, path: path}), do: {:worker, id, path}
  defp creation_result(other), do: other

  defp worker_data_files(path),
    do: Enum.reject(all_files(path), fn {name, _} -> name in ["manifest.json", ".recovery-authority-v1"] end)

  defp installed_authority(log) do
    case log |> :sys.get_state() |> Map.get(:recovery_authority, :missing) do
      %Bedrock.Service.RecoveryAuthority{} = authority -> Map.from_struct(authority)
      authority -> authority
    end
  end

  defp persistent_snapshot(log) do
    state = :sys.get_state(log)
    %{files: all_files(state.path), mode: state.mode, last: state.last_version, pending: state.pending_pushes}
  end

  defp pending_authorities(log),
    do:
      log
      |> :sys.get_state()
      |> Map.fetch!(:pending_pushes)
      |> Map.values()
      |> Enum.map(fn entry -> if is_map(entry), do: entry[:authority], else: :missing end)

  defp pending_waiter_count(log, key) do
    case Map.fetch!(:sys.get_state(log).pending_pushes, key) do
      %{waiters: ws} -> length(ws)
      {_tx, _from} -> 1
    end
  end

  defp pending_transaction(log, key) do
    case Map.fetch!(:sys.get_state(log).pending_pushes, key) do
      %{transaction: body} -> body
      {body, _from} -> body
    end
  end

  defp manifest_requires_authority?(path) do
    case Manifest.load_from_file(Path.join(path, "manifest.json")) do
      {:ok, manifest} -> manifest.params["recovery_authority_protocol"] == 1
      _ -> false
    end
  end

  defp write_manifest!(path, params), do: write_manifest_for!(path, "explicit-authority-log", Shale, params)

  defp write_manifest_for!(path, id, worker, params),
    do: Manifest.write_to_file(Manifest.new(TestCluster.name(), id, worker, params), Path.join(path, "manifest.json"))

  defp all_files(path),
    do:
      path
      |> Path.join("*")
      |> Path.wildcard(match_dot: true)
      |> Enum.filter(&File.regular?/1)
      |> Enum.sort()
      |> Enum.map(&{Path.basename(&1), File.read!(&1)})

  defp wal_files(path), do: path |> all_files() |> Enum.filter(fn {name, _} -> String.starts_with?(name, "wal_") end)

  defp stop_log(log, opts), do: if(Process.alive?(log), do: stop_supervised({Server, opts[:id]}), else: :ok)

  defp start_server(opts) do
    case start_supervised(Server.child_spec(opts)) do
      {:ok, pid} ->
        eventually(fn -> assert :sys.get_state(pid).segment_recycler end)
        {:ok, pid}

      {:error, {reason, {:child, _, _, _, _, _, _, _, _}}} ->
        {:error, reason}

      error ->
        error
    end
  catch
    :exit, reason -> {:error, reason}
  end

  defp in_task(fun), do: fun |> Task.async() |> Task.await(5_000)
  defp eventually(fun, timeout \\ 1_000), do: eventually_loop(fun, System.monotonic_time(:millisecond) + timeout)

  defp eventually_loop(fun, deadline) do
    fun.()
  rescue
    error in [ExUnit.AssertionError] ->
      if System.monotonic_time(:millisecond) < deadline do
        Process.sleep(10)
        eventually_loop(fun, deadline)
      else
        reraise error, __STACKTRACE__
      end
  end
end
