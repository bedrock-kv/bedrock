defmodule Bedrock.DataPlane.Materializer.Olivine.IdleSpindownTest do
  @moduledoc """
  Idle spin-down (bedrock-q67.21.5): a worker opted in via the
  "idle_timeout" manifest param exits {:shutdown, :idle} after a
  read-inactivity window — only client reads count as activity — after
  best-effort uploading a snapshot and arranging its own foreman
  removal. Workers without the param never spin down (the system
  shard's exemption).

  Deterministic technique: :sys.replace_state/2 rewinds last_read_at,
  then send(pid, :idle_check) drives the periodic check by hand. The
  foreman is the test process, so the deferred remove_worker call is
  assertable as a plain GenServer call message.
  """
  use ExUnit.Case, async: false

  alias Bedrock.ControlPlane.Director.Recovery
  alias Bedrock.ControlPlane.Director.State, as: DirectorState
  alias Bedrock.ControlPlane.Distributor.Recruitment
  alias Bedrock.DataPlane.Materializer.Olivine
  alias Bedrock.DataPlane.Materializer.Olivine.Database
  alias Bedrock.DataPlane.Materializer.Olivine.IndexManager
  alias Bedrock.DataPlane.Materializer.Olivine.Logic
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version
  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.Config, as: ObjectStorageConfig
  alias Bedrock.ObjectStorage.Keys, as: ObjectStorageKeys
  alias Bedrock.ObjectStorage.LocalFilesystem
  alias Bedrock.ObjectStorage.Snapshot

  defmodule TestCluster do
    @moduledoc false
    def name, do: "idle-spindown-test-cluster"
    def otp_name(:foreman), do: :idle_spindown_test_foreman
    def otp_name_for_worker(worker_id), do: :"idle_spindown_test_#{worker_id}"
  end

  setup do
    test_id = :erlang.unique_integer([:positive])
    tmp_dir = Path.join(System.tmp_dir!(), "olivine_idle_#{test_id}")
    File.rm_rf(tmp_dir)
    File.mkdir_p!(tmp_dir)
    on_exit(fn -> File.rm_rf(tmp_dir) end)
    {:ok, tmp_dir: tmp_dir, test_id: test_id}
  end

  defp start_worker(tmp_dir, params, opts \\ []) do
    worker_id = "idle_wkr_#{System.unique_integer([:positive])}"
    otp_name = :"olivine_idle_#{System.unique_integer([:positive])}"

    child_spec =
      Olivine.child_spec(
        Keyword.merge(
          [otp_name: otp_name, foreman: self(), id: worker_id, path: tmp_dir, params: params],
          opts
        )
      )

    # Unlinked: the {:shutdown, :idle} exit is the observable under test
    # and must reach the monitor, not tear the test down through a link.
    {GenServer, :start_link, args} = child_spec.start
    {:ok, pid} = apply(GenServer, :start, args)

    receive do
      {:"$gen_cast", {:worker_health, ^worker_id, {:ok, ^pid}}} -> :ok
    after
      5_000 -> flunk("no health report")
    end

    # Workers boot :locked and only recovery's unlock makes them
    # :running; idle spin-down defers while locked (the director is
    # counting on a locked worker). These tests exercise the running
    # steady state.
    :sys.replace_state(pid, &%{&1 | mode: :running})

    {pid, worker_id, otp_name}
  end

  defp rewind_idle_clock(pid, by_ms) do
    :sys.replace_state(pid, fn t -> %{t | last_read_at: System.monotonic_time(:millisecond) - by_ms} end)
  end

  describe "idle expiry" do
    test "with no reads the worker exits {:shutdown, :idle}, emits telemetry, and requests foreman removal",
         %{tmp_dir: tmp_dir} do
      handler_id = "idle-spindown-#{System.unique_integer([:positive])}"
      test_pid = self()

      :telemetry.attach(
        handler_id,
        [:bedrock, :materializer, :idle_spindown],
        fn _event, measurements, _meta, _cfg -> send(test_pid, {:spindown_telemetry, measurements}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler_id) end)

      {pid, worker_id, _otp_name} = start_worker(tmp_dir, %{"idle_timeout" => 60_000})
      ref = Process.monitor(pid)

      rewind_idle_clock(pid, 120_000)
      send(pid, :idle_check)

      assert_receive {:DOWN, ^ref, :process, ^pid, {:shutdown, :idle}}, 5_000
      assert_receive {:spindown_telemetry, %{idle_duration_ms: idle_ms}}
      assert idle_ms >= 60_000

      # The deferred self-removal calls the foreman only AFTER the exit.
      assert_receive {:"$gen_call", from, {:remove_worker, ^worker_id}}, 5_000
      GenServer.reply(from, :ok)
    end

    test "a real (short) idle_timeout expires on the periodic check without manual driving", %{tmp_dir: tmp_dir} do
      {pid, _worker_id, _otp_name} = start_worker(tmp_dir, %{"idle_timeout" => 50})
      ref = Process.monitor(pid)

      assert_receive {:DOWN, ^ref, :process, ^pid, {:shutdown, :idle}}, 5_000
    end
  end

  describe "activity tracking" do
    test "a read resets the idle timer", %{tmp_dir: tmp_dir} do
      {pid, _worker_id, _otp_name} = start_worker(tmp_dir, %{"idle_timeout" => 60_000})
      ref = Process.monitor(pid)

      rewind_idle_clock(pid, 120_000)

      # The read touches last_read_at before the check runs; the reply
      # itself (found or not) is irrelevant to the timer.
      _ = GenServer.call(pid, {:get, "k", Version.from_integer(0), [timeout: 100]}, 500)
      send(pid, :idle_check)

      refute_receive {:DOWN, ^ref, :process, ^pid, _}, 200
      assert Process.alive?(pid)
    end

    test "transaction application does NOT count as activity", %{tmp_dir: tmp_dir} do
      {pid, _worker_id, _otp_name} = start_worker(tmp_dir, %{"idle_timeout" => 60_000})
      ref = Process.monitor(pid)

      rewind_idle_clock(pid, 120_000)
      send(pid, {:apply_transactions, []})
      send(pid, :idle_check)

      assert_receive {:DOWN, ^ref, :process, ^pid, {:shutdown, :idle}}, 5_000
    end
  end

  describe "snapshot upload on idle exit" do
    test "a configured snapshot is uploaded before the worker stops", %{tmp_dir: tmp_dir, test_id: test_id} do
      object_storage_root = Path.join(System.tmp_dir!(), "olivine_idle_objects_#{test_id}")
      File.mkdir_p!(object_storage_root)
      backend = ObjectStorage.backend(LocalFilesystem, root: object_storage_root)

      old_config = Application.get_env(:bedrock, ObjectStorage)
      Application.put_env(:bedrock, ObjectStorage, backend: backend)

      on_exit(fn ->
        if old_config,
          do: Application.put_env(:bedrock, ObjectStorage, old_config),
          else: Application.delete_env(:bedrock, ObjectStorage)

        File.rm_rf(object_storage_root)
      end)

      {pid, worker_id, _otp_name} =
        start_worker(tmp_dir, %{"idle_timeout" => 60_000, "shard_id" => 42}, cluster: TestCluster)

      ref = Process.monitor(pid)
      rewind_idle_clock(pid, 120_000)
      send(pid, :idle_check)

      assert_receive {:DOWN, ^ref, :process, ^pid, {:shutdown, :idle}}, 5_000

      # The upload is synchronous and precedes the exit, so by the time
      # the DOWN arrives the snapshot is discoverable.
      # The worker never applied a transaction, so the bundle is small —
      # what matters is that it EXISTS and is discoverable before the
      # exit: it is the only durable artifact bridging spin-down to
      # demand-driven revival.
      snapshot = %Snapshot{} = snapshot_handle_for(42)
      assert {:ok, _version, _data} = Snapshot.read_latest(snapshot)

      assert_receive {:"$gen_call", from, {:remove_worker, ^worker_id}}, 5_000
      GenServer.reply(from, :ok)
    end
  end

  describe "re-adoption identity coexists with idle spin-down" do
    test "a worker with shard_id, no cluster, and idle_timeout exposes :shard_id and still honors idle_check",
         %{tmp_dir: tmp_dir} do
      {pid, _worker_id, _otp_name} = start_worker(tmp_dir, %{"idle_timeout" => 60_000, "shard_id" => 42})

      assert {:ok, %{shard_id: 42}} = GenServer.call(pid, {:info, [:shard_id]})

      ref = Process.monitor(pid)
      rewind_idle_clock(pid, 120_000)
      send(pid, :idle_check)

      assert_receive {:DOWN, ^ref, :process, ^pid, {:shutdown, :idle}}, 5_000
    end
  end

  describe "spin-down deferral" do
    test "a locked worker never spins down — the idle check re-arms instead", %{tmp_dir: tmp_dir} do
      {pid, _worker_id, _otp_name} = start_worker(tmp_dir, %{"idle_timeout" => 60_000})
      :sys.replace_state(pid, &%{&1 | mode: :locked})
      ref = Process.monitor(pid)

      rewind_idle_clock(pid, 120_000)
      send(pid, :idle_check)

      refute_receive {:DOWN, ^ref, :process, ^pid, _}, 200
      assert Process.alive?(pid)
    end

    test "an active compaction defers the spin-down", %{tmp_dir: tmp_dir} do
      {pid, _worker_id, _otp_name} = start_worker(tmp_dir, %{"idle_timeout" => 60_000})
      fake_task = %Task{ref: make_ref(), pid: self(), owner: pid, mfa: {__MODULE__, :noop, 0}}
      :sys.replace_state(pid, &%{&1 | compaction_task: fake_task})
      ref = Process.monitor(pid)

      rewind_idle_clock(pid, 120_000)
      send(pid, :idle_check)

      refute_receive {:DOWN, ^ref, :process, ^pid, _}, 200
      assert Process.alive?(pid)
    end

    test "a failed snapshot upload aborts the spin-down — the worker stays up and re-arms",
         %{tmp_dir: tmp_dir, test_id: test_id} do
      # An unwritable ObjectStorage root: the upload must fail, and the
      # worker must NOT exit — the snapshot is the only durable artifact
      # bridging spin-down to revival.
      unwritable_root = Path.join(System.tmp_dir!(), "olivine_idle_unwritable_#{test_id}")
      File.mkdir_p!(unwritable_root)
      File.chmod!(unwritable_root, 0o444)
      backend = ObjectStorage.backend(LocalFilesystem, root: unwritable_root)

      old_config = Application.get_env(:bedrock, ObjectStorage)
      Application.put_env(:bedrock, ObjectStorage, backend: backend)

      on_exit(fn ->
        if old_config,
          do: Application.put_env(:bedrock, ObjectStorage, old_config),
          else: Application.delete_env(:bedrock, ObjectStorage)

        File.chmod(unwritable_root, 0o755)
        File.rm_rf(unwritable_root)
      end)

      {pid, _worker_id, _otp_name} =
        start_worker(tmp_dir, %{"idle_timeout" => 60_000, "shard_id" => 42}, cluster: TestCluster)

      ref = Process.monitor(pid)
      rewind_idle_clock(pid, 120_000)

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          send(pid, :idle_check)
          refute_receive {:DOWN, ^ref, :process, ^pid, _}, 300
        end)

      assert Process.alive?(pid)
      assert log =~ "Idle spin-down aborted"
    end
  end

  describe "exemption" do
    test "a worker without an explicit idle_timeout never spins down", %{tmp_dir: tmp_dir} do
      {pid, _worker_id, _otp_name} = start_worker(tmp_dir, %{"shard_id" => 42})
      ref = Process.monitor(pid)

      send(pid, :idle_check)

      refute_receive {:DOWN, ^ref, :process, ^pid, _}, 200
      assert Process.alive?(pid)
    end
  end

  describe "production wiring" do
    # bedrock-q67.21.8: the whole path, end to end. The cluster config's
    # materializer_idle_timeout_ms rides the director's recruitment_ctx
    # as a worker param, Recruitment merges it into the params the
    # foreman persists in the manifest, and the worker the foreman then
    # starts spins itself down. Nothing below writes "idle_timeout" by
    # hand — the only knob the test sets is the cluster parameter.
    test "a materializer recruited the way production recruits one spins down after the configured idle period",
         %{tmp_dir: tmp_dir} do
      {_ctx, {:ok, pid, _node, _worker_id}} = recruit_through_the_director(tmp_dir, 50)
      ref = Process.monitor(pid)

      assert_receive {:DOWN, ^ref, :process, ^pid, {:shutdown, :idle}}, 5_000
    end

    # The escape hatch the parameter documents: zero is not a
    # zero-length window, it is off. The worker's opt-in gate
    # (is_integer and > 0) is what makes it so — and the param has to
    # ARRIVE as zero for that to mean anything, hence the first
    # assertion (without it the test passes on a worker that was handed
    # no param at all).
    test "a cluster that sets the timeout to zero recruits a worker that never spins down",
         %{tmp_dir: tmp_dir} do
      {ctx, {:ok, pid, _node, _worker_id}} = recruit_through_the_director(tmp_dir, 0)
      ref = Process.monitor(pid)

      assert ctx.worker_params == %{"idle_timeout" => 0}
      refute_receive {:DOWN, ^ref, :process, ^pid, _}, 300
      assert Process.alive?(pid)
      GenServer.stop(pid)
    end
  end

  defp recruit_through_the_director(tmp_dir, idle_timeout_ms) do
    ctx =
      %{materializer_idle_timeout_ms: idle_timeout_ms}
      |> recruitment_ctx_from_director()
      |> Map.merge(%{
        node_capabilities: %{materializer: [node()]},
        logs: %{"log_1" => [7]},
        log_refs: %{"log_1" => spawn(fn -> Process.sleep(:infinity) end)},
        create_worker_fn: &start_worker_as_foreman(tmp_dir, &1, &2, &3, &4)
      })

    {ctx, Recruitment.recruit(7, ctx)}
  end

  # The director's own recruitment context, captured at the seam where
  # it hands one to the distributor.
  defp recruitment_ctx_from_director(parameters) do
    test_pid = self()
    stub = spawn(fn -> Process.sleep(:infinity) end)

    Recovery.maybe_start_distributor(%DirectorState{
      state: :running,
      epoch: 1,
      cluster: TestCluster,
      config: %{parameters: parameters},
      transaction_system_layout: %{
        epoch: 1,
        sequencer: self(),
        proxies: [self()],
        resolvers: [],
        logs: %{}
      },
      distributor_start_fn: fn opts ->
        send(test_pid, {:recruitment_ctx, opts[:recruitment_ctx]})
        {:ok, stub}
      end
    })

    assert_received {:recruitment_ctx, ctx}
    ctx
  end

  # Stands in for Foreman.new_worker/4 on the one axis under test: the
  # manifest params it is handed become the worker's child_spec params,
  # the way Foreman.StartingWorkers.build_child_spec/1 hands them over.
  # It is not full foreman fidelity — no cluster and no object_storage,
  # so the spin-down snapshot is a no-op here (covered elsewhere in this
  # file).
  defp start_worker_as_foreman(tmp_dir, _foreman_ref, worker_id, :materializer, opts) do
    otp_name = TestCluster.otp_name_for_worker(worker_id)
    working_dir = Path.join(tmp_dir, worker_id)
    File.mkdir_p!(working_dir)

    child_spec =
      Olivine.child_spec(
        otp_name: otp_name,
        foreman: self(),
        id: worker_id,
        path: working_dir,
        params: opts[:params]
      )

    {GenServer, :start_link, args} = child_spec.start
    {:ok, _pid} = apply(GenServer, :start, args)

    {:ok, otp_name}
  end

  describe "the spin-down snapshot restores" do
    # The pin that matters most: the uploaded bundle must round-trip
    # through the cold-start restore path. The live idx file is a delta
    # chain (one record per window advance) that the bundle format
    # cannot represent — uploading it raw restores at best partially and
    # at worst as a silently empty shard, poisoned permanently by
    # put-if-not-exists. The upload therefore compacts first; this test
    # drives ≥2 window flushes to force a multi-record live chain, spins
    # down, and restores into a FRESH directory.
    test "a multi-window-flush shard spins down and revives from the bundle with its data intact",
         %{tmp_dir: tmp_dir, test_id: test_id} do
      {root, dir_a, dir_b} = object_storage_and_dirs(tmp_dir, test_id)
      _ = root

      {:ok, state} =
        Logic.startup(:"idle_rt_a_#{test_id}", self(), "rt_wkr_a", dir_a, cluster: TestCluster, shard_id: 42)

      # Two applies, each far enough apart that the 5s (in version-time)
      # window lag evicts the earlier one — two flushes, two idx records.
      state = apply_and_flush(state, "k1", "v1", 10_000_000)
      state = apply_and_flush(state, "k2", "v2", 20_000_000)
      durable = Database.durable_version(state.database)
      assert Version.to_integer(durable) > 0

      assert :ok = Logic.upload_snapshot_before_spindown(state)
      Logic.shutdown(state)

      # Cold start in a fresh directory: discovery + restore from the
      # uploaded bundle.
      {:ok, restored} =
        Logic.startup(:"idle_rt_b_#{test_id}", self(), "rt_wkr_b", dir_b, cluster: TestCluster, shard_id: 42)

      assert Database.durable_version(restored.database) == durable
      assert IndexManager.info(restored.index_manager, :n_keys) >= 1
      Logic.shutdown(restored)
    end

    test "a never-written shard's spin-down bundle does not poison revival", %{tmp_dir: tmp_dir, test_id: test_id} do
      {_root, dir_a, dir_b} = object_storage_and_dirs(tmp_dir, test_id)

      {:ok, state} =
        Logic.startup(:"idle_empty_a_#{test_id}", self(), "empty_wkr_a", dir_a, cluster: TestCluster, shard_id: 42)

      assert :ok = Logic.upload_snapshot_before_spindown(state)
      Logic.shutdown(state)

      # The revival must not hard-fail on the uploaded bundle — a
      # poisoned version-0 object would block every recruit forever.
      assert {:ok, restored} =
               Logic.startup(:"idle_empty_b_#{test_id}", self(), "empty_wkr_b", dir_b,
                 cluster: TestCluster,
                 shard_id: 42
               )

      Logic.shutdown(restored)
    end
  end

  defp object_storage_and_dirs(tmp_dir, test_id) do
    root = Path.join(System.tmp_dir!(), "olivine_idle_rt_objects_#{test_id}")
    File.mkdir_p!(root)
    backend = ObjectStorage.backend(LocalFilesystem, root: root)

    old_config = Application.get_env(:bedrock, ObjectStorage)
    Application.put_env(:bedrock, ObjectStorage, backend: backend)

    on_exit(fn ->
      if old_config,
        do: Application.put_env(:bedrock, ObjectStorage, old_config),
        else: Application.delete_env(:bedrock, ObjectStorage)

      File.rm_rf(root)
    end)

    dir_a = Path.join(tmp_dir, "a")
    dir_b = Path.join(tmp_dir, "b")
    File.mkdir_p!(dir_a)
    File.mkdir_p!(dir_b)
    {root, dir_a, dir_b}
  end

  defp apply_and_flush(state, key, value, version_int) do
    encoded = Transaction.encode(%{mutations: [{:set, key, value}], read_conflicts: {nil, []}, write_conflicts: []})
    {:ok, with_version} = Transaction.add_commit_version(encoded, Version.from_integer(version_int))

    {:ok, state, _version} = Logic.apply_transactions(state, [with_version])
    state = %{state | known_committed_version: Version.from_integer(version_int)}
    {:ok, state} = Logic.advance_window(state)
    state
  end

  defp snapshot_handle_for(shard_num) do
    tag = ObjectStorageKeys.shard_tag(shard_num)
    Snapshot.new(ObjectStorageConfig.backend(), tag)
  end
end
