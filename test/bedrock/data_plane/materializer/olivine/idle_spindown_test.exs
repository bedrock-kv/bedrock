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

  alias Bedrock.DataPlane.Materializer.Olivine
  alias Bedrock.DataPlane.Version
  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.Config, as: ObjectStorageConfig
  alias Bedrock.ObjectStorage.Keys, as: ObjectStorageKeys
  alias Bedrock.ObjectStorage.LocalFilesystem
  alias Bedrock.ObjectStorage.Snapshot

  defmodule TestCluster do
    @moduledoc false
    def name, do: "idle-spindown-test-cluster"
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

  describe "exemption" do
    test "a worker without an explicit idle_timeout never spins down", %{tmp_dir: tmp_dir} do
      {pid, _worker_id, _otp_name} = start_worker(tmp_dir, %{"shard_id" => 42})
      ref = Process.monitor(pid)

      send(pid, :idle_check)

      refute_receive {:DOWN, ^ref, :process, ^pid, _}, 200
      assert Process.alive?(pid)
    end
  end

  defp snapshot_handle_for(shard_num) do
    tag = ObjectStorageKeys.shard_tag(shard_num)
    Snapshot.new(ObjectStorageConfig.backend(), tag)
  end
end
