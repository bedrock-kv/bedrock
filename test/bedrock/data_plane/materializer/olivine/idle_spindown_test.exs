defmodule Bedrock.DataPlane.Materializer.Olivine.IdleSpindownTest do
  @moduledoc """
  Idle spin-down (bedrock-q67.13): a materializer with a configured
  `idle_timeout` and no client reads shuts itself down with reason
  `{:shutdown, :idle}`, best-effort uploads a snapshot when one is
  configured, and asks its foreman to remove the worker entry (which
  reclaims the on-disk working directory).

  Tests are deterministic: they rewind the tracked `last_read_at`
  timestamp via `:sys.replace_state/2` and drive the periodic check by
  sending `:idle_check` directly, instead of sleeping.
  """
  use ExUnit.Case, async: true

  import Bedrock.Test.DataPlane.TransactionTestSupport, only: [new_log_transaction: 2]

  alias Bedrock.DataPlane.Materializer
  alias Bedrock.DataPlane.Materializer.Olivine
  alias Bedrock.DataPlane.Version
  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.LocalFilesystem
  alias Bedrock.ObjectStorage.Snapshot

  @idle_timeout 60_000

  setup do
    tmp_dir = "/tmp/olivine_idle_#{System.unique_integer([:positive])}"
    File.rm_rf(tmp_dir)
    File.mkdir_p!(tmp_dir)
    on_exit(fn -> File.rm_rf(tmp_dir) end)
    {:ok, tmp_dir: tmp_dir}
  end

  defp start_worker(tmp_dir, params) do
    worker_id = "worker_#{System.unique_integer([:positive])}"
    otp_name = :"olivine_idle_#{System.unique_integer([:positive])}"

    child_spec =
      Olivine.child_spec(
        otp_name: otp_name,
        foreman: self(),
        id: worker_id,
        path: tmp_dir,
        params: params
      )

    {:ok, pid} = start_supervised(Map.put(child_spec, :restart, :temporary))

    # Wait for the worker's health report so startup has completed.
    assert_receive {:"$gen_cast", {:worker_health, ^worker_id, {:ok, ^pid}}}, 5_000

    {worker_id, pid}
  end

  defp rewind_last_read(pid, ms) do
    :sys.replace_state(pid, fn state ->
      %{state | last_read_at: System.monotonic_time(:millisecond) - ms}
    end)
  end

  defp attach_spindown_telemetry(test_pid) do
    handler_id = "idle-spindown-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler_id,
      [:bedrock, :materializer, :idle_spindown],
      fn event, measurements, metadata, _ -> send(test_pid, {:telemetry, event, measurements, metadata}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)
  end

  describe "idle expiry" do
    test "with no reads the worker exits {:shutdown, :idle}, emits telemetry, and requests foreman removal",
         %{tmp_dir: tmp_dir} do
      attach_spindown_telemetry(self())
      {worker_id, pid} = start_worker(tmp_dir, %{"idle_timeout" => @idle_timeout})
      ref = Process.monitor(pid)

      rewind_last_read(pid, @idle_timeout + 1)
      send(pid, :idle_check)

      assert_receive {:DOWN, ^ref, :process, ^pid, {:shutdown, :idle}}, 5_000

      assert_receive {:telemetry, [:bedrock, :materializer, :idle_spindown], measurements, _metadata}
      assert measurements.idle_duration_ms >= @idle_timeout

      # The worker asked its foreman (this test process) to remove its
      # entry, which is what reclaims the on-disk working directory.
      assert_receive {:"$gen_call", from, {:remove_worker, ^worker_id}}, 5_000
      GenServer.reply(from, :ok)
    end

    test "a real (short) idle_timeout expires on the periodic check without manual driving", %{tmp_dir: tmp_dir} do
      {_worker_id, pid} = start_worker(tmp_dir, %{"idle_timeout" => 50})
      ref = Process.monitor(pid)

      assert_receive {:DOWN, ^ref, :process, ^pid, {:shutdown, :idle}}, 5_000
    end
  end

  describe "activity tracking" do
    test "a read resets the idle timer", %{tmp_dir: tmp_dir} do
      {_worker_id, pid} = start_worker(tmp_dir, %{"idle_timeout" => @idle_timeout})
      ref = Process.monitor(pid)

      rewind_last_read(pid, @idle_timeout + 1)

      # A client read arrives before the periodic check fires.
      Materializer.get(pid, "some_key", Version.zero(), timeout: 1_000)

      send(pid, :idle_check)
      refute_receive {:DOWN, ^ref, :process, ^pid, _reason}, 200
      assert Process.alive?(pid)
    end

    test "transaction application does NOT count as activity", %{tmp_dir: tmp_dir} do
      {_worker_id, pid} = start_worker(tmp_dir, %{"idle_timeout" => @idle_timeout})
      ref = Process.monitor(pid)

      rewound_at = System.monotonic_time(:millisecond) - @idle_timeout - 1
      :sys.replace_state(pid, fn state -> %{state | mode: :running, last_read_at: rewound_at} end)

      # Apply a transaction and wait until it has been processed.
      send(pid, {:apply_transactions, [new_log_transaction(1, %{"key" => "value"})]})

      assert %{last_read_at: ^rewound_at} = :sys.get_state(pid)

      send(pid, :idle_check)
      assert_receive {:DOWN, ^ref, :process, ^pid, {:shutdown, :idle}}, 5_000
    end
  end

  describe "snapshot upload on idle exit" do
    test "a configured snapshot is uploaded before the worker stops", %{tmp_dir: tmp_dir} do
      object_storage_root = Path.join(tmp_dir, "object_storage")
      File.mkdir_p!(object_storage_root)
      backend = ObjectStorage.backend(LocalFilesystem, root: object_storage_root)
      snapshot = Snapshot.new(backend, "shard_idle_test")

      {_worker_id, pid} = start_worker(Path.join(tmp_dir, "worker"), %{"idle_timeout" => @idle_timeout})
      ref = Process.monitor(pid)

      :sys.replace_state(pid, fn state -> %{state | snapshot: snapshot} end)

      rewind_last_read(pid, @idle_timeout + 1)
      send(pid, :idle_check)
      assert_receive {:DOWN, ^ref, :process, ^pid, {:shutdown, :idle}}, 5_000

      assert {:ok, _version, _data} = Snapshot.read_latest(snapshot)
    end
  end

  describe "exemption" do
    test "a worker without an explicit idle_timeout never spins down", %{tmp_dir: tmp_dir} do
      {_worker_id, pid} = start_worker(tmp_dir, %{})
      ref = Process.monitor(pid)

      rewind_last_read(pid, @idle_timeout * 100)
      send(pid, :idle_check)

      refute_receive {:DOWN, ^ref, :process, ^pid, _reason}, 200
      assert Process.alive?(pid)
    end
  end
end
