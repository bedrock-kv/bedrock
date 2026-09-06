defmodule Bedrock.DataPlane.Materializer.Olivine.SnapshotUploadPolicyTest do
  @moduledoc """
  Snapshot upload policy (bedrock-zi44) as the worker sees it: the
  manifest params a materializer is created with must reach its
  `snapshot_policy`, an armed policy must actually produce a snapshot on
  its own schedule, and a worker with no trigger configured must behave
  exactly as it did before — no timer, no scheduled compaction.

  The scheduled path is driven by hand (`send(pid, :snapshot_check)`)
  wherever the assertion is about the decision rather than the timer;
  one test lets a real short interval fire on its own.
  """
  use ExUnit.Case, async: false

  alias Bedrock.DataPlane.Materializer.Olivine
  alias Bedrock.DataPlane.Materializer.Olivine.SnapshotPolicy
  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.Config, as: ObjectStorageConfig
  alias Bedrock.ObjectStorage.Keys, as: ObjectStorageKeys
  alias Bedrock.ObjectStorage.LocalFilesystem
  alias Bedrock.ObjectStorage.Snapshot

  defmodule TestCluster do
    @moduledoc false
    def name, do: "snapshot-policy-test-cluster"
  end

  setup do
    test_id = :erlang.unique_integer([:positive])
    tmp_dir = Path.join(System.tmp_dir!(), "olivine_snap_policy_#{test_id}")
    File.rm_rf(tmp_dir)
    File.mkdir_p!(tmp_dir)

    root = Path.join(System.tmp_dir!(), "olivine_snap_policy_objects_#{test_id}")
    File.mkdir_p!(root)
    old_config = Application.get_env(:bedrock, ObjectStorage)
    Application.put_env(:bedrock, ObjectStorage, backend: ObjectStorage.backend(LocalFilesystem, root: root))

    on_exit(fn ->
      if old_config,
        do: Application.put_env(:bedrock, ObjectStorage, old_config),
        else: Application.delete_env(:bedrock, ObjectStorage)

      File.rm_rf(root)
      File.rm_rf(tmp_dir)
    end)

    {:ok, tmp_dir: tmp_dir, test_id: test_id}
  end

  defp start_worker(tmp_dir, params) do
    worker_id = "snap_wkr_#{System.unique_integer([:positive])}"
    otp_name = :"olivine_snap_#{System.unique_integer([:positive])}"

    child_spec =
      Olivine.child_spec(
        otp_name: otp_name,
        foreman: self(),
        id: worker_id,
        path: tmp_dir,
        cluster: TestCluster,
        params: params
      )

    {GenServer, :start_link, args} = child_spec.start
    {:ok, pid} = apply(GenServer, :start, args)
    on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid, :normal) end)

    receive do
      {:"$gen_cast", {:worker_health, ^worker_id, {:ok, ^pid}}} -> :ok
    after
      5_000 -> flunk("no health report")
    end

    # Workers boot :locked; a locked worker is mid-recovery and defers
    # its snapshot check, so these tests exercise the running state.
    :sys.replace_state(pid, &%{&1 | mode: :running})

    pid
  end

  defp snapshot_handle_for(shard_num) do
    Snapshot.new(ObjectStorageConfig.backend(), ObjectStorageKeys.shard_tag(shard_num))
  end

  defp await_snapshot(_snapshot, 0), do: {:error, :timed_out}

  defp await_snapshot(snapshot, attempts_left) do
    case Snapshot.latest_version(snapshot) do
      {:ok, version} ->
        {:ok, version}

      {:error, :not_found} ->
        Process.sleep(25)
        await_snapshot(snapshot, attempts_left - 1)
    end
  end

  describe "manifest params" do
    test "arm the worker's policy", %{tmp_dir: tmp_dir} do
      pid =
        start_worker(tmp_dir, %{
          "shard_id" => 42,
          "snapshot_interval_ms" => 60_000,
          "snapshot_after_bytes" => 4_096,
          "snapshot_after_transactions" => 100
        })

      assert %SnapshotPolicy{interval_ms: 60_000, after_bytes: 4_096, after_transactions: 100, last_upload_at: at} =
               :sys.get_state(pid).snapshot_policy

      # started/2 ran at startup, so the interval is measured from boot
      # rather than firing on the first check.
      assert is_integer(at)
    end

    test "left out leave it unarmed", %{tmp_dir: tmp_dir} do
      pid = start_worker(tmp_dir, %{"shard_id" => 42})

      assert %SnapshotPolicy{interval_ms: nil, after_bytes: nil, after_transactions: nil} =
               :sys.get_state(pid).snapshot_policy
    end
  end

  describe "the scheduled trigger" do
    test "an armed worker snapshots on its own schedule", %{tmp_dir: tmp_dir} do
      _pid = start_worker(tmp_dir, %{"shard_id" => 42, "snapshot_interval_ms" => 40})

      assert {:ok, _version} = await_snapshot(snapshot_handle_for(42), 200)
    end

    test "a check inside the interval starts nothing", %{tmp_dir: tmp_dir} do
      pid = start_worker(tmp_dir, %{"shard_id" => 42, "snapshot_interval_ms" => 600_000})

      send(pid, :snapshot_check)
      assert nil == :sys.get_state(pid).compaction_task

      # Rewinding the policy's clock past the interval releases it.
      :sys.replace_state(pid, fn t ->
        %{t | snapshot_policy: %{t.snapshot_policy | last_upload_at: t.snapshot_policy.last_upload_at - 600_000}}
      end)

      send(pid, :snapshot_check)
      assert {:ok, _version} = await_snapshot(snapshot_handle_for(42), 200)
    end

    test "an unarmed worker never schedules one", %{tmp_dir: tmp_dir} do
      pid = start_worker(tmp_dir, %{"shard_id" => 42})

      # No timer was armed, and a check delivered by hand still declines
      # to compact: the policy has no trigger to fire.
      send(pid, :snapshot_check)
      assert nil == :sys.get_state(pid).compaction_task

      Process.sleep(100)
      assert {:error, :not_found} = Snapshot.latest_version(snapshot_handle_for(42))
    end

    test "a locked worker defers", %{tmp_dir: tmp_dir} do
      pid = start_worker(tmp_dir, %{"shard_id" => 42, "snapshot_interval_ms" => 40})
      :sys.replace_state(pid, &%{&1 | mode: :locked})

      Process.sleep(100)
      assert nil == :sys.get_state(pid).compaction_task
      assert {:error, :not_found} = Snapshot.latest_version(snapshot_handle_for(42))
    end
  end
end
