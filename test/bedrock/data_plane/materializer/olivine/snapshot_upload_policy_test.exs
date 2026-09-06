defmodule Bedrock.DataPlane.Materializer.Olivine.SnapshotUploadPolicyTest do
  @moduledoc """
  Snapshot upload policy (bedrock-zi44) as the worker sees it: the
  manifest params a materializer is created with have to reach its
  `snapshot_policy`, one knob at a time, and a worker created without
  them has to end up with the policy that uploads at every opportunity —
  the behaviour every materializer had before the policy existed.

  What the policy DOES with those knobs is `SnapshotPolicyTest`; where
  it is consulted is `LogicTest`.
  """
  use ExUnit.Case, async: false

  alias Bedrock.DataPlane.Materializer.Olivine
  alias Bedrock.DataPlane.Materializer.Olivine.SnapshotPolicy

  defmodule TestCluster do
    @moduledoc false
    def name, do: "snapshot-policy-test-cluster"
  end

  setup do
    tmp_dir = Path.join(System.tmp_dir!(), "olivine_snap_policy_#{:erlang.unique_integer([:positive])}")
    File.rm_rf(tmp_dir)
    File.mkdir_p!(tmp_dir)
    on_exit(fn -> File.rm_rf(tmp_dir) end)

    {:ok, tmp_dir: tmp_dir}
  end

  defp start_worker(tmp_dir, params) do
    worker_id = "snap_wkr_#{System.unique_integer([:positive])}"

    child_spec =
      Olivine.child_spec(
        otp_name: :"olivine_snap_#{System.unique_integer([:positive])}",
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

    pid
  end

  describe "manifest params" do
    test "set the worker's policy", %{tmp_dir: tmp_dir} do
      pid =
        start_worker(tmp_dir, %{
          "shard_id" => 42,
          "snapshot_min_interval_ms" => 60_000,
          "snapshot_after_bytes" => 4_096,
          "snapshot_after_transactions" => 100
        })

      assert %SnapshotPolicy{min_interval_ms: 60_000, after_bytes: 4_096, after_transactions: 100} =
               :sys.get_state(pid).snapshot_policy
    end

    test "set only the knobs they name", %{tmp_dir: tmp_dir} do
      pid = start_worker(tmp_dir, %{"shard_id" => 42, "snapshot_after_transactions" => 10_000})

      assert %SnapshotPolicy{min_interval_ms: nil, after_bytes: nil, after_transactions: 10_000} =
               :sys.get_state(pid).snapshot_policy
    end

    test "left out leave the policy uploading at every opportunity", %{tmp_dir: tmp_dir} do
      pid = start_worker(tmp_dir, %{"shard_id" => 42})
      policy = :sys.get_state(pid).snapshot_policy

      assert %SnapshotPolicy{min_interval_ms: nil, after_bytes: nil, after_transactions: nil} = policy
      assert :upload = SnapshotPolicy.decide(policy, System.monotonic_time(:millisecond))
    end
  end
end
