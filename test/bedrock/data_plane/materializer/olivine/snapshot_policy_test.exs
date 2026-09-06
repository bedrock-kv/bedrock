defmodule Bedrock.DataPlane.Materializer.Olivine.SnapshotPolicyTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Materializer.Olivine.SnapshotPolicy

  describe "decide/2 with nothing configured" do
    test "uploads at every opportunity" do
      policy = %SnapshotPolicy{}

      assert :upload = SnapshotPolicy.decide(policy, 0)
      assert :upload = policy |> SnapshotPolicy.uploaded(0) |> SnapshotPolicy.decide(0)

      assert :upload =
               policy
               |> SnapshotPolicy.uploaded(0)
               |> SnapshotPolicy.observe(0, 0)
               |> SnapshotPolicy.decide(1)
    end
  end

  describe "decide/2 with a scheduled interval" do
    setup do
      {:ok, policy: %SnapshotPolicy{interval_ms: 1_000}}
    end

    test "uploads when no snapshot has been taken yet", %{policy: policy} do
      assert :upload = SnapshotPolicy.decide(policy, 0)
    end

    test "waits until the interval has elapsed", %{policy: policy} do
      policy = SnapshotPolicy.uploaded(policy, 10_000)

      assert :wait = SnapshotPolicy.decide(policy, 10_000)
      assert :wait = SnapshotPolicy.decide(policy, 10_999)
      assert :upload = SnapshotPolicy.decide(policy, 11_000)
      assert :upload = SnapshotPolicy.decide(policy, 50_000)
    end

    test "the accumulated work is irrelevant to a pure interval policy", %{policy: policy} do
      policy = policy |> SnapshotPolicy.uploaded(0) |> SnapshotPolicy.observe(1_000_000, 1_000_000_000)

      assert :wait = SnapshotPolicy.decide(policy, 999)
    end
  end

  describe "decide/2 with thresholds" do
    test "a byte threshold fires at or above the configured size" do
      policy = %SnapshotPolicy{after_bytes: 4_096} |> SnapshotPolicy.uploaded(0) |> SnapshotPolicy.observe(1, 4_095)

      assert :wait = SnapshotPolicy.decide(policy, 0)
      assert :upload = policy |> SnapshotPolicy.observe(1, 1) |> SnapshotPolicy.decide(0)
    end

    test "a transaction threshold fires at or above the configured count" do
      policy =
        %SnapshotPolicy{after_transactions: 100} |> SnapshotPolicy.uploaded(0) |> SnapshotPolicy.observe(99, 1)

      assert :wait = SnapshotPolicy.decide(policy, 0)
      assert :upload = policy |> SnapshotPolicy.observe(1, 1) |> SnapshotPolicy.decide(0)
    end

    test "triggers are a disjunction: whichever fires first wins" do
      policy =
        SnapshotPolicy.uploaded(%SnapshotPolicy{interval_ms: 60_000, after_bytes: 4_096, after_transactions: 100}, 0)

      assert :wait = policy |> SnapshotPolicy.observe(99, 4_095) |> SnapshotPolicy.decide(59_999)
      assert :upload = policy |> SnapshotPolicy.observe(99, 4_095) |> SnapshotPolicy.decide(60_000)
      assert :upload = policy |> SnapshotPolicy.observe(100, 0) |> SnapshotPolicy.decide(0)
      assert :upload = policy |> SnapshotPolicy.observe(0, 4_096) |> SnapshotPolicy.decide(0)
    end
  end

  describe "uploaded/2" do
    test "restarts the clock and clears the accumulators" do
      policy =
        %SnapshotPolicy{interval_ms: 1_000, after_bytes: 10, after_transactions: 10}
        |> SnapshotPolicy.uploaded(0)
        |> SnapshotPolicy.observe(50, 50)

      assert :upload = SnapshotPolicy.decide(policy, 0)

      policy = SnapshotPolicy.uploaded(policy, 500)

      assert :wait = SnapshotPolicy.decide(policy, 500)
      assert :upload = SnapshotPolicy.decide(policy, 1_500)
    end
  end

  describe "check_interval_ms/1" do
    test "an unarmed policy never schedules a check" do
      assert :never = SnapshotPolicy.check_interval_ms(%SnapshotPolicy{})
    end

    test "an interval is checked four times per interval, with a floor" do
      assert 15_000 = SnapshotPolicy.check_interval_ms(%SnapshotPolicy{interval_ms: 60_000})
      assert 10 = SnapshotPolicy.check_interval_ms(%SnapshotPolicy{interval_ms: 1})
    end

    test "thresholds alone still need polling" do
      assert 1_000 = SnapshotPolicy.check_interval_ms(%SnapshotPolicy{after_bytes: 4_096})
      assert 1_000 = SnapshotPolicy.check_interval_ms(%SnapshotPolicy{after_transactions: 100})
    end
  end

  describe "started/2" do
    test "an interval measured from startup does not fire immediately" do
      policy = SnapshotPolicy.started(%SnapshotPolicy{interval_ms: 1_000}, 5_000)

      assert :wait = SnapshotPolicy.decide(policy, 5_999)
      assert :upload = SnapshotPolicy.decide(policy, 6_000)
    end
  end

  describe "from_params/1" do
    test "reads the manifest params a worker is created with" do
      assert %SnapshotPolicy{interval_ms: 60_000, after_bytes: 4_096, after_transactions: 100} =
               SnapshotPolicy.from_params(%{
                 "snapshot_interval_ms" => 60_000,
                 "snapshot_after_bytes" => 4_096,
                 "snapshot_after_transactions" => 100
               })
    end

    test "absent, non-integer or non-positive params leave the trigger disabled" do
      assert %SnapshotPolicy{interval_ms: nil, after_bytes: nil, after_transactions: nil} =
               SnapshotPolicy.from_params(%{})

      assert %SnapshotPolicy{interval_ms: nil, after_bytes: nil, after_transactions: nil} =
               SnapshotPolicy.from_params(%{
                 "snapshot_interval_ms" => 0,
                 "snapshot_after_bytes" => -1,
                 "snapshot_after_transactions" => "100"
               })
    end
  end
end
