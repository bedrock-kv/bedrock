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

  describe "decide/2 with a minimum interval" do
    setup do
      {:ok, policy: %SnapshotPolicy{min_interval_ms: 1_000}}
    end

    test "uploads when there is no previous upload to be too close to", %{policy: policy} do
      assert :upload = SnapshotPolicy.decide(policy, 0)
    end

    test "waits until the floor has been cleared", %{policy: policy} do
      policy = SnapshotPolicy.uploaded(policy, 10_000)

      assert :wait = SnapshotPolicy.decide(policy, 10_000)
      assert :wait = SnapshotPolicy.decide(policy, 10_999)
      assert :upload = SnapshotPolicy.decide(policy, 11_000)
      assert :upload = SnapshotPolicy.decide(policy, 50_000)
    end

    test "no amount of accumulated work clears the floor early", %{policy: policy} do
      policy = policy |> SnapshotPolicy.uploaded(0) |> SnapshotPolicy.observe(1_000_000, 1_000_000_000)

      assert :wait = SnapshotPolicy.decide(policy, 999)
    end
  end

  describe "decide/2 with thresholds" do
    test "a byte threshold is met at or above the configured size" do
      policy = SnapshotPolicy.observe(%SnapshotPolicy{after_bytes: 4_096}, 1, 4_095)

      assert :wait = SnapshotPolicy.decide(policy, 0)
      assert :upload = policy |> SnapshotPolicy.observe(1, 1) |> SnapshotPolicy.decide(0)
    end

    test "a transaction threshold is met at or above the configured count" do
      policy = SnapshotPolicy.observe(%SnapshotPolicy{after_transactions: 100}, 99, 1)

      assert :wait = SnapshotPolicy.decide(policy, 0)
      assert :upload = policy |> SnapshotPolicy.observe(1, 1) |> SnapshotPolicy.decide(0)
    end

    test "thresholds are a disjunction with each other: either one qualifies" do
      policy = %SnapshotPolicy{after_bytes: 4_096, after_transactions: 100}

      assert :wait = policy |> SnapshotPolicy.observe(99, 4_095) |> SnapshotPolicy.decide(0)
      assert :upload = policy |> SnapshotPolicy.observe(100, 0) |> SnapshotPolicy.decide(0)
      assert :upload = policy |> SnapshotPolicy.observe(0, 4_096) |> SnapshotPolicy.decide(0)
    end
  end

  describe "decide/2 with both a floor and a threshold" do
    setup do
      policy =
        SnapshotPolicy.uploaded(%SnapshotPolicy{min_interval_ms: 60_000, after_transactions: 100}, 0)

      {:ok, policy: policy}
    end

    test "the floor is a floor: a met threshold does not override it", %{policy: policy} do
      assert :wait = policy |> SnapshotPolicy.observe(1_000, 0) |> SnapshotPolicy.decide(59_999)
      assert :upload = policy |> SnapshotPolicy.observe(1_000, 0) |> SnapshotPolicy.decide(60_000)
    end

    test "clearing the floor is not enough on its own", %{policy: policy} do
      assert :wait = policy |> SnapshotPolicy.observe(99, 0) |> SnapshotPolicy.decide(600_000)
      assert :upload = policy |> SnapshotPolicy.observe(100, 0) |> SnapshotPolicy.decide(600_000)
    end
  end

  describe "uploaded/2" do
    test "restarts the floor and clears the accumulators" do
      policy =
        %SnapshotPolicy{min_interval_ms: 1_000, after_transactions: 10}
        |> SnapshotPolicy.uploaded(0)
        |> SnapshotPolicy.observe(50, 50)

      assert :upload = SnapshotPolicy.decide(policy, 1_000)

      policy = SnapshotPolicy.uploaded(policy, 1_000)

      # Both halves reset: inside the floor, and with nothing accumulated.
      assert :wait = SnapshotPolicy.decide(policy, 1_500)
      assert :wait = SnapshotPolicy.decide(policy, 2_000)
      assert :upload = policy |> SnapshotPolicy.observe(10, 0) |> SnapshotPolicy.decide(2_000)
    end
  end

  describe "from_params/1" do
    test "reads the manifest params a worker is created with" do
      assert %SnapshotPolicy{min_interval_ms: 60_000, after_bytes: 4_096, after_transactions: 100} =
               SnapshotPolicy.from_params(%{
                 "snapshot_min_interval_ms" => 60_000,
                 "snapshot_after_bytes" => 4_096,
                 "snapshot_after_transactions" => 100
               })
    end

    test "absent, non-integer or non-positive params leave the knob off" do
      assert %SnapshotPolicy{min_interval_ms: nil, after_bytes: nil, after_transactions: nil} =
               SnapshotPolicy.from_params(%{})

      assert %SnapshotPolicy{min_interval_ms: nil, after_bytes: nil, after_transactions: nil} =
               SnapshotPolicy.from_params(%{
                 "snapshot_min_interval_ms" => 0,
                 "snapshot_after_bytes" => -1,
                 "snapshot_after_transactions" => "100"
               })
    end
  end
end
