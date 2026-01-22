defmodule Bedrock.ControlPlane.Coordinator.RecoveryCapabilityTrackerTest do
  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.Coordinator.RecoveryCapabilityTracker

  describe "new/0" do
    test "creates a new tracker with nil hash" do
      tracker = RecoveryCapabilityTracker.new()

      assert tracker.hash == nil
    end
  end

  describe "update_hash/2" do
    test "updates the hash field" do
      tracker = RecoveryCapabilityTracker.new()

      updated = RecoveryCapabilityTracker.update_hash(tracker, "some_hash")

      assert updated.hash == "some_hash"
    end
  end

  describe "get_hash/1" do
    test "returns the current hash" do
      tracker = %RecoveryCapabilityTracker{hash: "test_hash"}

      assert RecoveryCapabilityTracker.get_hash(tracker) == "test_hash"
    end

    test "returns nil when hash is not set" do
      tracker = RecoveryCapabilityTracker.new()

      assert RecoveryCapabilityTracker.get_hash(tracker) == nil
    end
  end

  describe "check_for_recovery_state_changes/3" do
    test "returns unchanged when hash matches" do
      node_capabilities = %{node() => [:log, :materializer]}
      service_directory = %{}

      tracker = RecoveryCapabilityTracker.new()
      tracker = RecoveryCapabilityTracker.update_recovery_state_hash(tracker, node_capabilities, service_directory)

      {status, _updated_tracker} =
        RecoveryCapabilityTracker.check_for_recovery_state_changes(
          tracker,
          node_capabilities,
          service_directory
        )

      assert status == :unchanged
    end

    test "returns changed when capabilities differ" do
      node_capabilities1 = %{node() => [:log]}
      node_capabilities2 = %{node() => [:log, :materializer]}
      service_directory = %{}

      tracker = RecoveryCapabilityTracker.new()
      tracker = RecoveryCapabilityTracker.update_recovery_state_hash(tracker, node_capabilities1, service_directory)

      {status, updated_tracker} =
        RecoveryCapabilityTracker.check_for_recovery_state_changes(
          tracker,
          node_capabilities2,
          service_directory
        )

      assert status == :changed
      refute updated_tracker.hash == tracker.hash
    end

    test "returns changed when hash is nil" do
      node_capabilities = %{node() => [:log]}
      service_directory = %{}

      tracker = RecoveryCapabilityTracker.new()

      {status, _updated_tracker} =
        RecoveryCapabilityTracker.check_for_recovery_state_changes(
          tracker,
          node_capabilities,
          service_directory
        )

      assert status == :changed
    end
  end

  describe "update_recovery_state_hash/3" do
    test "updates hash based on capabilities and service directory" do
      node_capabilities = %{node() => [:log, :materializer]}

      service_directory = %{
        "service1" => {:log, {:log_worker, node()}}
      }

      tracker = RecoveryCapabilityTracker.new()

      updated =
        RecoveryCapabilityTracker.update_recovery_state_hash(
          tracker,
          node_capabilities,
          service_directory
        )

      assert updated.hash
      assert is_binary(updated.hash)
    end
  end

  describe "check_for_capability_changes/2 (deprecated)" do
    test "delegates to check_for_recovery_state_changes with empty service directory" do
      node_capabilities = %{node() => [:log]}

      tracker = RecoveryCapabilityTracker.new()

      {status, _updated} =
        RecoveryCapabilityTracker.check_for_capability_changes(tracker, node_capabilities)

      assert status == :changed
    end
  end

  describe "update_capability_hash/2 (deprecated)" do
    test "delegates to update_recovery_state_hash with empty service directory" do
      node_capabilities = %{node() => [:log, :materializer]}

      tracker = RecoveryCapabilityTracker.new()

      updated = RecoveryCapabilityTracker.update_capability_hash(tracker, node_capabilities)

      assert updated.hash
    end
  end
end
