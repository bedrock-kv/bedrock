defmodule Bedrock.Cluster.Gateway.DirectorRelationsTest do
  use ExUnit.Case, async: true

  alias Bedrock.Cluster.Gateway.DirectorRelations
  alias Bedrock.Cluster.Gateway.State

  describe "advertise_worker_with_leader_check/2" do
    test "ignores worker advertisement when no leader available" do
      state = %State{known_leader: :unavailable}
      worker_pid = self()

      result = DirectorRelations.advertise_worker_with_leader_check(state, worker_pid)

      assert result == state
    end

    # Note: Testing with actual leader requires complex mocking of GenServer calls
    # This would be better suited for integration tests with proper cluster setup
  end

  describe "pull_services_from_foreman_and_register/1" do
    test "does nothing when no leader available" do
      state = %State{known_leader: :unavailable}

      result = DirectorRelations.pull_services_from_foreman_and_register(state)

      assert result == state
    end

    # Note: Testing with actual leader requires mocking foreman calls
    # This would be better suited for integration tests with proper cluster setup
  end

  describe "worker_info_to_service_info/1" do
    test "converts worker info to service info format" do
      worker_info = [
        id: "service_1",
        kind: :storage,
        otp_name: :storage_1_worker,
        pid: self()
      ]

      result = DirectorRelations.worker_info_to_service_info(worker_info)

      assert result == {"service_1", :storage, {:storage_1_worker, Node.self()}}
    end

    test "handles log worker correctly" do
      worker_info = [
        id: "log_service",
        kind: :log,
        otp_name: :log_worker_1,
        pid: self()
      ]

      result = DirectorRelations.worker_info_to_service_info(worker_info)

      assert result == {"log_service", :log, {:log_worker_1, Node.self()}}
    end
  end
end
