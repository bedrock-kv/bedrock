defmodule Bedrock.Durability.TelemetryTest do
  use ExUnit.Case, async: false

  import Bedrock.Test.TelemetryTestHelper

  alias Bedrock.Durability

  defmodule DurableCluster do
    @moduledoc false
    def node_config do
      [
        coordinator: [path: "/var/lib/bedrock/coordinator", persistent: true],
        log: [path: "/var/lib/bedrock/log"],
        materializer: [path: "/var/lib/bedrock/storage"]
      ]
    end

    def fetch_config do
      {:ok, %{parameters: %{desired_replication_factor: 3, desired_logs: 3}}}
    end
  end

  defmodule UndersizedCluster do
    @moduledoc false
    def node_config do
      [
        coordinator: [],
        log: [],
        materializer: []
      ]
    end

    def fetch_config do
      {:ok, %{parameters: %{desired_replication_factor: 1, desired_logs: 1}}}
    end
  end

  describe "profile telemetry" do
    test "emits :ok event when profile passes" do
      attach_telemetry_reflector(
        self(),
        [[:bedrock, :durability, :profile, :ok]],
        "durability-ok-#{System.unique_integer([:positive])}"
      )

      profile = Durability.profile(DurableCluster)
      assert profile.status == :ok

      {measurements, metadata} = expect_telemetry([:bedrock, :durability, :profile, :ok])

      assert measurements.failed_checks == 0
      assert metadata.status == :ok
      assert metadata.reasons == []
      assert metadata.target_type == :cluster_module
      assert metadata.target_module == DurableCluster
    end

    test "emits :failed event when profile fails" do
      attach_telemetry_reflector(
        self(),
        [[:bedrock, :durability, :profile, :failed]],
        "durability-failed-#{System.unique_integer([:positive])}"
      )

      profile = Durability.profile(UndersizedCluster)
      assert profile.status == :failed

      {measurements, metadata} = expect_telemetry([:bedrock, :durability, :profile, :failed])

      assert measurements.failed_checks == 6
      assert metadata.status == :failed
      assert :desired_logs_too_low in metadata.reasons
      assert :coordinator_persistence_disabled in metadata.reasons
      assert :missing_materializer_path in metadata.reasons
      assert metadata.target_type == :cluster_module
      assert metadata.target_module == UndersizedCluster
    end
  end
end
