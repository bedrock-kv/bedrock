defmodule Bedrock.Durability.ProfileTest do
  use ExUnit.Case, async: true

  alias Bedrock.Durability
  alias Bedrock.Durability.Profile

  defmodule DurableCluster do
    @moduledoc false
    def node_config do
      [
        coordinator: [path: "/var/lib/bedrock/coordinator"],
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

  describe "profile/1" do
    test "returns an ok profile when durability requirements are met" do
      profile = Durability.profile(DurableCluster)

      assert %Profile{} = profile
      assert profile.status == :ok
      assert profile.reasons == []
      assert profile.checks.desired_replication_factor.status == :ok
      assert profile.checks.desired_logs.status == :ok
      assert profile.checks.coordinator_path.status == :ok
      assert profile.checks.log_path.status == :ok
      assert profile.checks.materializer_path.status == :ok
    end

    test "returns failed checks with explicit reasons when requirements are not met" do
      profile = Durability.profile(UndersizedCluster)

      assert profile.status == :failed

      assert Enum.sort(profile.reasons) ==
               Enum.sort([
                 :desired_replication_factor_too_low,
                 :desired_logs_too_low,
                 :missing_coordinator_path,
                 :missing_log_path,
                 :missing_materializer_path
               ])
    end

    test "falls back to worker path for log/materializer checks" do
      profile =
        Durability.profile(%{
          node_config: [
            coordinator: [path: "/var/lib/bedrock/coordinator"],
            worker: [path: "/var/lib/bedrock/worker"]
          ],
          cluster_config: %{parameters: %{desired_replication_factor: 3, desired_logs: 3}}
        })

      assert profile.status == :ok
      assert profile.checks.log_path.status == :ok
      assert profile.checks.materializer_path.status == :ok
    end

    test "supports evaluating a raw node config keyword list" do
      profile =
        Durability.profile(
          coordinator: [path: "/var/lib/bedrock/coordinator"],
          worker: [path: "/var/lib/bedrock/worker"]
        )

      assert profile.status == :failed
      assert :desired_replication_factor_too_low in profile.reasons
      assert :desired_logs_too_low in profile.reasons
    end
  end

  describe "require/2" do
    test "returns strict-mode error for failed profile" do
      assert {:error, {:durability_profile_failed, reasons}} =
               Durability.require(UndersizedCluster, :strict)

      assert :missing_coordinator_path in reasons
      assert :desired_logs_too_low in reasons
    end

    test "returns ok in relaxed mode for failed profile" do
      assert :ok = Durability.require(UndersizedCluster, :relaxed)
    end

    test "returns ok in strict mode for passing profile" do
      assert :ok = Durability.require(DurableCluster, :strict)
    end
  end
end
