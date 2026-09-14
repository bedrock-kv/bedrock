defmodule Bedrock.Docs.DurabilityFoundationExamplesTest do
  use ExUnit.Case, async: false

  alias Bedrock.Durability
  alias Bedrock.Internal.ClusterSupervisor
  alias Bedrock.ObjectStorage.Config
  alias Bedrock.ObjectStorage.S3

  test "strict and relaxed profile examples behave as documented" do
    node_config = [
      coordinator: [path: "/tmp/bedrock/coordinator", persistent: true],
      log: [path: "/tmp/bedrock/log"],
      materializer: [path: "/tmp/bedrock/materializer"],
      durability: [desired_replication_factor: 1, desired_logs: 1]
    ]

    assert {:error, {:durability_profile_failed, reasons}} = Durability.require(node_config, :strict)
    assert :desired_replication_factor_too_low in reasons
    assert :desired_logs_too_low in reasons

    assert :ok = Durability.require(node_config, :relaxed)
  end

  test "runtime durability mode defaults to strict with explicit relaxed override" do
    assert :strict == ClusterSupervisor.durability_mode([])
    assert :strict == ClusterSupervisor.durability_mode(durability: [mode: :unsupported])
    assert :relaxed == ClusterSupervisor.durability_mode(durability_mode: :relaxed)
    assert :relaxed == ClusterSupervisor.durability_mode(durability: [mode: :relaxed])
  end

  test "s3 shorthand node config example normalizes to S3 backend tuple" do
    node_config = [
      object_storage: :s3,
      s3: [
        bucket: "bedrock",
        access_key_id: "minio_key",
        secret_access_key: "minio_secret",
        scheme: "http://",
        region: "local",
        host: "127.0.0.1",
        port: 9000
      ]
    ]

    assert {S3, backend_config} = Config.cluster_backend(node_config)
    assert backend_config[:bucket] == "bedrock"
    assert backend_config[:config][:access_key_id] == "minio_key"
    assert backend_config[:config][:secret_access_key] == "minio_secret"
    assert backend_config[:config][:scheme] == "http://"
    assert backend_config[:config][:region] == "local"
    assert backend_config[:config][:host] == "127.0.0.1"
    assert backend_config[:config][:port] == 9000
  end
end
