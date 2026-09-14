defmodule Bedrock.ObjectStorage.ConfigTest do
  use ExUnit.Case, async: false

  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.Config
  alias Bedrock.ObjectStorage.LocalFilesystem
  alias Bedrock.ObjectStorage.S3

  setup do
    original = Application.get_env(:bedrock, ObjectStorage)

    on_exit(fn ->
      if original do
        Application.put_env(:bedrock, ObjectStorage, original)
      else
        Application.delete_env(:bedrock, ObjectStorage)
      end
    end)

    :ok
  end

  describe "cluster_backend/1" do
    test "normalizes a module-only object_storage" do
      assert {LocalFilesystem, []} = Config.cluster_backend(object_storage: LocalFilesystem)
    end

    test "normalizes S3 tuple config and merges request options" do
      node_config = [
        object_storage:
          {S3,
           [
             bucket: "bedrock-override",
             config: [region: "us-east-1"],
             host: "localhost",
             port: 9000,
             scheme: "http://"
           ]},
        s3: [bucket: "bedrock-default", access_key_id: "default_key"]
      ]

      {module, backend_config} = Config.cluster_backend(node_config)

      assert module == S3
      assert backend_config[:bucket] == "bedrock-override"
      assert backend_config[:config][:region] == "us-east-1"
      assert backend_config[:config][:access_key_id] == "default_key"
      assert backend_config[:config][:host] == "localhost"
      assert backend_config[:config][:port] == 9000
      assert backend_config[:config][:scheme] == "http://"
    end

    test "normalizes :local_filesystem shorthand with top-level config" do
      node_config = [object_storage: :local_filesystem, local_filesystem: [root: "/tmp/bedrock-local"]]

      assert {LocalFilesystem, [root: "/tmp/bedrock-local"]} = Config.cluster_backend(node_config)
    end
  end

  describe "config/0" do
    test "returns configured keyword list" do
      Application.put_env(:bedrock, ObjectStorage,
        backend: LocalFilesystem,
        bootstrap_key: "test/key"
      )

      config = Config.config()

      assert config[:backend] == LocalFilesystem
      assert config[:bootstrap_key] == "test/key"
    end

    test "returns empty list when not configured" do
      Application.delete_env(:bedrock, ObjectStorage)

      assert [] = Config.config()
    end
  end
end
