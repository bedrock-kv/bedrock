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

  describe "backend/0" do
    test "returns configured backend with tuple format" do
      Application.put_env(:bedrock, ObjectStorage, backend: {LocalFilesystem, root: "/custom/path"})

      assert {LocalFilesystem, [root: "/custom/path"]} = Config.backend()
    end

    test "normalizes module-only config" do
      Application.put_env(:bedrock, ObjectStorage, backend: LocalFilesystem)

      assert {LocalFilesystem, []} = Config.backend()
    end

    test "returns default when not configured" do
      Application.delete_env(:bedrock, ObjectStorage)

      {module, config} = Config.backend()

      assert module == LocalFilesystem
      assert Keyword.has_key?(config, :root)
      assert config[:root] =~ "bedrock_objects"
    end

    test "normalizes :s3 shorthand using top-level :s3 config" do
      Application.put_env(:bedrock, ObjectStorage,
        backend: :s3,
        s3: [
          bucket: "bedrock",
          access_key_id: "minio_key",
          secret_access_key: "minio_secret",
          region: "local",
          host: "127.0.0.1",
          port: 9000,
          scheme: "http://"
        ]
      )

      {module, backend_config} = Config.backend()

      assert module == S3
      assert backend_config[:bucket] == "bedrock"
      assert backend_config[:config][:access_key_id] == "minio_key"
      assert backend_config[:config][:secret_access_key] == "minio_secret"
      assert backend_config[:config][:host] == "127.0.0.1"
      assert backend_config[:config][:port] == 9000
      assert backend_config[:config][:region] == "local"
      assert backend_config[:config][:scheme] == "http://"
    end

    test "normalizes S3 tuple config and merges request options" do
      Application.put_env(:bedrock, ObjectStorage,
        backend:
          {S3,
           [
             bucket: "bedrock-override",
             config: [region: "us-east-1"],
             host: "localhost",
             port: 9000,
             scheme: "http://"
           ]},
        s3: [bucket: "bedrock-default", access_key_id: "default_key"]
      )

      {module, backend_config} = Config.backend()

      assert module == S3
      assert backend_config[:bucket] == "bedrock-override"
      assert backend_config[:config][:region] == "us-east-1"
      assert backend_config[:config][:access_key_id] == "default_key"
      assert backend_config[:config][:host] == "localhost"
      assert backend_config[:config][:port] == 9000
      assert backend_config[:config][:scheme] == "http://"
    end

    test "normalizes :local_filesystem shorthand with top-level config" do
      Application.put_env(:bedrock, ObjectStorage,
        backend: :local_filesystem,
        local_filesystem: [root: "/tmp/bedrock-local"]
      )

      assert {LocalFilesystem, [root: "/tmp/bedrock-local"]} = Config.backend()
    end
  end

  describe "bootstrap_key/0" do
    test "returns configured key" do
      Application.put_env(:bedrock, ObjectStorage, bootstrap_key: "my-cluster/bootstrap")

      assert "my-cluster/bootstrap" = Config.bootstrap_key()
    end

    test "returns nil when not configured" do
      Application.delete_env(:bedrock, ObjectStorage)

      assert nil == Config.bootstrap_key()
    end
  end

  describe "bootstrap_key!/0" do
    test "returns configured key" do
      Application.put_env(:bedrock, ObjectStorage, bootstrap_key: "prod/bootstrap")

      assert "prod/bootstrap" = Config.bootstrap_key!()
    end

    test "raises when not configured" do
      Application.delete_env(:bedrock, ObjectStorage)

      assert_raise RuntimeError, ~r/bootstrap_key not configured/, fn ->
        Config.bootstrap_key!()
      end
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
