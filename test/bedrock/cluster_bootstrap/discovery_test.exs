defmodule Bedrock.ClusterBootstrap.DiscoveryTest do
  use ExUnit.Case, async: true

  alias Bedrock.ClusterBootstrap.Discovery
  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.LocalFilesystem
  alias Bedrock.SystemKeys.ClusterBootstrap

  setup do
    root = Path.join(System.tmp_dir!(), "discovery_test_#{:erlang.unique_integer([:positive])}")
    File.mkdir_p!(root)
    on_exit(fn -> File.rm_rf!(root) end)

    backend = ObjectStorage.backend(LocalFilesystem, root: root)
    bootstrap_key = "test_cluster/bootstrap"

    {:ok, backend: backend, bootstrap_key: bootstrap_key, root: root}
  end

  describe "discover/3 when ClusterBootstrap exists" do
    test "returns {:ok, :coordinator, bootstrap, version_token} when node is in coordinators list",
         ctx do
      self_node = :test@localhost
      bootstrap = create_bootstrap_with_coordinator(self_node)
      write_bootstrap(ctx.backend, ctx.bootstrap_key, bootstrap)

      assert {:ok, :coordinator, result, _version_token} =
               Discovery.discover(ctx.backend, ctx.bootstrap_key, self_node)

      assert result.cluster_id == bootstrap.cluster_id
      assert result.epoch == bootstrap.epoch
    end

    test "returns {:ok, :worker, bootstrap, version_token} when node is NOT in coordinators list",
         ctx do
      self_node = :test@localhost
      other_node = :other@localhost
      bootstrap = create_bootstrap_with_coordinator(other_node)
      write_bootstrap(ctx.backend, ctx.bootstrap_key, bootstrap)

      assert {:ok, :worker, result, _version_token} =
               Discovery.discover(ctx.backend, ctx.bootstrap_key, self_node)

      assert result.cluster_id == bootstrap.cluster_id
    end
  end

  describe "discover/3 when ClusterBootstrap does not exist (first boot)" do
    test "creates initial bootstrap and returns {:ok, :coordinator, bootstrap, nil}", ctx do
      self_node = :test@localhost

      assert {:ok, :coordinator, bootstrap, nil} =
               Discovery.discover(ctx.backend, ctx.bootstrap_key, self_node)

      # Verify bootstrap was created
      assert is_binary(bootstrap.cluster_id)
      assert bootstrap.epoch == 1
      assert bootstrap.logs == []
      assert coordinator_nodes(bootstrap) == [self_node]

      # Verify it was written to object storage
      {:ok, data} = ObjectStorage.get(ctx.backend, ctx.bootstrap_key)
      {:ok, stored} = ClusterBootstrap.read(data)
      assert stored.cluster_id == bootstrap.cluster_id
    end

    test "loses race and returns {:ok, :coordinator, bootstrap, version_token} if in winning bootstrap",
         ctx do
      self_node = :test@localhost

      # Simulate race: another node writes first with us as coordinator
      bootstrap = create_bootstrap_with_coordinator(self_node)
      write_bootstrap(ctx.backend, ctx.bootstrap_key, bootstrap)

      # Our discover should re-read and find we're a coordinator
      assert {:ok, :coordinator, result, _version_token} =
               Discovery.discover(ctx.backend, ctx.bootstrap_key, self_node)

      assert result.cluster_id == bootstrap.cluster_id
    end

    test "loses race and returns {:ok, :worker, bootstrap, version_token} if not in winning bootstrap",
         ctx do
      self_node = :test@localhost
      winner_node = :winner@localhost

      # Simulate race: another node writes first without us
      bootstrap = create_bootstrap_with_coordinator(winner_node)
      write_bootstrap(ctx.backend, ctx.bootstrap_key, bootstrap)

      # Our discover should re-read and find we're not a coordinator
      assert {:ok, :worker, result, _version_token} =
               Discovery.discover(ctx.backend, ctx.bootstrap_key, self_node)

      assert result.cluster_id == bootstrap.cluster_id
    end
  end

  describe "create_initial/1" do
    test "creates bootstrap with generated cluster_id" do
      node = :test@localhost
      bootstrap = Discovery.create_initial(node)

      assert is_binary(bootstrap.cluster_id)
      assert String.length(bootstrap.cluster_id) == 8
    end

    test "creates bootstrap with epoch 1" do
      bootstrap = Discovery.create_initial(:test@localhost)
      assert bootstrap.epoch == 1
    end

    test "creates bootstrap with empty logs" do
      bootstrap = Discovery.create_initial(:test@localhost)
      assert bootstrap.logs == []
    end

    test "creates bootstrap with self as sole coordinator" do
      node = :test@localhost
      bootstrap = Discovery.create_initial(node)
      assert coordinator_nodes(bootstrap) == [node]
    end
  end

  describe "discover/3 returns version token" do
    test "returns version token when bootstrap exists", ctx do
      self_node = :test@localhost
      bootstrap = create_bootstrap_with_coordinator(self_node)
      write_bootstrap(ctx.backend, ctx.bootstrap_key, bootstrap)

      assert {:ok, :coordinator, _bootstrap, version_token} =
               Discovery.discover(ctx.backend, ctx.bootstrap_key, self_node)

      assert is_binary(version_token)
      assert String.starts_with?(version_token, "sha256:")
    end

    test "returns nil version token on first boot", ctx do
      self_node = :test@localhost

      assert {:ok, :coordinator, _bootstrap, nil} =
               Discovery.discover(ctx.backend, ctx.bootstrap_key, self_node)
    end
  end

  describe "write_bootstrap/4" do
    test "succeeds with valid version token", ctx do
      self_node = :test@localhost
      bootstrap = create_bootstrap_with_coordinator(self_node)
      write_bootstrap(ctx.backend, ctx.bootstrap_key, bootstrap)

      {:ok, :coordinator, _bootstrap, version_token} =
        Discovery.discover(ctx.backend, ctx.bootstrap_key, self_node)

      updated_bootstrap = %{bootstrap | epoch: 2}

      assert :ok =
               Discovery.write_bootstrap(
                 ctx.backend,
                 ctx.bootstrap_key,
                 version_token,
                 updated_bootstrap
               )

      # Verify update was written
      {:ok, data} = ObjectStorage.get(ctx.backend, ctx.bootstrap_key)
      {:ok, stored} = ClusterBootstrap.read(data)
      assert stored.epoch == 2
    end

    test "fails with stale version token", ctx do
      self_node = :test@localhost
      bootstrap = create_bootstrap_with_coordinator(self_node)
      write_bootstrap(ctx.backend, ctx.bootstrap_key, bootstrap)

      {:ok, :coordinator, _bootstrap, stale_token} =
        Discovery.discover(ctx.backend, ctx.bootstrap_key, self_node)

      # Another recovery completes first
      concurrent_bootstrap = %{bootstrap | epoch: 99}
      write_bootstrap(ctx.backend, ctx.bootstrap_key, concurrent_bootstrap)

      # Our write with stale token should fail
      our_bootstrap = %{bootstrap | epoch: 2}

      assert {:error, :version_mismatch} =
               Discovery.write_bootstrap(
                 ctx.backend,
                 ctx.bootstrap_key,
                 stale_token,
                 our_bootstrap
               )

      # Content should remain from concurrent recovery
      {:ok, data} = ObjectStorage.get(ctx.backend, ctx.bootstrap_key)
      {:ok, stored} = ClusterBootstrap.read(data)
      assert stored.epoch == 99
    end
  end

  # Helper functions

  defp create_bootstrap_with_coordinator(node) do
    %{
      cluster_id: "test1234",
      epoch: 1,
      logs: [],
      coordinators: [%{node: Atom.to_string(node)}]
    }
  end

  defp write_bootstrap(backend, key, bootstrap) do
    data = ClusterBootstrap.to_binary(bootstrap)
    :ok = ObjectStorage.put(backend, key, data)
  end

  defp coordinator_nodes(bootstrap) do
    bootstrap.coordinators
    |> Enum.map(& &1.node)
    |> Enum.map(&String.to_atom/1)
  end
end
