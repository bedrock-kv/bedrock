defmodule Bedrock.DirectoryPartitionTest do
  @moduledoc """
  Behavioral tests for partition-scoped directory operations, create_or_open,
  removal semantics, accessors, keyspace/key-range coercion, and node value
  encoding variants.

  Uses an in-memory key/value store behind MockRepo stubs so operations
  exercise the real directory layer logic end-to-end instead of scripted
  expectations.
  """
  use ExUnit.Case, async: true

  import Mox

  alias Bedrock.Directory
  alias Bedrock.KeyRange
  alias Bedrock.Keyspace

  setup :verify_on_exit!

  setup do
    {:ok, store} = Agent.start_link(fn -> %{} end)
    stub_repo_with_store(store)

    {:ok, counter} = Agent.start_link(fn -> 1 end)

    next_prefix_fn = fn ->
      Agent.get_and_update(counter, fn n -> {<<0x15, n>>, n + 1} end)
    end

    root = Directory.root(MockRepo, next_prefix_fn: next_prefix_fn)

    {:ok, root: root, store: store}
  end

  # In-memory repo backing. Mirrors how Bedrock.Repo packs keyspace keys and
  # encodes/decodes values so directory operations behave as they would
  # against a real transaction.

  defp stub_repo_with_store(store) do
    stub(MockRepo, :transact, fn callback -> callback.() end)

    stub(MockRepo, :get, fn key when is_binary(key) -> raw_get(store, key) end)

    stub(MockRepo, :get, fn %Keyspace{} = keyspace, key ->
      case raw_get(store, Keyspace.pack(keyspace, key)) do
        nil -> nil
        value when is_nil(keyspace.value_encoding) -> value
        value -> keyspace.value_encoding.unpack(value)
      end
    end)

    stub(MockRepo, :put, fn %Keyspace{} = keyspace, key, value ->
      store_put(store, keyspace, key, value)
    end)

    stub(MockRepo, :get_range, fn range -> range_get(store, to_range(range), []) end)

    stub(MockRepo, :get_range, fn range, opts when is_list(opts) ->
      range_get(store, to_range(range), opts)
    end)

    stub(MockRepo, :clear_range, fn range -> range_clear(store, to_range(range)) end)
  end

  defp store_put(store, keyspace, key, value) do
    packed_key = Keyspace.pack(keyspace, key)
    packed_value = if keyspace.value_encoding, do: keyspace.value_encoding.pack(value), else: value
    Agent.update(store, &Map.put(&1, packed_key, packed_value))
    :ok
  end

  defp raw_get(store, key), do: Agent.get(store, &Map.get(&1, key))

  defp to_range(range), do: Bedrock.ToKeyRange.to_key_range(range)

  defp range_get(store, {start_key, end_key}, opts) do
    results =
      store
      |> Agent.get(& &1)
      |> Enum.filter(fn {key, _} -> key >= start_key and key < end_key end)
      |> Enum.sort()

    case Keyword.get(opts, :limit) do
      nil -> results
      limit -> Enum.take(results, limit)
    end
  end

  defp range_clear(store, {start_key, end_key}) do
    Agent.update(store, fn map ->
      map |> Enum.reject(fn {key, _} -> key >= start_key and key < end_key end) |> Map.new()
    end)

    :ok
  end

  defp seed_stored_version(store, root, version) do
    layer = root.directory_layer

    store_put(
      store,
      layer.node_keyspace,
      ["version"],
      <<elem(version, 0)::little-32, elem(version, 1)::little-32, elem(version, 2)::little-32>>
    )
  end

  defp open_partition(root, path) do
    {:ok, _} = Directory.create(root, path, layer: "partition")
    {:ok, %Directory.Partition{} = partition} = Directory.open(root, path)
    partition
  end

  describe "partition-scoped operations" do
    test "create through a partition creates a directory visible to open and exists?", %{root: root} do
      partition = open_partition(root, ["app"])

      assert {:ok, %Directory.Node{path: ["app", "users"]}} = Directory.create(partition, ["app", "users"])
      assert Directory.exists?(partition, ["app", "users"])
      assert {:ok, %Directory.Node{path: ["app", "users"]}} = Directory.open(partition, ["app", "users"])
      refute Directory.exists?(partition, ["app", "other"])
    end

    test "create_or_open through a partition opens an existing directory and creates a missing one", %{root: root} do
      partition = open_partition(root, ["app"])

      assert {:ok, %Directory.Node{prefix: prefix}} = Directory.create_or_open(partition, ["app", "logs"])
      assert {:ok, %Directory.Node{prefix: ^prefix}} = Directory.create_or_open(partition, ["app", "logs"])
    end

    test "list through a partition delegates to the partition's directory layer", %{root: root} do
      partition = open_partition(root, ["app"])

      {:ok, _} = Directory.create(partition, ["users"])
      {:ok, _} = Directory.create(partition, ["logs"])

      assert {:ok, children} = Directory.list(partition, [])
      assert "users" in children
      assert "logs" in children
    end

    test "move through a partition relocates the directory", %{root: root} do
      partition = open_partition(root, ["app"])
      {:ok, _} = Directory.create(partition, ["app", "users"])

      assert :ok = Directory.move(partition, ["app", "users"], ["app", "members"])
      refute Directory.exists?(partition, ["app", "users"])
      assert Directory.exists?(partition, ["app", "members"])
    end

    test "remove through a partition deletes the directory", %{root: root} do
      partition = open_partition(root, ["app"])
      {:ok, _} = Directory.create(partition, ["app", "users"])

      assert :ok = Directory.remove(partition, ["app", "users"])
      refute Directory.exists?(partition, ["app", "users"])
      assert {:error, :directory_does_not_exist} = Directory.remove(partition, ["app", "users"])
    end

    test "remove_if_exists through a partition succeeds whether or not the directory exists", %{root: root} do
      partition = open_partition(root, ["app"])
      {:ok, _} = Directory.create(partition, ["app", "users"])

      assert :ok = Directory.remove_if_exists(partition, ["app", "users"])
      assert :ok = Directory.remove_if_exists(partition, ["app", "users"])
    end

    test "paths containing '..' are rejected before touching the database", %{root: root} do
      partition = open_partition(root, ["app"])
      escape_message = "Cannot access path outside partition boundary"

      assert_raise ArgumentError, escape_message, fn -> Directory.create(partition, [".."]) end
      assert_raise ArgumentError, escape_message, fn -> Directory.open(partition, ["../secrets"]) end
      assert_raise ArgumentError, escape_message, fn -> Directory.create_or_open(partition, [".."]) end
      assert_raise ArgumentError, escape_message, fn -> Directory.move(partition, [".."], ["app", "x"]) end
      assert_raise ArgumentError, escape_message, fn -> Directory.move(partition, ["app", "x"], ["../x"]) end
      assert_raise ArgumentError, escape_message, fn -> Directory.remove(partition, [".."]) end
      assert_raise ArgumentError, escape_message, fn -> Directory.remove_if_exists(partition, [".."]) end
      assert_raise ArgumentError, escape_message, fn -> Directory.list(partition, [".."]) end
      assert_raise ArgumentError, escape_message, fn -> Directory.exists?(partition, [".."]) end
    end

    test "non-list paths are rejected for partition operations", %{root: root} do
      partition = open_partition(root, ["app"])

      assert_raise ArgumentError, "Invalid path format for partition operation", fn ->
        Directory.open(partition, "users")
      end
    end
  end

  describe "create_or_open/3" do
    test "rejects the root path", %{root: root} do
      assert {:error, :cannot_create_or_open_root} = Directory.create_or_open(root, [])
    end

    test "creates a missing directory and opens it on subsequent calls", %{root: root} do
      assert {:ok, %Directory.Node{prefix: prefix, path: ["config"]}} = Directory.create_or_open(root, ["config"])
      assert Directory.exists?(root, ["config"])
      assert {:ok, %Directory.Node{prefix: ^prefix}} = Directory.create_or_open(root, ["config"])
    end

    test "propagates parent errors when creating nested directories", %{root: root} do
      assert {:error, :parent_directory_does_not_exist} = Directory.create_or_open(root, ["missing", "child"])
    end
  end

  describe "remove_if_exists/2" do
    test "returns :ok for a directory that does not exist", %{root: root} do
      assert :ok = Directory.remove_if_exists(root, ["nope"])
    end

    test "removes an existing directory", %{root: root} do
      {:ok, _} = Directory.create(root, ["temp"])
      assert :ok = Directory.remove_if_exists(root, ["temp"])
      refute Directory.exists?(root, ["temp"])
    end

    test "refuses to remove the root directory", %{root: root} do
      assert {:error, :cannot_remove_root} = Directory.remove_if_exists(root, [])
    end

    test "passes through version incompatibility errors", %{root: root, store: store} do
      seed_stored_version(store, root, {2, 0, 0})
      assert {:error, :incompatible_directory_version} = Directory.remove_if_exists(root, ["anything"])
    end
  end

  describe "list/2" do
    test "returns a version error when the stored directory version is incompatible", %{root: root, store: store} do
      seed_stored_version(store, root, {2, 0, 0})
      assert {:error, :incompatible_directory_version} = Directory.list(root, ["any"])
    end

    test "does not report a directory as a child of itself", %{root: root} do
      {:ok, _} = Directory.create(root, ["app"])

      assert {:ok, children} = Directory.list(root, ["app"])
      refute "app" in children
    end

    test "defaults to listing the root path", %{root: root} do
      {:ok, _} = Directory.create(root, ["alpha"])
      assert {:ok, children} = Directory.list(root)
      assert "alpha" in children
    end
  end

  describe "path/1 and layer/1 accessors" do
    test "report path and layer for nodes, partitions, and layers", %{root: root} do
      {:ok, node} = Directory.create(root, ["docs"], layer: "document")
      partition = open_partition(root, ["part"])
      layer = root.directory_layer

      assert Directory.path(node) == ["docs"]
      assert Directory.path(partition) == ["part"]
      assert Directory.path(layer) == []

      assert Directory.layer(node) == "document"
      assert Directory.layer(partition) == "partition"
      assert Directory.layer(layer) == nil
    end
  end

  describe "keyspace and key range coercion" do
    test "to_keyspace on a partition applies the partition prefix and options", %{root: root} do
      partition = open_partition(root, ["part"])

      keyspace = Directory.to_keyspace(partition, key_encoding: Bedrock.Encoding.Tuple)

      assert Keyspace.prefix(keyspace) == partition.prefix
      assert keyspace.key_encoding == Bedrock.Encoding.Tuple
    end

    test "to_keyspace on a bare directory layer raises", %{root: root} do
      assert_raise ArgumentError, ~r/Cannot get keyspace from directory layer/, fn ->
        Directory.to_keyspace(root.directory_layer)
      end
    end

    test "ToKeyRange covers the directory prefix for nodes and partitions but raises for layers", %{root: root} do
      {:ok, node} = Directory.create(root, ["docs"])
      partition = open_partition(root, ["part"])

      assert Bedrock.ToKeyRange.to_key_range(node) == KeyRange.from_prefix(node.prefix)
      assert Bedrock.ToKeyRange.to_key_range(partition) == KeyRange.from_prefix(partition.prefix)

      assert_raise ArgumentError, ~r/Cannot get key range from directory layer/, fn ->
        Bedrock.ToKeyRange.to_key_range(root.directory_layer)
      end
    end

    test "ToKeyspace uses the directory prefix for nodes and partitions but raises for layers", %{root: root} do
      {:ok, node} = Directory.create(root, ["docs"])
      partition = open_partition(root, ["part"])

      assert Bedrock.ToKeyspace.to_keyspace(node) == Keyspace.new(node.prefix)
      assert Bedrock.ToKeyspace.to_keyspace(partition) == Keyspace.new(partition.prefix)

      assert_raise ArgumentError, ~r/Cannot get keyspace from directory layer/, fn ->
        Bedrock.ToKeyspace.to_keyspace(root.directory_layer)
      end
    end
  end

  describe "node value encoding" do
    test "a directory created with only a version round-trips it through open", %{root: root} do
      {:ok, created} = Directory.create(root, ["versioned"], version: "v1")
      assert %{version: "v1", metadata: nil} = created

      assert {:ok, %Directory.Node{version: "v1", metadata: nil, layer: nil}} = Directory.open(root, ["versioned"])
    end

    test "a directory created with only metadata round-trips it through open", %{root: root} do
      {:ok, created} = Directory.create(root, ["tagged"], metadata: "owner=me")
      assert %{version: nil, metadata: "owner=me"} = created

      assert {:ok, %Directory.Node{version: nil, metadata: "owner=me", layer: nil}} = Directory.open(root, ["tagged"])
    end

    test "a directory created with version, metadata, and layer round-trips all fields", %{root: root} do
      {:ok, _} = Directory.create(root, ["full"], layer: "document", version: "v2", metadata: "extra")

      assert {:ok, %Directory.Node{layer: "document", version: "v2", metadata: "extra"}} =
               Directory.open(root, ["full"])
    end

    test "a layered directory with a version but no metadata round-trips both fields", %{root: root} do
      {:ok, _} = Directory.create(root, ["layered"], layer: "document", version: "v3")

      assert {:ok, %Directory.Node{layer: "document", version: "v3", metadata: nil}} =
               Directory.open(root, ["layered"])
    end
  end

  describe "hierarchy scans over the real store" do
    # Regression tests for bedrock-j09: directory node keys must be
    # byte-prefixes of their descendants' node keys so that range scans in
    # do_list/do_remove/do_move_recursive actually cover the subtree.

    test "list returns the children of a non-root directory", %{root: root} do
      {:ok, _} = Directory.create(root, ["app"])
      {:ok, _} = Directory.create(root, ["app", "users"])

      assert {:ok, ["users"]} = Directory.list(root, ["app"])
    end

    test "list works at deeper nesting levels", %{root: root} do
      {:ok, _} = Directory.create(root, ["app"])
      {:ok, _} = Directory.create(root, ["app", "users"])
      {:ok, _} = Directory.create(root, ["app", "users", "profiles"])
      {:ok, _} = Directory.create(root, ["app", "users", "settings"])

      assert {:ok, ["users"]} = Directory.list(root, ["app"])
      assert {:ok, children} = Directory.list(root, ["app", "users"])
      assert Enum.sort(children) == ["profiles", "settings"]
    end

    test "list does not report siblings that merely share a name prefix", %{root: root} do
      {:ok, _} = Directory.create(root, ["app"])
      {:ok, _} = Directory.create(root, ["apple"])
      {:ok, _} = Directory.create(root, ["app", "users"])

      assert {:ok, ["users"]} = Directory.list(root, ["app"])
    end

    test "remove deletes the entire subtree, not just the directory itself", %{root: root} do
      {:ok, _} = Directory.create(root, ["app"])
      {:ok, _} = Directory.create(root, ["app", "users"])
      {:ok, _} = Directory.create(root, ["app", "users", "profiles"])

      assert :ok = Directory.remove(root, ["app"])

      refute Directory.exists?(root, ["app"])
      refute Directory.exists?(root, ["app", "users"])
      assert {:error, :directory_does_not_exist} = Directory.open(root, ["app", "users"])
      assert {:error, :directory_does_not_exist} = Directory.open(root, ["app", "users", "profiles"])
    end

    # Regression test for bedrock-esg: do_move_recursive must use proper
    # range arguments for repo.get_range/clear_range and relocate descendants.
    test "move relocates a directory together with all of its descendants", %{root: root} do
      {:ok, _} = Directory.create(root, ["app"])
      {:ok, _} = Directory.create(root, ["app", "users"])
      {:ok, _} = Directory.create(root, ["app", "users", "profiles"])
      {:ok, _} = Directory.create(root, ["archive"])

      assert :ok = Directory.move(root, ["app", "users"], ["archive", "users"])

      refute Directory.exists?(root, ["app", "users"])
      refute Directory.exists?(root, ["app", "users", "profiles"])
      assert {:ok, %Directory.Node{}} = Directory.open(root, ["archive", "users"])
      assert {:ok, %Directory.Node{}} = Directory.open(root, ["archive", "users", "profiles"])
      assert {:ok, ["profiles"]} = Directory.list(root, ["archive", "users"])
    end

    test "packed parent node keys are byte-prefixes of descendant node keys" do
      parent = Directory.NodeKey.pack(["app"])
      child = Directory.NodeKey.pack(["app", "users"])
      grandchild = Directory.NodeKey.pack(["app", "users", "profiles"])
      sibling = Directory.NodeKey.pack(["apple"])

      assert binary_part(child, 0, byte_size(parent)) == parent
      assert binary_part(grandchild, 0, byte_size(child)) == child
      refute match?(<<^parent::binary, _::binary>>, sibling)
    end
  end

  describe "node key encoding" do
    test "the root path round-trips through pack and unpack" do
      assert Directory.NodeKey.pack([]) == <<>>
      assert Directory.NodeKey.unpack(<<>>) == []

      assert ["a", "b"] |> Directory.NodeKey.pack() |> Directory.NodeKey.unpack() == ["a", "b"]
    end
  end
end
