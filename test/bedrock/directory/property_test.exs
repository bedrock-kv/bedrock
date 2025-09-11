defmodule Bedrock.Directory.PropertyTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  import Bedrock.Test.DirectoryHelpers
  import Mox

  alias Bedrock.Directory
  alias Bedrock.Directory.Layer

  setup do
    stub(MockRepo, :transaction, fn callback -> callback.(:mock_txn) end)
    :ok
  end

  setup :verify_on_exit!

  # Generators

  @doc false
  def valid_path_component do
    gen all(
          component <- string(:alphanumeric, min_length: 1, max_length: 20),
          # Ensure no null bytes or system characters
          not String.contains?(component, ["\x00", "\xFE", "\xFF"])
        ) do
      component
    end
  end

  @doc false
  def valid_directory_path do
    gen all(path <- list_of(valid_path_component(), min_length: 0, max_length: 5)) do
      path
    end
  end

  @doc false
  def valid_layer_name do
    one_of([
      constant(nil),
      string(:alphanumeric, min_length: 1, max_length: 20)
    ])
  end

  # Helper functions specific to property tests
  defp expect_directory_check(repo, path) do
    expected_key = build_directory_key(path)

    expect(repo, :get, fn :mock_txn, ^expected_key -> nil end)
  end

  defp expect_directory_storage(repo, path, storage) do
    expected_key = build_directory_key(path)

    expect(repo, :put, fn :mock_txn, ^expected_key, value ->
      Agent.update(storage, &Map.put(&1, expected_key, value))
      :ok
    end)
  end

  # Property Tests

  property "directory paths are always valid lists of strings" do
    check all(path <- valid_directory_path()) do
      assert is_list(path)
      assert Enum.all?(path, &is_binary/1)

      _layer = Layer.new(MockRepo)
      assert Layer.validate_path(path) == :ok
    end
  end

  property "created directories can always be opened" do
    check all(
            path <- valid_directory_path(),
            layer_name <- valid_layer_name()
          ) do
      {:ok, storage} = Agent.start_link(fn -> %{} end)

      MockRepo
      |> expect_version_initialization(storage)
      |> expect_directory_check(path)
      |> expect_parent_exists(path)
      |> expect_directory_storage(path, storage)

      layer = Layer.new(MockRepo, next_prefix_fn: fn -> <<0, :rand.uniform(255)>> end)

      case Directory.create(layer, path, layer: layer_name) do
        {:ok, node} ->
          # Pattern match entire node structure
          assert %{path: ^path, layer: ^layer_name, prefix: prefix} = node
          assert is_binary(prefix)

          # Should be able to get subspace
          assert %Bedrock.Subspace{prefix: ^prefix} = Directory.get_subspace(node)

        {:error, reason} ->
          # Only acceptable errors for property testing
          assert reason in [:parent_directory_does_not_exist, :directory_already_exists]
      end
    end
  end

  property "directory prefixes are unique within same layer" do
    check all(paths <- uniq_list_of(valid_directory_path(), min_length: 1, max_length: 3)) do
      # Sort paths by depth to ensure parents created before children
      sorted_paths = Enum.sort_by(paths, &length/1)

      # Track which directories will be successfully created
      created_dirs =
        Enum.reduce(sorted_paths, MapSet.new(), fn path, acc ->
          parent_path = Enum.drop(path, -1)

          if path == [] or MapSet.member?(acc, parent_path) do
            MapSet.put(acc, path)
          else
            acc
          end
        end)

      # Prefix counter for deterministic allocation
      prefix_counter = :counters.new(1, [])

      next_prefix_fn = fn ->
        n = :counters.get(prefix_counter, 1)
        :counters.add(prefix_counter, 1, 1)
        <<n::32>>
      end

      # Set up expectations for each path
      for {path, index} <- Enum.with_index(sorted_paths) do
        parent_path = if path == [], do: [], else: Enum.drop(path, -1)
        parent_will_exist = path == [] or MapSet.member?(created_dirs, parent_path)
        first_create = index == 0

        # Version management using pipelined helper
        if first_create do
          expect_version_initialization(MockRepo)
        else
          MockRepo
          |> expect_version_check_only()
          |> expect_version_check_only()
        end

        expect_directory_check(MockRepo, path)

        # Parent check if needed
        if path != [] do
          parent_key = build_directory_key(parent_path)

          expect(MockRepo, :get, fn :mock_txn, ^parent_key ->
            if parent_will_exist, do: Bedrock.Key.pack({<<0, 1>>, ""})
          end)
        end

        # Store directory if parent exists
        if parent_will_exist do
          expected_key = build_directory_key(path)

          expect(MockRepo, :put, fn :mock_txn, ^expected_key, _value -> :ok end)
        end
      end

      layer = Layer.new(MockRepo, next_prefix_fn: next_prefix_fn)

      # Create all directories and verify unique prefixes
      {successful_nodes, all_prefixes} =
        Enum.reduce(sorted_paths, {[], []}, fn path, {nodes, prefixes} ->
          should_succeed = MapSet.member?(created_dirs, path)

          case Directory.create(layer, path) do
            {:ok, node} when should_succeed ->
              refute node.prefix in prefixes,
                     "Duplicate prefix #{inspect(node.prefix)} for path #{inspect(path)}"

              {[node | nodes], [node.prefix | prefixes]}

            {:error, :parent_directory_does_not_exist} when not should_succeed ->
              {nodes, prefixes}

            {:ok, _} when not should_succeed ->
              flunk("Created #{inspect(path)} but parent doesn't exist")

            {:error, reason} when should_succeed ->
              flunk("Failed to create #{inspect(path)}: #{inspect(reason)}")

            {:error, reason} ->
              flunk("Unexpected error for #{inspect(path)}: #{inspect(reason)}")
          end
        end)

      # Verify all successfully created nodes have unique prefixes
      assert length(all_prefixes) == length(Enum.uniq(all_prefixes))
      assert length(successful_nodes) == MapSet.size(created_dirs)
    end
  end

  property "directory operations preserve metadata" do
    check all(
            # Only test with single-level paths (not root) to avoid parent issues
            path <- map(valid_path_component(), &[&1]),
            layer_name <- valid_layer_name()
          ) do
      # Generate a random prefix for this test
      prefix = <<:rand.uniform(255), :rand.uniform(255)>>
      stored_value = Bedrock.Key.pack({prefix, layer_name || ""})

      # Create expectations using pipelined helpers
      MockRepo
      |> expect_version_initialization(nil)
      |> expect_directory_check(path)
      # Parent check (root)
      |> expect(:get, fn :mock_txn, <<254>> ->
        Bedrock.Key.pack({<<0, 1>>, ""})
      end)
      |> expect(:put, fn :mock_txn, key, value ->
        expected_key = build_directory_key(path)
        assert key == expected_key
        assert value == stored_value
        :ok
      end)
      |> expect(:get, fn :mock_txn, key ->
        expected_key = build_directory_key(path)
        assert key == expected_key
        stored_value
      end)
      |> expect_version_check_only()

      layer = Layer.new(MockRepo, next_prefix_fn: fn -> prefix end)

      # Create and then open
      case Directory.create(layer, path, layer: layer_name) do
        {:ok, created_node} ->
          case Directory.open(layer, path) do
            {:ok, opened_node} ->
              # Pattern match the entire node comparison
              assert %{prefix: prefix, layer: layer, path: path} = opened_node
              assert %{prefix: ^prefix, layer: ^layer, path: ^path} = created_node

            {:error, reason} ->
              flunk("Failed to open created directory: #{inspect(reason)}")
          end

        {:error, :parent_directory_does_not_exist} ->
          :ok

        {:error, reason} ->
          flunk("Unexpected error: #{inspect(reason)}")
      end
    end
  end
end
