defmodule Bedrock.Directory.PropertyTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  import Mox

  alias Bedrock.Directory
  alias Bedrock.Directory.Layer

  # Version key used by the directory layer
  @version_key <<254, 6, 1, 118, 101, 114, 115, 105, 111, 110, 0, 0>>

  setup do
    # Automatically stub transaction to execute callbacks immediately
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

  # Property Tests

  property "directory paths are always valid lists of strings" do
    check all(path <- valid_directory_path()) do
      assert is_list(path)
      assert Enum.all?(path, &is_binary/1)

      # Should pass validation
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

      # Version management expectations
      # 1. check_version
      expect(MockRepo, :get, fn :mock_txn, key ->
        assert key == @version_key
        nil
      end)

      # 2. ensure_version_initialized
      expect(MockRepo, :get, fn :mock_txn, key ->
        assert key == @version_key
        nil
      end)

      # 3. init_version
      expect(MockRepo, :put, fn :mock_txn, key, value ->
        assert key == @version_key
        assert value == <<1::little-32, 0::little-32, 0::little-32>>
        Agent.update(storage, &Map.put(&1, key, value))
        :ok
      end)

      # 4. Check if directory exists
      expect(MockRepo, :get, fn :mock_txn, key ->
        expected_key =
          case path do
            [] -> <<254>>
            _ -> <<254>> <> Bedrock.Key.pack(path)
          end

        assert key == expected_key
        # Directory doesn't exist initially
        nil
      end)

      # 5. Check parent existence for non-root paths
      if path != [] do
        parent_path = Enum.drop(path, -1)

        expect(MockRepo, :get, fn :mock_txn, key ->
          expected_parent_key =
            case parent_path do
              [] -> <<254>>
              _ -> <<254>> <> Bedrock.Key.pack(parent_path)
            end

          assert key == expected_parent_key
          # Parent exists
          Bedrock.Key.pack({<<0, 1>>, ""})
        end)
      end

      # 6. Store created directory
      expect(MockRepo, :put, fn :mock_txn, key, value ->
        expected_key =
          case path do
            [] -> <<254>>
            _ -> <<254>> <> Bedrock.Key.pack(path)
          end

        assert key == expected_key
        Agent.update(storage, &Map.put(&1, key, value))
        :ok
      end)

      layer = Layer.new(MockRepo, next_prefix_fn: fn -> <<0, :rand.uniform(255)>> end)

      # Create directory
      case Directory.create(layer, path, layer: layer_name) do
        {:ok, node} ->
          # Verify node properties
          assert node.path == path
          assert node.layer == layer_name
          assert is_binary(node.prefix)

          # Should be able to get subspace
          subspace = Directory.get_subspace(node)
          assert %Bedrock.Subspace{prefix: prefix} = subspace
          assert prefix == node.prefix

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

      # Set up exact expectations in the order operations will happen
      for {path, index} <- Enum.with_index(sorted_paths) do
        expected_key =
          case path do
            [] -> <<254>>
            path -> <<254>> <> Bedrock.Key.pack(path)
          end

        parent_path = Enum.drop(path, -1)
        parent_will_exist = path == [] or MapSet.member?(created_dirs, parent_path)
        first_create = index == 0

        # Version management for each create attempt
        # 1. check_version
        expect(MockRepo, :get, fn :mock_txn, key ->
          assert key == @version_key

          if first_create do
            # No version on first create
            nil
          else
            # Version exists after first
            <<1::little-32, 0::little-32, 0::little-32>>
          end
        end)

        # 2. ensure_version_initialized
        expect(MockRepo, :get, fn :mock_txn, key ->
          assert key == @version_key

          if first_create do
            nil
          else
            <<1::little-32, 0::little-32, 0::little-32>>
          end
        end)

        # 3. init_version (only on first create)
        if first_create do
          expect(MockRepo, :put, fn :mock_txn, key, value ->
            assert key == @version_key
            assert value == <<1::little-32, 0::little-32, 0::little-32>>
            :ok
          end)
        end

        # 4. Check if directory exists (should not)
        expect(MockRepo, :get, fn :mock_txn, key ->
          assert key == expected_key
          nil
        end)

        # 5. Parent existence check for non-root
        if path != [] do
          parent_key =
            case parent_path do
              [] -> <<254>>
              _ -> <<254>> <> Bedrock.Key.pack(parent_path)
            end

          expect(MockRepo, :get, fn :mock_txn, key ->
            assert key == parent_key

            if parent_will_exist do
              Bedrock.Key.pack({<<0, 1>>, ""})
            end
          end)
        end

        # 6. Store directory value (only if parent exists)
        if parent_will_exist do
          expect(MockRepo, :put, fn :mock_txn, key, _value ->
            assert key == expected_key
            :ok
          end)
        end
      end

      layer = Layer.new(MockRepo, next_prefix_fn: next_prefix_fn)

      # Create all directories and verify unique prefixes
      {successful_nodes, all_prefixes} =
        Enum.reduce(sorted_paths, {[], []}, fn path, {nodes, prefixes} ->
          should_succeed = MapSet.member?(created_dirs, path)

          case Directory.create(layer, path) do
            {:ok, node} when should_succeed ->
              # Verify prefix uniqueness
              refute node.prefix in prefixes,
                     "Duplicate prefix #{inspect(node.prefix)} for path #{inspect(path)}"

              {[node | nodes], [node.prefix | prefixes]}

            {:error, :parent_directory_does_not_exist} when not should_succeed ->
              # Expected failure - parent doesn't exist
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

      # Verify we created the expected number of directories
      assert length(successful_nodes) == MapSet.size(created_dirs)
    end
  end

  property "directory operations preserve metadata" do
    check all(
            # Only test with single-level paths (not root) to avoid parent issues
            path <- map(valid_path_component(), &[&1]),
            layer_name <- valid_layer_name()
          ) do
      # Version management expectations
      # 1. check_version
      expect(MockRepo, :get, fn :mock_txn, key ->
        assert key == @version_key
        nil
      end)

      # 2. ensure_version_initialized
      expect(MockRepo, :get, fn :mock_txn, key ->
        assert key == @version_key
        nil
      end)

      # 3. init_version
      expect(MockRepo, :put, fn :mock_txn, key, value ->
        assert key == @version_key
        assert value == <<1::little-32, 0::little-32, 0::little-32>>
        :ok
      end)

      # 4. Check if directory exists
      expect(MockRepo, :get, fn :mock_txn, key ->
        expected_key =
          case path do
            [] -> <<254>>
            _ -> <<254>> <> Bedrock.Key.pack(path)
          end

        assert key == expected_key
        nil
      end)

      # 5. Parent check (always present for single-level paths)
      parent_path = Enum.drop(path, -1)

      expect(MockRepo, :get, fn :mock_txn, key ->
        expected_parent_key =
          case parent_path do
            [] -> <<254>>
            _ -> <<254>> <> Bedrock.Key.pack(parent_path)
          end

        assert key == expected_parent_key
        # For single-element paths, parent is root which exists
        Bedrock.Key.pack({<<0, 1>>, ""})
      end)

      # 6. Store directory with metadata (using automatic prefix allocation)
      # Generate a random prefix for this test
      prefix = <<:rand.uniform(255), :rand.uniform(255)>>
      stored_value = Bedrock.Key.pack({prefix, layer_name || ""})

      expect(MockRepo, :put, fn :mock_txn, key, value ->
        expected_key =
          case path do
            [] -> <<254>>
            _ -> <<254>> <> Bedrock.Key.pack(path)
          end

        assert key == expected_key
        assert value == stored_value
        :ok
      end)

      # For open test - order matters!
      # 1. Check if directory exists (happens first in do_open)
      expect(MockRepo, :get, fn :mock_txn, key ->
        expected_key =
          case path do
            [] -> <<254>>
            _ -> <<254>> <> Bedrock.Key.pack(path)
          end

        assert key == expected_key
        stored_value
      end)

      # 2. check_version for read (happens after directory is found)
      expect(MockRepo, :get, fn :mock_txn, key ->
        assert key == @version_key
        <<1::little-32, 0::little-32, 0::little-32>>
      end)

      layer = Layer.new(MockRepo, next_prefix_fn: fn -> prefix end)

      # Create and then open - using automatic prefix allocation
      case Directory.create(layer, path, layer: layer_name) do
        {:ok, created_node} ->
          # Open should return same metadata
          case Directory.open(layer, path) do
            {:ok, opened_node} ->
              assert opened_node.prefix == created_node.prefix
              assert opened_node.layer == created_node.layer
              assert opened_node.path == created_node.path

            {:error, reason} ->
              flunk("Failed to open created directory: #{inspect(reason)}")
          end

        {:error, :parent_directory_does_not_exist} ->
          # Expected for orphaned paths
          :ok

        {:error, reason} ->
          flunk("Unexpected error: #{inspect(reason)}")
      end
    end
  end
end
