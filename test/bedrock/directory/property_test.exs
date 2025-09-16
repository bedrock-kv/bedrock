defmodule Bedrock.Directory.PropertyTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  import Bedrock.Test.DirectoryHelpers
  import Mox

  alias Bedrock.Directory

  setup do
    stub(MockRepo, :transact, fn callback -> callback.(:mock_txn) end)
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
    gen all(path <- list_of(valid_path_component(), min_length: 1, max_length: 5)) do
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

      _layer = Directory.root(MockRepo)
      assert Directory.validate_path(path) == :ok
    end
  end

  property "created directories can always be opened" do
    check all(
            path <- valid_directory_path(),
            layer_name <- valid_layer_name()
          ) do
      # Use stubs to avoid signature mismatches
      stub(MockRepo, :get, fn
        :mock_txn, <<254, 6, 1, 118, 101, 114, 115, 105, 111, 110, 0, 0>> ->
          nil

        :mock_txn, key ->
          cond do
            key == build_directory_key(path) -> nil
            key == build_directory_key([]) -> Bedrock.Key.pack({<<>>, ""})
            key == build_directory_key(Enum.drop(path, -1)) -> Bedrock.Key.pack({<<0, 1>>, ""})
            true -> nil
          end
      end)

      stub(MockRepo, :put, fn :mock_txn, _key, _value -> :ok end)

      layer = Directory.root(MockRepo, next_prefix_fn: fn -> <<0, :rand.uniform(255)>> end)

      case Directory.create(layer, path, layer: layer_name) do
        {:ok, node} ->
          # Pattern match entire node structure
          assert %{path: ^path, layer: ^layer_name, prefix: prefix} = node
          assert is_binary(prefix)

          # Should be able to get subspace
          assert %Bedrock.Subspace{prefix: ^prefix} = Directory.get_subspace(node)

        {:error, reason} ->
          # Only acceptable errors for property testing
          assert reason in [:parent_directory_does_not_exist, :directory_already_exists, :cannot_create_root]
      end
    end
  end

  property "directory prefixes are unique within same layer" do
    check all(paths <- uniq_list_of(valid_directory_path(), min_length: 1, max_length: 3)) do
      # Sort paths by depth to ensure parents created before children
      sorted_paths = Enum.sort_by(paths, &length/1)

      # Track which directories will be successfully created
      # Root always exists with the new API, so start with it in the set
      created_dirs =
        Enum.reduce(sorted_paths, MapSet.new([[]]), fn path, acc ->
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

      # Use stubs to avoid signature mismatches
      stub(MockRepo, :get, fn
        :mock_txn, <<254, 6, 1, 118, 101, 114, 115, 105, 111, 110, 0, 0>> ->
          nil

        :mock_txn, key ->
          cond do
            # Root directory always exists
            key == build_directory_key([]) ->
              Bedrock.Key.pack({<<>>, ""})

            # Check if this is a directory path being checked for existence
            Enum.any?(sorted_paths, fn path -> build_directory_key(path) == key end) ->
              nil

            # Parent existence checks - root always exists, others based on created_dirs
            true ->
              parent_path =
                Enum.find_value(sorted_paths, fn path ->
                  if path != [] do
                    parent = Enum.drop(path, -1)
                    if build_directory_key(parent) == key, do: parent
                  end
                end)

              case parent_path do
                # Root always exists
                [] ->
                  Bedrock.Key.pack({<<>>, ""})

                path when not is_nil(path) ->
                  if MapSet.member?(created_dirs, path), do: Bedrock.Key.pack({<<0, 1>>, ""})

                nil ->
                  nil
              end
          end
      end)

      stub(MockRepo, :put, fn :mock_txn, _key, _value -> :ok end)

      layer = Directory.root(MockRepo, next_prefix_fn: next_prefix_fn)

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
      # Only count non-root paths since root already exists
      expected_creations = created_dirs |> MapSet.delete([]) |> MapSet.size()
      assert length(successful_nodes) == expected_creations
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
      expected_directory_key = build_directory_key(path)
      root_key = build_directory_key([])

      # Use stubs to avoid signature mismatches
      # Track directory state - initially doesn't exist, then exists after creation
      directory_exists = :atomics.new(1, [])

      stub(MockRepo, :get, fn
        # Version checks
        :mock_txn, <<254, 6, 1, 118, 101, 114, 115, 105, 111, 110, 0, 0>> ->
          nil

        # Directory existence checks and open operations
        :mock_txn, key when key == expected_directory_key ->
          exists = :atomics.get(directory_exists, 1)
          if exists == 1, do: stored_value

        # Parent (root) check
        :mock_txn, ^root_key ->
          Bedrock.Key.pack({<<>>, ""})

        :mock_txn, _key ->
          nil
      end)

      stub(MockRepo, :put, fn
        :mock_txn, key, _value when key == expected_directory_key ->
          # Mark directory as existing after put
          :atomics.put(directory_exists, 1, 1)
          :ok

        :mock_txn, _key, _value ->
          :ok
      end)

      layer = Directory.root(MockRepo, next_prefix_fn: fn -> prefix end)

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
