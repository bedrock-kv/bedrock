defmodule Bedrock.Cluster.Gateway.TransactionBuilder.KeySelectorCrossShardTest do
  use ExUnit.Case, async: true

  alias Bedrock.Cluster.Gateway.TransactionBuilder.KeySelectorResolution
  alias Bedrock.Cluster.Gateway.TransactionBuilder.LayoutIndex
  alias Bedrock.KeySelector

  setup do
    # Clean up any test state between tests
    cleanup_test_mocks()
    :ok
  end

  describe "cross-shard single KeySelector resolution" do
    test "single KeySelector resolves within shard boundaries" do
      # Mock a layout index with predictable shard boundaries
      layout_index =
        create_mock_layout_index([
          {"shard1", "a", "m"},
          {"shard2", "n", "z"}
        ])

      # Mock storage server responses using the dependency injection system
      setup_storage_mocks(%{
        :shard1_pid => fn %KeySelector{key: "j"}, _version ->
          {:ok, {"j", "value_j"}}
        end
      })

      key_selector = KeySelector.first_greater_or_equal("j")

      result =
        KeySelectorResolution.resolve_key_selector(
          layout_index,
          key_selector,
          1,
          timeout: 5000
        )

      assert {:ok, {"j", "value_j"}} = result
    end

    test "single KeySelector with large positive offset crosses shard boundary" do
      layout_index =
        create_mock_layout_index([
          {"shard1", "a", "m"},
          {"shard2", "n", "z"}
        ])

      # Mock storage server responses and shard boundaries
      setup_storage_mocks(%{
        :shard1_pid => fn key_selector, _version ->
          # Simulate partial resolution - not enough keys in this shard
          case key_selector do
            # Only 3 keys available
            %KeySelector{key: "k", offset: 10} -> {:partial, 3}
            _ -> {:error, :not_found}
          end
        end,
        :shard2_pid => fn key_selector, _version ->
          # Should receive continuation with adjusted offset
          case key_selector do
            # 10 - 3 = 7
            %KeySelector{key: "n", offset: 7} -> {:ok, {"t", "value_t"}}
            _ -> {:error, :not_found}
          end
        end
      })

      setup_shard_boundaries("n")

      # KeySelector that needs to skip 10 keys from "k"
      key_selector = "k" |> KeySelector.first_greater_or_equal() |> KeySelector.add(10)

      result =
        KeySelectorResolution.resolve_key_selector(
          layout_index,
          key_selector,
          1,
          timeout: 5000
        )

      assert {:ok, {"t", "value_t"}} = result
    end

    test "single KeySelector with large negative offset crosses shard boundary backward" do
      layout_index =
        create_mock_layout_index([
          {"shard1", "a", "m"},
          {"shard2", "n", "z"}
        ])

      setup_storage_mocks(%{
        :shard2_pid => fn key_selector, _version ->
          # Simulate partial resolution going backward
          case key_selector do
            # Only 3 keys available backward
            %KeySelector{key: "p", offset: -10} -> {:partial, 3}
            _ -> {:error, :not_found}
          end
        end,
        :shard1_pid => fn key_selector, _version ->
          # Should receive continuation with adjusted offset from end of shard1
          case key_selector do
            # 10 - 3 = 7
            %KeySelector{key: "m", offset: -7} -> {:ok, {"f", "value_f"}}
            _ -> {:error, :not_found}
          end
        end
      })

      # next shard start, previous shard end
      setup_shard_boundaries("n", "m")

      # KeySelector that needs to go back 10 keys from "p"
      key_selector = "p" |> KeySelector.first_greater_or_equal() |> KeySelector.add(-10)

      result =
        KeySelectorResolution.resolve_key_selector(
          layout_index,
          key_selector,
          1,
          timeout: 5000
        )

      assert {:ok, {"f", "value_f"}} = result
    end

    test "single KeySelector resolution fails when crossing too many shards" do
      layout_index =
        create_mock_layout_index([
          {"shard1", "a", "d"},
          {"shard2", "e", "h"},
          {"shard3", "i", "l"},
          {"shard4", "m", "p"},
          {"shard5", "q", "z"}
        ])

      # Mock all shards to return partial resolution
      setup_storage_mocks(%{
        :shard1_pid => fn _, _ -> {:partial, 2} end,
        :shard2_pid => fn _, _ -> {:partial, 2} end,
        :shard3_pid => fn _, _ -> {:partial, 2} end,
        :shard4_pid => fn _, _ -> {:partial, 2} end,
        :shard5_pid => fn _, _ -> {:partial, 2} end
      })

      # Set up shard boundary progression: a->e->i->m->q
      # First boundary, others will be set dynamically
      setup_shard_boundaries("e")

      # KeySelector that needs to skip way more keys than available
      key_selector = "a" |> KeySelector.first_greater_or_equal() |> KeySelector.add(1000)

      result =
        KeySelectorResolution.resolve_key_selector(
          layout_index,
          key_selector,
          1,
          timeout: 5000
        )

      # Should eventually give up and return clamped due to circuit breaker
      assert {:error, :clamped} = result
    end
  end

  describe "cross-shard range KeySelector resolution" do
    test "range KeySelector entirely within single shard" do
      layout_index =
        create_mock_layout_index([
          {"shard1", "a", "m"},
          {"shard2", "n", "z"}
        ])

      setup_range_mocks(%{
        :shard1_pid => fn start_selector, end_selector, _version, _opts ->
          # Both selectors resolve within shard1
          case {start_selector.key, end_selector.key} do
            {"d", "j"} -> {:ok, [{"e", "value_e"}, {"f", "value_f"}, {"g", "value_g"}]}
            _ -> {:error, :not_found}
          end
        end
      })

      start_selector = KeySelector.first_greater_or_equal("d")
      end_selector = KeySelector.first_greater_than("j")

      result =
        KeySelectorResolution.resolve_key_selector_range(
          layout_index,
          start_selector,
          end_selector,
          1,
          limit: 100
        )

      assert {:ok, results} = result
      assert length(results) == 3
      assert {"e", "value_e"} in results
      assert {"f", "value_f"} in results
      assert {"g", "value_g"} in results
    end

    test "range KeySelector spans multiple shards - currently clamped" do
      layout_index =
        create_mock_layout_index([
          {"shard1", "a", "m"},
          {"shard2", "n", "z"}
        ])

      # Cross-shard ranges aren't fully implemented yet, so no need to set up mocks
      # The implementation should return :clamped for cross-shard ranges

      # shard1
      start_selector = KeySelector.first_greater_or_equal("j")
      # shard2
      end_selector = KeySelector.first_greater_than("r")

      result =
        KeySelectorResolution.resolve_key_selector_range(
          layout_index,
          start_selector,
          end_selector,
          1,
          limit: 100
        )

      # Current implementation should return :clamped for cross-shard ranges
      assert {:error, :clamped} = result
    end

    test "range KeySelector with limit stops at shard boundary - currently clamped" do
      layout_index =
        create_mock_layout_index([
          {"shard1", "a", "m"},
          {"shard2", "n", "z"}
        ])

      # This test would span multiple shards, so current implementation clamps it
      # shard1
      start_selector = KeySelector.first_greater_or_equal("d")
      # shard2
      end_selector = KeySelector.first_greater_than("z")

      result =
        KeySelectorResolution.resolve_key_selector_range(
          layout_index,
          start_selector,
          end_selector,
          1,
          limit: 3
        )

      # Current implementation should return :clamped for cross-shard ranges
      assert {:error, :clamped} = result
    end

    test "range KeySelector returns clamped when crossing too many shards" do
      # For now, cross-shard ranges are clamped in the current implementation
      layout_index =
        create_mock_layout_index([
          {"shard1", "a", "d"},
          {"shard2", "e", "h"},
          {"shard3", "i", "z"}
        ])

      start_selector = KeySelector.first_greater_or_equal("b")
      # Spans all 3 shards
      end_selector = KeySelector.first_greater_than("y")

      result =
        KeySelectorResolution.resolve_key_selector_range(
          layout_index,
          start_selector,
          end_selector,
          1,
          limit: 100
        )

      # Current implementation should return clamped for multi-shard ranges
      assert {:error, :clamped} = result
    end
  end

  describe "error handling in cross-shard scenarios" do
    test "handles storage server failures during cross-shard resolution" do
      layout_index =
        create_mock_layout_index([
          {"shard1", "a", "m"},
          {"shard2", "n", "z"}
        ])

      setup_storage_mocks(%{
        # Needs continuation
        :shard1_pid => fn _, _ -> {:partial, 5} end,
        # Server failed
        :shard2_pid => fn _, _ -> {:error, :unavailable} end
      })

      setup_shard_boundaries("n")

      key_selector = "j" |> KeySelector.first_greater_or_equal() |> KeySelector.add(10)

      result =
        KeySelectorResolution.resolve_key_selector(
          layout_index,
          key_selector,
          1,
          timeout: 5000
        )

      assert {:error, :unavailable} = result
    end

    test "handles version mismatches during cross-shard resolution" do
      layout_index =
        create_mock_layout_index([
          {"shard1", "a", "m"},
          {"shard2", "n", "z"}
        ])

      setup_storage_mocks(%{
        :shard1_pid => fn _, _ -> {:partial, 3} end,
        :shard2_pid => fn _, version ->
          case version do
            100 -> {:error, :version_too_new}
            _ -> {:ok, {"p", "value_p"}}
          end
        end
      })

      setup_shard_boundaries("n")

      key_selector = "k" |> KeySelector.first_greater_or_equal() |> KeySelector.add(5)

      result =
        KeySelectorResolution.resolve_key_selector(
          layout_index,
          key_selector,
          # Version too new for shard2
          100,
          timeout: 5000
        )

      assert {:error, :version_too_new} = result
    end
  end

  # Test utility functions

  defp create_mock_layout_index(shard_specs) do
    # Create a mock LayoutIndex struct that works with the real KeySelectorResolution
    # Need to handle the gb_trees format correctly with proper end-key normalization

    # Convert shard specs to normalized segments for gb_trees
    segments =
      Enum.map(shard_specs, fn {shard_id, start_key, end_key} ->
        mock_pid = String.to_atom("#{shard_id}_pid")
        # Normalize end key: :end becomes sentinel, others stay as-is
        normalized_end = if end_key == :end, do: <<0xFF, 0xFF>>, else: end_key <> "\x00"
        {normalized_end, {start_key, [mock_pid]}}
      end)

    # Build a gb_tree from the segments
    tree = :gb_trees.from_orddict(segments)

    %LayoutIndex{tree: tree}
  end

  defp setup_storage_mocks(mock_map) do
    # Set up mocks using the dependency injection system in KeySelectorResolution
    Enum.each(mock_map, fn {mock_pid, response_fn} ->
      Process.put({:test_storage_mock, mock_pid}, response_fn)
    end)
  end

  defp setup_range_mocks(mock_map) do
    # Set up range mocks for Storage.range_fetch calls
    Enum.each(mock_map, fn {mock_pid, response_fn} ->
      Process.put({:test_range_storage_mock, mock_pid}, response_fn)
    end)
  end

  defp setup_shard_boundaries(next_shard_start, prev_shard_end \\ nil) do
    # Set up mock shard boundary information for cross-shard continuation
    Process.put(:test_next_shard_start, next_shard_start)
    if prev_shard_end, do: Process.put(:test_prev_shard_end, prev_shard_end)
  end

  defp cleanup_test_mocks do
    # Clean up all test-related process dictionary entries
    Process.get()
    |> Enum.filter(fn {key, _} ->
      match?({:test_storage_mock, _}, key) or
        match?({:test_range_storage_mock, _}, key) or
        key in [:test_next_shard_start, :test_prev_shard_end]
    end)
    |> Enum.each(fn {key, _} -> Process.delete(key) end)
  end
end
