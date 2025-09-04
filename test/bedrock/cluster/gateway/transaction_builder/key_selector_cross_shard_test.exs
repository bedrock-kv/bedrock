defmodule Bedrock.Cluster.Gateway.TransactionBuilder.KeySelectorCrossShardTest do
  use ExUnit.Case, async: true

  alias Bedrock.Cluster.Gateway.TransactionBuilder.KeySelectorResolution
  alias Bedrock.Cluster.Gateway.TransactionBuilder.LayoutIndex
  alias Bedrock.KeySelector

  setup do
    :ok
  end

  describe "cross-shard single KeySelector resolution" do
    test "single KeySelector resolves within shard boundaries" do
      layout_index = create_mock_layout_index([{"shard1", <<>>, "m"}, {"shard2", "m", :end}])

      storage_fetch_fn = fn :shard1_pid, %KeySelector{key: "j"}, _version, _opts ->
        {:ok, {"j", "value_j"}}
      end

      result =
        KeySelectorResolution.resolve_key_selector(
          layout_index,
          KeySelector.first_greater_or_equal("j"),
          1,
          storage_fetch_fn: storage_fetch_fn,
          timeout: 5000
        )

      assert {:ok, {"j", "value_j"}} = result
    end

    test "single KeySelector with large positive offset crosses shard boundary" do
      layout_index = create_mock_layout_index([{"shard1", <<>>, "m"}, {"shard2", "m", :end}])

      storage_fetch_fn = fn
        :shard1_pid, %KeySelector{key: "k", offset: 10}, _version, _opts ->
          {:partial, 3}

        :shard2_pid, %KeySelector{key: <<109, 0>>, offset: 7, or_equal: true}, _version, _opts ->
          {:ok, {"t", "value_t"}}

        pid, selector, _version, _opts ->
          {:error, {:unexpected_call, pid, selector}}
      end

      result =
        KeySelectorResolution.resolve_key_selector(
          layout_index,
          "k" |> KeySelector.first_greater_or_equal() |> KeySelector.add(10),
          1,
          storage_fetch_fn: storage_fetch_fn,
          timeout: 5000
        )

      assert {:ok, {"t", "value_t"}} = result
    end

    test "single KeySelector with large negative offset crosses shard boundary backward" do
      layout_index = create_mock_layout_index([{"shard1", <<>>, "m"}, {"shard2", "m", :end}])

      storage_fetch_fn = fn
        :shard2_pid, %KeySelector{key: "p", offset: -10}, _version, _opts ->
          {:partial, 3}

        :shard1_pid, %KeySelector{key: <<>>, offset: -7, or_equal: true}, _version, _opts ->
          {:ok, {"f", "value_f"}}

        pid, selector, _version, _opts ->
          {:error, {:unexpected_call, pid, selector}}
      end

      result =
        KeySelectorResolution.resolve_key_selector(
          layout_index,
          "p" |> KeySelector.first_greater_or_equal() |> KeySelector.add(-10),
          1,
          storage_fetch_fn: storage_fetch_fn,
          timeout: 5000
        )

      assert {:ok, {"f", "value_f"}} = result
    end

    test "KeySelector at keyspace boundary returns not_found rather than error" do
      layout_index = create_mock_layout_index([{"shard1", <<>>, "m"}, {"shard2", "m", :end}])

      storage_fetch_fn = fn
        :shard1_pid, %KeySelector{key: "a", offset: -5}, _version, _opts ->
          {:partial, 2}

        pid, selector, _version, _opts ->
          {:error, {:unexpected_call, pid, selector}}
      end

      result =
        KeySelectorResolution.resolve_key_selector(
          layout_index,
          "a" |> KeySelector.first_greater_or_equal() |> KeySelector.add(-5),
          1,
          storage_fetch_fn: storage_fetch_fn,
          timeout: 5000
        )

      # Should return not_found (no more data) rather than clamped (error)
      assert {:error, :not_found} = result
    end
  end

  describe "cross-shard range KeySelector resolution" do
    test "range KeySelector entirely within single shard" do
      layout_index = create_mock_layout_index([{"shard1", <<>>, "m"}, {"shard2", "m", :end}])

      storage_range_fetch_fn = fn
        :shard1_pid, %KeySelector{key: "d"}, %KeySelector{key: "j"}, _version, _opts ->
          {:ok, [{"e", "value_e"}, {"f", "value_f"}, {"g", "value_g"}]}

        pid, start_sel, end_sel, _version, _opts ->
          {:error, {:unexpected_call, pid, start_sel, end_sel}}
      end

      result =
        KeySelectorResolution.resolve_key_selector_range(
          layout_index,
          KeySelector.first_greater_or_equal("d"),
          KeySelector.first_greater_than("j"),
          1,
          storage_range_fetch_fn: storage_range_fetch_fn,
          limit: 100
        )

      assert {:ok, results} = result
      assert results == [{"e", "value_e"}, {"f", "value_f"}, {"g", "value_g"}]
    end

    test "range KeySelector spans multiple shards with continuation" do
      layout_index = create_mock_layout_index([{"shard1", <<>>, "m"}, {"shard2", "m", :end}])

      storage_range_fetch_fn = fn
        :shard1_pid, %KeySelector{key: "j"}, %KeySelector{key: "r"}, _version, _opts ->
          continuation_selector = KeySelector.first_greater_or_equal(<<109, 0>>)
          {:partial, [{"k", "value_k"}, {"l", "value_l"}], continuation_selector}

        :shard2_pid, %KeySelector{key: <<109, 0>>, or_equal: true}, %KeySelector{key: "r"}, _version, _opts ->
          {:ok, [{"p", "value_p"}, {"q", "value_q"}]}

        pid, start_sel, end_sel, _version, _opts ->
          {:error, {:unexpected_call, pid, start_sel, end_sel}}
      end

      result =
        KeySelectorResolution.resolve_key_selector_range(
          layout_index,
          KeySelector.first_greater_or_equal("j"),
          KeySelector.first_greater_than("r"),
          1,
          storage_range_fetch_fn: storage_range_fetch_fn,
          limit: 100
        )

      assert {:ok, results} = result
      assert results == [{"k", "value_k"}, {"l", "value_l"}, {"p", "value_p"}, {"q", "value_q"}]
    end
  end

  describe "error handling in cross-shard scenarios" do
    test "handles storage server failures during cross-shard resolution" do
      layout_index = create_mock_layout_index([{"shard1", <<>>, "m"}, {"shard2", "m", :end}])

      storage_fetch_fn = fn
        :shard1_pid, %KeySelector{key: "j", offset: 10}, _version, _opts ->
          {:partial, 5}

        :shard2_pid, %KeySelector{key: <<109, 0>>, offset: 5, or_equal: true}, _version, _opts ->
          {:error, :unavailable}

        pid, selector, _version, _opts ->
          {:error, {:unexpected_call, pid, selector}}
      end

      result =
        KeySelectorResolution.resolve_key_selector(
          layout_index,
          "j" |> KeySelector.first_greater_or_equal() |> KeySelector.add(10),
          1,
          storage_fetch_fn: storage_fetch_fn,
          timeout: 5000
        )

      assert {:error, :unavailable} = result
    end

    test "handles version mismatches during cross-shard resolution" do
      layout_index = create_mock_layout_index([{"shard1", <<>>, "m"}, {"shard2", "m", :end}])

      storage_fetch_fn = fn
        :shard1_pid, %KeySelector{key: "k", offset: 5}, _version, _opts ->
          {:partial, 3}

        :shard2_pid, %KeySelector{key: <<109, 0>>, offset: 2, or_equal: true}, 100, _opts ->
          {:error, :version_too_new}

        :shard2_pid, %KeySelector{key: <<109, 0>>, offset: 2, or_equal: true}, _version, _opts ->
          {:ok, {"p", "value_p"}}

        pid, selector, version, _opts ->
          {:error, {:unexpected_call, pid, selector, version}}
      end

      result =
        KeySelectorResolution.resolve_key_selector(
          layout_index,
          "k" |> KeySelector.first_greater_or_equal() |> KeySelector.add(5),
          100,
          storage_fetch_fn: storage_fetch_fn,
          timeout: 5000
        )

      assert {:error, :version_too_new} = result
    end
  end

  defp create_mock_layout_index(shard_specs) do
    segments = build_contiguous_segments(shard_specs, <<>>, [])
    tree = :gb_trees.from_orddict(segments)
    %LayoutIndex{tree: tree}
  end

  defp build_contiguous_segments([], _current_start, acc), do: Enum.reverse(acc)

  defp build_contiguous_segments([{shard_id, _spec_start, end_key} | rest], current_start, acc) do
    mock_pid = String.to_atom("#{shard_id}_pid")

    normalized_end =
      case end_key do
        :end -> <<0xFF, 0xFF>>
        key when is_binary(key) -> key <> "\x00"
      end

    segment = {normalized_end, {current_start, [mock_pid]}}
    build_contiguous_segments(rest, normalized_end, [segment | acc])
  end
end
