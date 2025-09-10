defmodule Bedrock.Cluster.Gateway.TransactionBuilder.RangeReadsTest do
  use ExUnit.Case, async: true

  alias Bedrock.Cluster.Gateway.TransactionBuilder.LayoutIndex
  alias Bedrock.Cluster.Gateway.TransactionBuilder.RangeReads
  alias Bedrock.Cluster.Gateway.TransactionBuilder.State
  alias Bedrock.Cluster.Gateway.TransactionBuilder.Tx

  # Common mock functions
  defp standard_async_stream_fn do
    fn servers, task_fn, _opts ->
      servers
      |> Enum.map(task_fn)
      |> Enum.map(&{:ok, &1})
    end
  end

  defp success_storage_fn(data, has_more) do
    fn _server, _min_key, _max_key, _read_version, _opts ->
      {:ok, {data, has_more}}
    end
  end

  defp error_storage_fn(error) do
    fn _server, _min_key, _max_key, _read_version, _opts ->
      {:error, error}
    end
  end

  defp create_standard_layout(storage_ids \\ ["storage1"]) do
    %{
      sequencer: :test_sequencer,
      storage_teams: [
        %{
          key_range: {"", :end},
          storage_ids: storage_ids
        }
      ],
      services: Map.new(storage_ids, &{&1, %{kind: :storage, status: {:up, String.to_atom("#{&1}_pid")}}})
    }
  end

  defp create_test_state(opts \\ []) do
    layout = Keyword.get(opts, :transaction_system_layout, create_standard_layout())
    layout_index = LayoutIndex.build_index(layout)
    tx = Keyword.get(opts, :tx, Tx.new())

    %State{
      state: :valid,
      gateway: :test_gateway,
      transaction_system_layout: layout,
      layout_index: layout_index,
      read_version: Keyword.get(opts, :read_version, 12_345),
      read_version_lease_expiration: nil,
      commit_version: nil,
      tx: tx,
      stack: [],
      fastest_storage_servers: %{},
      fetch_timeout_in_ms: 100,
      lease_renewal_threshold: 100,
      active_range_queries: %{}
    }
  end

  describe "get_range/4" do
    test "returns batch results from single storage team" do
      expected_data = [{"key1", "value1"}, {"key2", "value2"}]

      state = create_test_state()

      opts = [
        async_stream_fn: standard_async_stream_fn(),
        storage_get_range_fn: success_storage_fn(expected_data, false)
      ]

      {_new_state, result} = RangeReads.get_range(state, {"a", "z"}, 10, opts)

      assert {:ok, {^expected_data, false}} = result
    end

    test "handles storage failures gracefully" do
      state = create_test_state()

      opts = [
        async_stream_fn: standard_async_stream_fn(),
        storage_get_range_fn: error_storage_fn(:unavailable)
      ]

      {_new_state, result} = RangeReads.get_range(state, {"a", "z"}, 10, opts)

      assert {:error, :unavailable} = result
    end

    test "respects limit parameter" do
      storage_get_range_fn = fn _server, _min_key, _max_key, _read_version, opts ->
        batch_size = Keyword.get(opts, :limit, 10)
        data = Enum.map(1..batch_size, fn i -> {"key#{i}", "value#{i}"} end)
        {:ok, {data, true}}
      end

      state = create_test_state()
      opts = [async_stream_fn: standard_async_stream_fn(), storage_get_range_fn: storage_get_range_fn]

      {_new_state, result} = RangeReads.get_range(state, {"a", "z"}, 5, opts)

      assert {:ok, {data, true}} = result
      assert length(data) == 5
    end

    test "includes tx writes when storage says no more keys but writes exist beyond batch" do
      storage_data = [
        {"a", "storage_value_a"},
        {"b", "storage_value_b"},
        {"k", "storage_value_k"}
      ]

      # Create tx with writes beyond the storage batch
      tx_writes =
        :gb_trees.insert(
          "m",
          "tx_value_m",
          :gb_trees.insert("n", "tx_value_n", :gb_trees.insert("o", "tx_value_o", :gb_trees.empty()))
        )

      tx_with_writes = %Tx{writes: tx_writes}
      state = create_test_state(tx: tx_with_writes)

      opts = [
        async_stream_fn: standard_async_stream_fn(),
        storage_get_range_fn: success_storage_fn(storage_data, false)
      ]

      {_new_state, result} = RangeReads.get_range(state, {"a", "z"}, 10, opts)

      expected_merged_data = [
        {"a", "storage_value_a"},
        {"b", "storage_value_b"},
        {"k", "storage_value_k"},
        {"m", "tx_value_m"},
        {"n", "tx_value_n"},
        {"o", "tx_value_o"}
      ]

      assert {:ok, {^expected_merged_data, false}} = result
    end
  end
end
