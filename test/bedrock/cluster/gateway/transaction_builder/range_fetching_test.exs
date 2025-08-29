defmodule Bedrock.Cluster.Gateway.TransactionBuilder.RangeFetchingTest do
  use ExUnit.Case, async: true

  alias Bedrock.Cluster.Gateway.TransactionBuilder.LayoutUtils
  alias Bedrock.Cluster.Gateway.TransactionBuilder.RangeFetching
  alias Bedrock.Cluster.Gateway.TransactionBuilder.State
  alias Bedrock.Cluster.Gateway.TransactionBuilder.Tx

  defmodule TestKeyCodec do
    @moduledoc false
    def encode_key(key), do: {:ok, key}
    def decode_key(key), do: {:ok, key}
  end

  defmodule TestValueCodec do
    @moduledoc false
    def encode_value(value), do: {:ok, value}
    def decode_value(value), do: {:ok, value}
  end

  defp create_test_state(opts) do
    layout =
      Keyword.get(opts, :transaction_system_layout, %{
        sequencer: :test_sequencer,
        storage_teams: [],
        services: %{}
      })

    layout_index = LayoutUtils.build_layout_index(layout)

    %State{
      state: :valid,
      gateway: :test_gateway,
      transaction_system_layout: layout,
      layout_index: layout_index,
      key_codec: TestKeyCodec,
      value_codec: TestValueCodec,
      read_version: Keyword.get(opts, :read_version),
      read_version_lease_expiration: nil,
      commit_version: nil,
      tx: Tx.new(),
      stack: [],
      fastest_storage_servers: %{},
      fetch_timeout_in_ms: 100,
      lease_renewal_threshold: 100,
      active_range_queries: %{}
    }
  end

  describe "batch range fetching with do_range_batch/4" do
    test "returns batch results from single storage team" do
      # Create state with mock storage function that covers our query range
      layout = %{
        sequencer: :test_sequencer,
        storage_teams: [
          %{
            key_range: {"", :end},
            storage_ids: ["storage1"]
          }
        ],
        services: %{
          "storage1" => %{kind: :storage, status: {:up, :storage1_pid}}
        }
      }

      state = create_test_state(transaction_system_layout: layout, read_version: 12_345)

      # Create mock storage function that tracks calls
      call_tracker = fn -> [] end |> Agent.start_link() |> elem(1)

      mock_storage_fn = fn _pids, {start_key, end_key}, _version, _batch_size, _timeout ->
        Agent.update(call_tracker, fn calls ->
          [{start_key, end_key} | calls]
        end)

        # Return some test data
        data = [
          {"#{start_key}1", "value1"},
          {"#{start_key}2", "value2"}
        ]

        {:ok, %{data: data, has_more: false}}
      end

      opts = [storage_fetch_fn: mock_storage_fn]

      # Call do_range_batch
      {_new_state, result} = RangeFetching.do_range_batch(state, {"a", "z"}, 10, opts)

      # Should succeed with batch and continuation
      assert {:ok, data, continuation} = result
      assert is_list(data)
      assert continuation == :finished

      # Storage function should have been called
      calls = Agent.get(call_tracker, & &1)
      assert length(calls) > 0

      Agent.stop(call_tracker)
    end

    test "handles single storage team correctly" do
      layout = %{
        sequencer: :test_sequencer,
        storage_teams: [
          %{
            key_range: {"", :end},
            storage_ids: ["storage1"]
          }
        ],
        services: %{
          "storage1" => %{kind: :storage, status: {:up, :storage1_pid}}
        }
      }

      state = create_test_state(transaction_system_layout: layout, read_version: 12_345)

      mock_storage_fn = fn _pids, {_start_key, _end_key}, _version, _batch_size, _timeout ->
        data = [{"key1", "value1"}, {"key2", "value2"}]
        {:ok, %{data: data, has_more: false}}
      end

      opts = [storage_fetch_fn: mock_storage_fn]

      # Call do_range_batch
      {_new_state, result} = RangeFetching.do_range_batch(state, {"a", "z"}, 10, opts)

      # Should succeed with batch and continuation
      assert {:ok, data, continuation} = result
      assert data == [{"key1", "value1"}, {"key2", "value2"}]
      assert continuation == :finished
    end

    test "handles storage failures gracefully" do
      layout = %{
        sequencer: :test_sequencer,
        storage_teams: [
          %{
            key_range: {"", :end},
            storage_ids: ["storage1"]
          }
        ],
        services: %{
          "storage1" => %{kind: :storage, status: {:up, :storage1_pid}}
        }
      }

      state = create_test_state(transaction_system_layout: layout, read_version: 12_345)

      # Mock storage function that always fails
      mock_storage_fn = fn _pids, {_start_key, _end_key}, _version, _batch_size, _timeout ->
        {:error, :unavailable}
      end

      opts = [storage_fetch_fn: mock_storage_fn]

      # Call do_range_batch
      {_new_state, result} = RangeFetching.do_range_batch(state, {"a", "z"}, 10, opts)

      # Should return error
      assert {:error, :unavailable} = result
    end

    test "respects limit parameter" do
      layout = %{
        sequencer: :test_sequencer,
        storage_teams: [
          %{
            key_range: {"", :end},
            storage_ids: ["storage1"]
          }
        ],
        services: %{
          "storage1" => %{kind: :storage, status: {:up, :storage1_pid}}
        }
      }

      state = create_test_state(transaction_system_layout: layout, read_version: 12_345)

      mock_storage_fn = fn _pids, {_start_key, _end_key}, _version, batch_size, _timeout ->
        # Return exactly batch_size items to simulate has_more = true
        data = Enum.map(1..batch_size, fn i -> {"key#{i}", "value#{i}"} end)
        {:ok, %{data: data, has_more: true}}
      end

      opts = [storage_fetch_fn: mock_storage_fn]

      # Call do_range_batch with small batch size
      {_new_state, result} = RangeFetching.do_range_batch(state, {"a", "z"}, 5, opts)

      # Should succeed with continuation indicating more data
      assert {:ok, data, continuation} = result
      assert length(data) == 5
      assert {:continue_from, _next_key} = continuation
    end

    test "includes tx writes when storage says no more keys but writes exist beyond batch" do
      layout = %{
        sequencer: :test_sequencer,
        storage_teams: [
          %{
            key_range: {"", :end},
            storage_ids: ["storage1"]
          }
        ],
        services: %{
          "storage1" => %{kind: :storage, status: {:up, :storage1_pid}}
        }
      }

      # Create state with some pending writes in the transaction
      state = create_test_state(transaction_system_layout: layout, read_version: 12_345)

      # Add some writes to the tx that fall within our query range but beyond the storage batch
      tx_with_writes = %{
        state.tx
        | writes:
            :gb_trees.insert(
              "m",
              "tx_value_m",
              :gb_trees.insert("n", "tx_value_n", :gb_trees.insert("o", "tx_value_o", state.tx.writes))
            )
      }

      state = %{state | tx: tx_with_writes}

      # Mock storage function that returns limited data and says no more keys
      mock_storage_fn = fn _pids, {_start_key, _end_key}, _version, _batch_size, _timeout ->
        # Storage only returns keys up to "k", then says has_more = false
        data = [
          {"a", "storage_value_a"},
          {"b", "storage_value_b"},
          {"k", "storage_value_k"}
        ]

        {:ok, %{data: data, has_more: false}}
      end

      opts = [storage_fetch_fn: mock_storage_fn]

      # Query range "a" to "z" - this should include both storage results AND tx writes
      {_new_state, result} = RangeFetching.do_range_batch(state, {"a", "z"}, 10, opts)

      # Should succeed and include ALL keys in range: storage keys + tx writes
      assert {:ok, data, :finished} = result

      # Verify complete result includes both storage and tx writes in sorted order
      assert data == [
               {"a", "storage_value_a"},
               {"b", "storage_value_b"},
               {"k", "storage_value_k"},
               {"m", "tx_value_m"},
               {"n", "tx_value_n"},
               {"o", "tx_value_o"}
             ]
    end
  end
end
