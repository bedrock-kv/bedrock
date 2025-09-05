defmodule Bedrock.Cluster.Gateway.TransactionBuilder.RangeReadsTest do
  use ExUnit.Case, async: true

  alias Bedrock.Cluster.Gateway.TransactionBuilder.LayoutUtils
  alias Bedrock.Cluster.Gateway.TransactionBuilder.RangeReads
  alias Bedrock.Cluster.Gateway.TransactionBuilder.State
  alias Bedrock.Cluster.Gateway.TransactionBuilder.Tx

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

  describe "batch range fetching with fetch_range/4" do
    test "returns batch results from single storage team" do
      # Create mock async_stream function that simulates storage racing
      async_stream_fn = fn servers, task_fn, _opts ->
        servers
        |> Enum.map(task_fn)
        |> Enum.map(&{:ok, &1})
      end

      # Mock storage function using dependency injection
      storage_range_fetch_fn = fn _server, _min_key, _max_key, _read_version, _opts ->
        # Return test data in the expected format
        data = [
          {"a1", "value1"},
          {"a2", "value2"}
        ]

        {:ok, {data, false}}
      end

      # Create state with mock storage configuration
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
      opts = [async_stream_fn: async_stream_fn, storage_range_fetch_fn: storage_range_fetch_fn]

      # Call fetch_range
      {_new_state, result} = RangeReads.fetch_range(state, {"a", "z"}, 10, opts)

      # Should succeed with tuple format
      assert {:ok, {data, has_more}} = result
      assert data == [{"a1", "value1"}, {"a2", "value2"}]
      assert has_more == false
    end

    test "handles single storage team correctly" do
      async_stream_fn = fn servers, task_fn, _opts ->
        servers
        |> Enum.map(task_fn)
        |> Enum.map(&{:ok, &1})
      end

      storage_range_fetch_fn = fn _server, _min_key, _max_key, _read_version, _opts ->
        data = [{"key1", "value1"}, {"key2", "value2"}]
        {:ok, {data, false}}
      end

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
      opts = [async_stream_fn: async_stream_fn, storage_range_fetch_fn: storage_range_fetch_fn]

      {_new_state, result} = RangeReads.fetch_range(state, {"a", "z"}, 10, opts)

      assert {:ok, {data, has_more}} = result
      assert data == [{"key1", "value1"}, {"key2", "value2"}]
      assert has_more == false
    end

    test "handles storage failures gracefully" do
      async_stream_fn = fn servers, task_fn, _opts ->
        servers
        |> Enum.map(task_fn)
        |> Enum.map(&{:ok, &1})
      end

      storage_range_fetch_fn = fn _server, _min_key, _max_key, _read_version, _opts ->
        {:error, :unavailable}
      end

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
      opts = [async_stream_fn: async_stream_fn, storage_range_fetch_fn: storage_range_fetch_fn]

      {_new_state, result} = RangeReads.fetch_range(state, {"a", "z"}, 10, opts)

      # Should return error
      assert {:error, :unavailable} = result
    end

    test "respects limit parameter" do
      async_stream_fn = fn servers, task_fn, _opts ->
        servers
        |> Enum.map(task_fn)
        |> Enum.map(&{:ok, &1})
      end

      storage_range_fetch_fn = fn _server, _min_key, _max_key, _read_version, opts ->
        batch_size = Keyword.get(opts, :limit, 10)
        # Return exactly batch_size items to simulate has_more = true
        data = Enum.map(1..batch_size, fn i -> {"key#{i}", "value#{i}"} end)
        {:ok, {data, true}}
      end

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
      opts = [async_stream_fn: async_stream_fn, storage_range_fetch_fn: storage_range_fetch_fn]

      # Call with small batch size
      {_new_state, result} = RangeReads.fetch_range(state, {"a", "z"}, 5, opts)

      # Should succeed with has_more indicating more data
      assert {:ok, {data, has_more}} = result
      assert length(data) == 5
      assert has_more == true
    end

    test "includes tx writes when storage says no more keys but writes exist beyond batch" do
      async_stream_fn = fn servers, task_fn, _opts ->
        servers
        |> Enum.map(task_fn)
        |> Enum.map(&{:ok, &1})
      end

      storage_range_fetch_fn = fn _server, _min_key, _max_key, _read_version, _opts ->
        # Storage only returns keys up to "k", then says has_more = false
        data = [
          {"a", "storage_value_a"},
          {"b", "storage_value_b"},
          {"k", "storage_value_k"}
        ]

        {:ok, {data, false}}
      end

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
      opts = [async_stream_fn: async_stream_fn, storage_range_fetch_fn: storage_range_fetch_fn]

      # Query range "a" to "z" - this should include both storage results AND tx writes
      {_new_state, result} = RangeReads.fetch_range(state, {"a", "z"}, 10, opts)

      # Should succeed and include ALL keys in range: storage keys + tx writes
      assert {:ok, {data, has_more}} = result
      assert has_more == false

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
