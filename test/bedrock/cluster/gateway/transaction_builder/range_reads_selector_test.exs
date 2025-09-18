defmodule Bedrock.Cluster.Gateway.TransactionBuilder.RangeReadsKeySelectorTest do
  use ExUnit.Case, async: true

  alias Bedrock.Cluster.Gateway.TransactionBuilder.LayoutIndex
  alias Bedrock.Cluster.Gateway.TransactionBuilder.RangeReads
  alias Bedrock.Cluster.Gateway.TransactionBuilder.State
  alias Bedrock.Cluster.Gateway.TransactionBuilder.Tx
  alias Bedrock.KeySelector

  defp create_test_state(opts) do
    layout =
      Keyword.get(opts, :transaction_system_layout, %{
        sequencer: :test_sequencer,
        storage_teams: [],
        services: %{}
      })

    layout_index = LayoutIndex.build_index(layout)

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

  defp default_storage_layout do
    %{
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
  end

  describe "key selector range reads with conflict tracking" do
    test "tracks conflicts on actual returned keys, not selector keys" do
      # Mock storage function that resolves selectors to actual keys
      storage_get_range_fn = fn _server, start_sel, end_sel, _read_version, _opts ->
        # Simulate key selector resolution:
        # start_sel = {key: "user", offset: 1} resolves to "user:alice"
        # end_sel = {key: "user", offset: 3} resolves to "user:charlie"
        # The actual data returned is between these resolved keys
        assert start_sel.key == "user"
        assert end_sel.key == "user"

        data = [
          {"user:alice", "alice_data"},
          {"user:bob", "bob_data"},
          {"user:charlie", "charlie_data"}
        ]

        {:ok, {data, false}}
      end

      state = create_test_state(transaction_system_layout: default_storage_layout(), read_version: 12_345)

      start_selector = %KeySelector{key: "user", offset: 1}
      end_selector = %KeySelector{key: "user", offset: 3}

      opts = [storage_get_range_fn: storage_get_range_fn]

      # Perform the range query with selectors
      assert {new_state, {:ok, {data, false}}} =
               RangeReads.get_range_selectors(state, start_selector, end_selector, 10, opts)

      assert data == [
               {"user:alice", "alice_data"},
               {"user:bob", "bob_data"},
               {"user:charlie", "charlie_data"}
             ]

      # THIS IS THE CRITICAL TEST: Verify transaction state tracks the actual key range
      # The tx should have range conflict tracking for the actual keys returned,
      # NOT the selector keys ("user", "user")
      tx_ranges = new_state.tx.range_reads

      # Should have one range read entry
      assert length(tx_ranges) == 1

      # Get the range that was actually tracked - should be based on actual returned keys, not selector keys
      assert [{"user:alice", <<"user:charlie", 0>>}] = tx_ranges

      # These assertions would FAIL with the old buggy code that used selector keys
      # This would be wrong - selector key, not actual key
      refute elem(hd(tx_ranges), 0) == "user"
      # This would be wrong - selector key, not actual key
      refute elem(hd(tx_ranges), 1) == "user"
    end

    test "tracks conflicts correctly for empty key selector results" do
      # Mock storage function that returns no results
      storage_get_range_fn = fn _server, start_sel, end_sel, _read_version, _opts ->
        assert start_sel.key == "nonexistent"
        assert end_sel.key == "nonexistent"

        # No data found
        {:ok, {[], false}}
      end

      state = create_test_state(transaction_system_layout: default_storage_layout(), read_version: 12_345)

      start_selector = %KeySelector{key: "nonexistent", offset: 0}
      end_selector = %KeySelector{key: "nonexistent", offset: 10}

      opts = [storage_get_range_fn: storage_get_range_fn]

      # Perform the range query with selectors
      assert {new_state, {:ok, {[], false}}} =
               RangeReads.get_range_selectors(state, start_selector, end_selector, 10, opts)

      # For empty results, no range conflict is tracked because start == end
      # (The add_range_conflict function filters out point ranges)
      assert [] == new_state.tx.range_reads
    end

    test "handles partial results correctly" do
      # Mock storage that returns partial results (with has_more = true)
      storage_get_range_fn = fn _server, start_sel, end_sel, _read_version, opts ->
        _limit = Keyword.get(opts, :limit, 10)
        assert start_sel.key == "items"
        assert end_sel.key == "items"

        # Return only first 2 items, indicate more available
        data = [
          {"items:001", "item_001_data"},
          {"items:002", "item_002_data"}
        ]

        # has_more = true
        {:ok, {data, true}}
      end

      state = create_test_state(transaction_system_layout: default_storage_layout(), read_version: 12_345)

      start_selector = %KeySelector{key: "items", offset: 0}
      end_selector = %KeySelector{key: "items", offset: 100}

      opts = [storage_get_range_fn: storage_get_range_fn]

      # Query with small limit
      assert {new_state, {:ok, {data, true}}} =
               RangeReads.get_range_selectors(state, start_selector, end_selector, 2, opts)

      assert data == [
               {"items:001", "item_001_data"},
               {"items:002", "item_002_data"}
             ]

      # Conflict tracking should cover the actual keys returned
      assert [{"items:001", <<"items:002", 0>>}] = new_state.tx.range_reads
    end

    test "properly merges with existing transaction writes" do
      # This test ensures the actual key range is used for merging with writes
      storage_get_range_fn = fn _server, start_sel, end_sel, _read_version, _opts ->
        assert start_sel.key == "data"
        assert end_sel.key == "data"

        # Storage returns some keys
        data = [
          {"data:10", "storage_val_10"},
          {"data:30", "storage_val_30"}
        ]

        {:ok, {data, false}}
      end

      state = create_test_state(transaction_system_layout: default_storage_layout(), read_version: 12_345)

      # Add some writes to the transaction that should be included in results
      tx_with_writes = %{
        state.tx
        | writes: :gb_trees.insert("data:20", "tx_val_20", :gb_trees.insert("data:25", "tx_val_25", state.tx.writes))
      }

      state = %{state | tx: tx_with_writes}

      start_selector = %KeySelector{key: "data", offset: 0}
      end_selector = %KeySelector{key: "data", offset: 100}

      opts = [storage_get_range_fn: storage_get_range_fn]

      assert {new_state, {:ok, {data, false}}} =
               RangeReads.get_range_selectors(state, start_selector, end_selector, 10, opts)

      # Should get merged results: storage + transaction writes
      assert data == [
               # from storage
               {"data:10", "storage_val_10"},
               # from tx writes
               {"data:20", "tx_val_20"},
               # from tx writes
               {"data:25", "tx_val_25"},
               # from storage
               {"data:30", "storage_val_30"}
             ]

      # The range tracked should be based on the ACTUAL returned keys, including writes
      assert [{"data:10", <<"data:30", 0>>}] = new_state.tx.range_reads
    end
  end
end
