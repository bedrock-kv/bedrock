defmodule Bedrock.Cluster.Gateway.TransactionBuilder.TxTest do
  use ExUnit.Case, async: true

  alias Bedrock.Cluster.Gateway.TransactionBuilder.Tx
  alias Bedrock.DataPlane.Transaction

  # Helper function for testing - converts gb_trees writes back to map format
  defp writes_to_map(%Tx{writes: writes}) do
    writes |> :gb_trees.to_list() |> Map.new()
  end

  # Helper function for testing - converts entire Tx to map format for easy comparison
  defp to_test_map(%Tx{} = tx) do
    %{
      mutations: tx.mutations,
      writes: writes_to_map(tx),
      reads: tx.reads,
      range_writes: tx.range_writes,
      range_reads: tx.range_reads
    }
  end

  describe "new/0" do
    test "creates empty transaction" do
      tx = Tx.new()

      assert to_test_map(tx) == %{
               mutations: [],
               writes: %{},
               reads: %{},
               range_writes: [],
               range_reads: []
             }

      assert %{
               mutations: [],
               write_conflicts: [],
               read_conflicts: {nil, []}
             } = tx |> Tx.commit(nil) |> then(&elem(Transaction.decode(&1), 1))
    end
  end

  describe "set/3" do
    test "sets key-value pair" do
      tx = Tx.set(Tx.new(), "key1", "value1")

      assert to_test_map(tx) == %{
               mutations: [{:set, "key1", "value1"}],
               writes: %{"key1" => "value1"},
               reads: %{},
               range_writes: [{"key1", "key1\0"}],
               range_reads: []
             }

      assert %{
               mutations: [{:set, "key1", "value1"}],
               write_conflicts: [{"key1", "key1\0"}],
               read_conflicts: {nil, []}
             } = tx |> Tx.commit(nil) |> then(&elem(Transaction.decode(&1), 1))
    end

    test "sets multiple key-value pairs" do
      tx =
        Tx.new()
        |> Tx.set("key1", "value1")
        |> Tx.set("key2", "value2")
        |> Tx.set("key3", "value3")

      assert to_test_map(tx) == %{
               mutations: [{:set, "key3", "value3"}, {:set, "key2", "value2"}, {:set, "key1", "value1"}],
               writes: %{"key1" => "value1", "key2" => "value2", "key3" => "value3"},
               reads: %{},
               range_writes: [{"key1", "key1\0"}, {"key2", "key2\0"}, {"key3", "key3\0"}],
               range_reads: []
             }

      assert %{
               mutations: [
                 {:set, "key1", "value1"},
                 {:set, "key2", "value2"},
                 {:set, "key3", "value3"}
               ],
               write_conflicts: write_conflicts,
               read_conflicts: {nil, []}
             } = tx |> Tx.commit(nil) |> then(&elem(Transaction.decode(&1), 1))

      assert write_conflicts == [
               {"key1", "key1\0"},
               {"key2", "key2\0"},
               {"key3", "key3\0"}
             ]
    end

    test "overwrites existing key" do
      tx =
        Tx.new()
        |> Tx.set("key1", "value1")
        |> Tx.set("key1", "updated_value")

      assert to_test_map(tx) == %{
               mutations: [{:set, "key1", "updated_value"}],
               writes: %{"key1" => "updated_value"},
               reads: %{},
               range_writes: [{"key1", "key1\0"}],
               range_reads: []
             }

      assert %{
               mutations: [
                 {:set, "key1", "updated_value"}
               ],
               write_conflicts: [{"key1", "key1\0"}],
               read_conflicts: {nil, []}
             } = tx |> Tx.commit(nil) |> then(&elem(Transaction.decode(&1), 1))
    end

    test "handles empty string key and value" do
      tx = Tx.set(Tx.new(), "", "")

      assert %{
               mutations: [{:set, "", ""}],
               write_conflicts: [{"", "\0"}],
               read_conflicts: {nil, []}
             } = tx |> Tx.commit(nil) |> then(&elem(Transaction.decode(&1), 1))
    end

    test "handles unicode keys and values" do
      tx = Tx.set(Tx.new(), "键名", "值")

      assert %{
               mutations: [{:set, "键名", "值"}],
               write_conflicts: [{"键名", "键名\0"}],
               read_conflicts: {nil, []}
             } = tx |> Tx.commit(nil) |> then(&elem(Transaction.decode(&1), 1))
    end

    test "handles binary data with null bytes" do
      binary_key = "\x00\x01\xFF\x02"
      binary_value = "\xFF\x00\x01\x02"

      tx = Tx.set(Tx.new(), binary_key, binary_value)

      expected_end_key = binary_key <> "\0"

      assert %{
               mutations: [{:set, ^binary_key, ^binary_value}],
               write_conflicts: [{^binary_key, ^expected_end_key}],
               read_conflicts: {nil, []}
             } = tx |> Tx.commit(nil) |> then(&elem(Transaction.decode(&1), 1))
    end
  end

  describe "clear/2" do
    test "clears single key" do
      tx =
        Tx.new()
        |> Tx.set("key1", "value1")
        |> Tx.clear("key2")

      assert to_test_map(tx) == %{
               mutations: [{:clear, "key2"}, {:set, "key1", "value1"}],
               writes: %{"key1" => "value1", "key2" => :clear},
               reads: %{},
               range_writes: [{"key1", "key1\0"}, {"key2", "key2\0"}],
               range_reads: []
             }

      assert %{
               mutations: [
                 {:set, "key1", "value1"},
                 {:clear, "key2"}
               ],
               write_conflicts: [
                 {"key1", "key1\0"},
                 {"key2", "key2\0"}
               ],
               read_conflicts: {nil, []}
             } = tx |> Tx.commit(nil) |> then(&elem(Transaction.decode(&1), 1))
    end

    test "clear overwrites existing key" do
      tx =
        Tx.new()
        |> Tx.set("key1", "value1")
        |> Tx.clear("key1")

      assert to_test_map(tx) == %{
               mutations: [{:clear, "key1"}],
               writes: %{"key1" => :clear},
               reads: %{},
               range_writes: [{"key1", "key1\0"}],
               range_reads: []
             }

      assert %{
               mutations: [
                 {:clear, "key1"}
               ],
               write_conflicts: [{"key1", "key1\0"}],
               read_conflicts: {nil, []}
             } = tx |> Tx.commit(nil) |> then(&elem(Transaction.decode(&1), 1))
    end
  end

  describe "clear_range/3" do
    test "clears range of keys" do
      tx = Tx.clear_range(Tx.new(), "a", "z")

      assert to_test_map(tx) == %{
               mutations: [{:clear_range, "a", "z"}],
               writes: %{},
               reads: %{},
               range_writes: [{"a", "z"}],
               range_reads: []
             }

      assert %{
               mutations: [{:clear_range, "a", "z"}],
               write_conflicts: [{"a", "z"}],
               read_conflicts: {nil, []}
             } = tx |> Tx.commit(nil) |> then(&elem(Transaction.decode(&1), 1))
    end

    test "clears range removes individual ops in range" do
      tx =
        Tx.new()
        |> Tx.set("apple", "fruit")
        |> Tx.set("zebra", "animal")
        |> Tx.set("banana", "fruit")
        |> Tx.clear_range("a", "m")

      assert %{
               mutations: [
                 {:set, "zebra", "animal"},
                 {:clear_range, "a", "m"}
               ],
               write_conflicts: write_conflicts,
               read_conflicts: {nil, []}
             } = tx |> Tx.commit(nil) |> then(&elem(Transaction.decode(&1), 1))

      assert write_conflicts == [
               {"a", "m"},
               {"zebra", "zebra\0"}
             ]
    end
  end

  describe "get/4" do
    test "gets value from writes cache" do
      tx = Tx.set(Tx.new(), "cached_key", "cached_value")

      fetch_fn = fn _key, state ->
        flunk("Should not call fetch_fn when value is in writes cache")
        {{:ok, "should_not_reach"}, state}
      end

      {new_tx, result, state} = Tx.get(tx, "cached_key", fetch_fn, :test_state)

      assert result == {:ok, "cached_value"}
      assert state == :test_state

      assert to_test_map(new_tx) == %{
               mutations: [{:set, "cached_key", "cached_value"}],
               writes: %{"cached_key" => "cached_value"},
               reads: %{},
               range_writes: [{"cached_key", "cached_key\0"}],
               range_reads: []
             }

      assert new_tx == tx
    end

    test "gets value from reads cache when not in writes" do
      tx = then(Tx.new(), &%{&1 | reads: %{"cached_key" => "cached_value"}})

      fetch_fn = fn _key, state ->
        flunk("Should not call fetch_fn when value is in reads cache")
        {{:ok, "should_not_reach"}, state}
      end

      {new_tx, result, state} = Tx.get(tx, "cached_key", fetch_fn, :test_state)

      assert result == {:ok, "cached_value"}
      assert new_tx == tx
      assert state == :test_state
    end

    test "fetches from storage when not in cache" do
      tx = Tx.new()

      fetch_fn = fn key, state ->
        assert key == "missing_key"
        assert state == :test_state
        {{:ok, "fetched_value"}, :new_state}
      end

      {new_tx, result, new_state} = Tx.get(tx, "missing_key", fetch_fn, :test_state)

      assert result == {:ok, "fetched_value"}
      assert new_state == :new_state
      assert new_tx.reads == %{"missing_key" => "fetched_value"}
    end

    test "handles fetch error" do
      tx = Tx.new()

      fetch_fn = fn _key, state ->
        {{:error, :not_found}, state}
      end

      {new_tx, result, state} = Tx.get(tx, "missing_key", fetch_fn, :test_state)

      assert result == {:error, :not_found}
      assert state == :test_state
      assert new_tx.reads == %{"missing_key" => :clear}
    end

    test "handles cleared key in writes" do
      tx = Tx.clear(Tx.new(), "cleared_key")

      fetch_fn = fn _key, _state ->
        flunk("Should not call fetch_fn for cleared key")
      end

      {new_tx, result, state} = Tx.get(tx, "cleared_key", fetch_fn, :test_state)

      assert result == {:error, :not_found}
      assert new_tx == tx
      assert state == :test_state
    end

    test "handles cleared key in reads" do
      tx = then(Tx.new(), &%{&1 | reads: %{"cleared_key" => :clear}})

      fetch_fn = fn _key, _state ->
        flunk("Should not call fetch_fn for cleared key in reads")
      end

      {new_tx, result, state} = Tx.get(tx, "cleared_key", fetch_fn, :test_state)

      assert result == {:error, :not_found}
      assert new_tx == tx
      assert state == :test_state
    end

    test "writes cache takes precedence over reads cache" do
      tx =
        Tx.new()
        |> then(&%{&1 | reads: %{"key" => "old_value"}})
        |> Tx.set("key", "new_value")

      fetch_fn = fn _key, _state ->
        flunk("Should not fetch when value is in writes cache")
      end

      {new_tx, result, state} = Tx.get(tx, "key", fetch_fn, :test_state)

      assert result == {:ok, "new_value"}
      assert new_tx == tx
      assert state == :test_state
    end
  end

  # get_range/6 functionality has been moved to client-side streaming

  describe "commit/1" do
    test "commits empty transaction" do
      tx = Tx.new()

      assert %{
               mutations: [],
               write_conflicts: [],
               read_conflicts: {nil, []}
             } = tx |> Tx.commit(nil) |> then(&elem(Transaction.decode(&1), 1))
    end

    test "commits transaction with only writes" do
      tx =
        Tx.new()
        |> Tx.set("key1", "value1")
        |> Tx.set("key2", "value2")

      assert %{
               mutations: [
                 {:set, "key1", "value1"},
                 {:set, "key2", "value2"}
               ],
               write_conflicts: [
                 {"key1", "key1\0"},
                 {"key2", "key2\0"}
               ],
               read_conflicts: {nil, []}
             } = tx |> Tx.commit(nil) |> then(&elem(Transaction.decode(&1), 1))
    end

    test "commits transaction with reads and writes" do
      tx =
        Tx.new()
        |> Tx.set("key1", "value1")
        |> then(&%{&1 | reads: %{"read_key" => "read_value"}})

      read_version = Bedrock.DataPlane.Version.from_integer(123)

      assert %{
               mutations: [{:set, "key1", "value1"}],
               write_conflicts: [{"key1", "key1\0"}],
               read_conflicts: {^read_version, [{"read_key", "read_key\0"}]}
             } = tx |> Tx.commit(read_version) |> then(&elem(Transaction.decode(&1), 1))
    end

    test "commits complex transaction with multiple operations" do
      tx =
        Tx.new()
        |> Tx.set("key1", "value1")
        |> Tx.clear("key2")
        |> Tx.set("key3", "value3")
        |> then(&%{&1 | reads: %{"read_key" => "read_value"}})

      read_version = Bedrock.DataPlane.Version.from_integer(456)

      assert %{
               mutations: [
                 {:set, "key1", "value1"},
                 {:clear, "key2"},
                 {:set, "key3", "value3"}
               ],
               write_conflicts: [
                 {"key1", "key1\0"},
                 {"key2", "key2\0"},
                 {"key3", "key3\0"}
               ],
               read_conflicts: {^read_version, [{"read_key", "read_key\0"}]}
             } = tx |> Tx.commit(read_version) |> then(&elem(Transaction.decode(&1), 1))
    end

    test "coalesces overlapping ranges in conflicts" do
      tx = then(Tx.new(), &%{&1 | range_reads: [{"a", "m"}, {"k", "z"}, {"b", "n"}]})

      read_version = Bedrock.DataPlane.Version.from_integer(789)

      assert %{
               mutations: [],
               write_conflicts: [],
               read_conflicts: {^read_version, [{"a", "m"}, {"k", "z"}, {"b", "n"}]}
             } = tx |> Tx.commit(read_version) |> then(&elem(Transaction.decode(&1), 1))
    end
  end

  describe "edge cases and error handling" do
    test "handles large keys and values" do
      large_key = String.duplicate("k", 1000)
      large_value = String.duplicate("v", 10_000)

      tx = Tx.set(Tx.new(), large_key, large_value)

      assert %{
               mutations: [{:set, ^large_key, ^large_value}],
               write_conflicts: write_conflicts,
               read_conflicts: {nil, []}
             } = tx |> Tx.commit(nil) |> then(&elem(Transaction.decode(&1), 1))

      expected_end_key = large_key <> "\0"
      assert write_conflicts == [{large_key, expected_end_key}]
    end

    test "handles mixed operations" do
      large_key = "large_key"
      large_value = "large_value"

      tx =
        Tx.new()
        |> Tx.set(large_key, large_value)
        |> Tx.clear("clear_key")
        |> Tx.clear_range("a", "b")

      assert %{
               mutations: [
                 {:set, ^large_key, ^large_value},
                 {:clear, "clear_key"},
                 {:clear_range, "a", "b"}
               ],
               write_conflicts: write_conflicts,
               read_conflicts: {nil, []}
             } = tx |> Tx.commit(nil) |> then(&elem(Transaction.decode(&1), 1))

      expected_large_end_key = large_key <> "\0"

      assert write_conflicts == [
               {"a", "b"},
               {"clear_key", "clear_key\0"},
               {large_key, expected_large_end_key}
             ]
    end

    test "handles key collision between set and clear" do
      tx =
        Tx.new()
        |> Tx.set("collision_key", "value")
        |> Tx.clear("collision_key")

      assert writes_to_map(tx)["collision_key"] == :clear

      assert %{
               mutations: [
                 {:clear, "collision_key"}
               ],
               write_conflicts: [{"collision_key", "collision_key\0"}],
               read_conflicts: {nil, []}
             } = tx |> Tx.commit(nil) |> then(&elem(Transaction.decode(&1), 1))
    end
  end

  describe "clear_range edge cases" do
    test "clear_range with empty range does nothing" do
      tx =
        Tx.new()
        |> Tx.set("key", "value")
        # empty range
        |> Tx.clear_range("m", "m")

      assert to_test_map(tx) == %{
               mutations: [{:set, "key", "value"}],
               writes: %{"key" => "value"},
               reads: %{},
               range_writes: [{"key", "key\0"}],
               range_reads: []
             }
    end

    test "clear_range at range boundaries" do
      tx =
        Tx.new()
        |> Tx.set("a", "val_a")
        # on boundary
        |> Tx.set("b", "val_b")
        |> Tx.set("c", "val_c")
        # includes "b", excludes "c"
        |> Tx.clear_range("b", "c")

      assert to_test_map(tx) == %{
               mutations: [{:clear_range, "b", "c"}, {:set, "c", "val_c"}, {:set, "a", "val_a"}],
               writes: %{"a" => "val_a", "c" => "val_c"},
               reads: %{},
               range_writes: [{"a", "a\0"}, {"b", "c\0"}],
               range_reads: []
             }
    end

    test "clear_range removes exact key matches" do
      tx =
        Tx.new()
        |> Tx.set("exact_match", "value")
        # clears exactly this key
        |> Tx.clear_range("exact_match", "exact_match\0")

      assert to_test_map(tx) == %{
               mutations: [{:clear_range, "exact_match", "exact_match\0"}],
               writes: %{},
               reads: %{},
               range_writes: [{"exact_match", "exact_match\0"}],
               range_reads: []
             }
    end

    test "clear_range with overlapping ranges merges them" do
      tx =
        Tx.new()
        |> Tx.clear_range("a", "f")
        # overlaps with first range
        |> Tx.clear_range("d", "j")

      assert to_test_map(tx) == %{
               mutations: [{:clear_range, "d", "j"}, {:clear_range, "a", "f"}],
               writes: %{},
               reads: %{},
               # merged
               range_writes: [{"a", "j"}],
               range_reads: []
             }
    end

    test "clear_range with adjacent ranges merges them" do
      tx =
        Tx.new()
        |> Tx.clear_range("a", "f")
        # adjacent: end of first = start of second
        |> Tx.clear_range("f", "j")

      assert to_test_map(tx) == %{
               mutations: [{:clear_range, "f", "j"}, {:clear_range, "a", "f"}],
               writes: %{},
               reads: %{},
               # merged
               range_writes: [{"a", "j"}],
               range_reads: []
             }
    end

    test "clear_range with non-overlapping ranges keeps them separate" do
      tx =
        Tx.new()
        |> Tx.clear_range("a", "c")
        # gap between ranges
        |> Tx.clear_range("f", "j")

      assert to_test_map(tx) == %{
               mutations: [{:clear_range, "f", "j"}, {:clear_range, "a", "c"}],
               writes: %{},
               reads: %{},
               # kept separate
               range_writes: [{"a", "c"}, {"f", "j"}],
               range_reads: []
             }
    end

    test "clear_range clears reads in range" do
      tx =
        Tx.new()
        |> then(&%{&1 | reads: %{"inside" => "value", "outside" => "value"}})
        # "inside" falls in range, "outside" doesn't
        |> Tx.clear_range("h", "k")

      assert to_test_map(tx) == %{
               mutations: [{:clear_range, "h", "k"}],
               writes: %{},
               # "inside" cleared
               reads: %{"inside" => :clear, "outside" => "value"},
               range_writes: [{"h", "k"}],
               range_reads: []
             }
    end

    test "clear_range removes writes by key iteration from gb_trees" do
      tx =
        Tx.new()
        |> Tx.set("apple", "fruit")
        |> Tx.set("avocado", "fruit")
        |> Tx.set("banana", "fruit")
        |> Tx.set("cherry", "fruit")
        # should remove "banana", "cherry"
        |> Tx.clear_range("b", "d")

      assert to_test_map(tx) == %{
               mutations: [
                 {:clear_range, "b", "d"},
                 {:set, "avocado", "fruit"},
                 {:set, "apple", "fruit"}
               ],
               # "banana", "cherry" removed
               writes: %{"apple" => "fruit", "avocado" => "fruit"},
               reads: %{},
               range_writes: [{"apple", "apple\0"}, {"avocado", "avocado\0"}, {"b", "d"}],
               range_reads: []
             }
    end

    test "clear_range with unicode keys" do
      tx =
        Tx.new()
        |> Tx.set("α", "alpha")
        |> Tx.set("β", "beta")
        |> Tx.set("γ", "gamma")
        # clears β but not γ
        |> Tx.clear_range("β", "γ")

      # "β" should be cleared but "α" and "γ" should remain
      expected_writes = %{"α" => "alpha", "γ" => "gamma"}
      assert writes_to_map(tx) == expected_writes

      assert to_test_map(tx) == %{
               mutations: [{:clear_range, "β", "γ"}, {:set, "γ", "gamma"}, {:set, "α", "alpha"}],
               writes: expected_writes,
               reads: %{},
               range_writes: [{"α", "α\0"}, {"β", "γ\0"}],
               range_reads: []
             }
    end

    test "clear_range with binary keys containing null bytes" do
      key_in_range = "\x00\x05"
      key_out_range = "\x00\x10"

      tx =
        Tx.new()
        |> Tx.set(key_in_range, "in_range")
        |> Tx.set(key_out_range, "out_range")
        # clears key_in_range
        |> Tx.clear_range("\x00\x00", "\x00\x08")

      expected_writes = %{key_out_range => "out_range"}
      assert writes_to_map(tx) == expected_writes
    end

    test "clear_range preserves mutations outside range in correct order" do
      tx =
        Tx.new()
        # before range
        |> Tx.set("alpha", "1")
        # in range - should be removed
        |> Tx.set("beta", "2")
        # in range - should be removed
        |> Tx.set("gamma", "3")
        # after range
        |> Tx.set("zeta", "4")
        # clear before range
        |> Tx.clear("alpha")
        # removes beta, gamma mutations
        |> Tx.clear_range("b", "h")

      assert %{
               mutations: [
                 {:set, "zeta", "4"},
                 {:clear, "alpha"},
                 {:clear_range, "b", "h"}
               ],
               write_conflicts: write_conflicts,
               read_conflicts: {nil, []}
             } = tx |> Tx.commit(nil) |> then(&elem(Transaction.decode(&1), 1))

      # Verify conflicts are in lexicographic order
      assert write_conflicts == [
               {"alpha", "alpha\0"},
               {"b", "h"},
               {"zeta", "zeta\0"}
             ]
    end
  end

  describe "conflict range helper functions" do
    test "add_read_conflict_range/3 adds a single read conflict range" do
      tx = Tx.add_read_conflict_range(Tx.new(), "a", "z")

      assert tx.range_reads == [{"a", "z"}]

      # Should not affect writes or other fields
      assert to_test_map(tx) == %{
               mutations: [],
               writes: %{},
               reads: %{},
               range_writes: [],
               range_reads: [{"a", "z"}]
             }
    end

    test "add_read_conflict_range/3 merges overlapping read conflict ranges" do
      tx =
        Tx.new()
        |> Tx.add_read_conflict_range("a", "m")
        |> Tx.add_read_conflict_range("k", "z")

      # Should be merged into single range
      assert tx.range_reads == [{"a", "z"}]
    end

    test "add_read_conflict_range/3 merges adjacent read conflict ranges" do
      tx =
        Tx.new()
        |> Tx.add_read_conflict_range("a", "f")
        |> Tx.add_read_conflict_range("f", "j")

      # Should be merged into single range
      assert tx.range_reads == [{"a", "j"}]
    end

    test "add_read_conflict_range/3 keeps non-overlapping read conflict ranges separate" do
      tx =
        Tx.new()
        |> Tx.add_read_conflict_range("a", "c")
        |> Tx.add_read_conflict_range("f", "j")

      # Should keep separate
      assert tx.range_reads == [{"a", "c"}, {"f", "j"}]
    end

    test "add_read_conflict_range/3 handles empty ranges" do
      tx = Tx.add_read_conflict_range(Tx.new(), "m", "m")

      assert tx.range_reads == [{"m", "m"}]
    end

    test "add_write_conflict_range/3 adds a single write conflict range" do
      tx = Tx.add_write_conflict_range(Tx.new(), "a", "z")

      assert tx.range_writes == [{"a", "z"}]

      # Should not affect reads or other fields
      assert to_test_map(tx) == %{
               mutations: [],
               writes: %{},
               reads: %{},
               range_writes: [{"a", "z"}],
               range_reads: []
             }
    end

    test "add_write_conflict_range/3 merges overlapping write conflict ranges" do
      tx =
        Tx.new()
        |> Tx.add_write_conflict_range("a", "m")
        |> Tx.add_write_conflict_range("k", "z")

      # Should be merged into single range
      assert tx.range_writes == [{"a", "z"}]
    end

    test "add_write_conflict_range/3 merges adjacent write conflict ranges" do
      tx =
        Tx.new()
        |> Tx.add_write_conflict_range("a", "f")
        |> Tx.add_write_conflict_range("f", "j")

      # Should be merged into single range
      assert tx.range_writes == [{"a", "j"}]
    end

    test "add_write_conflict_range/3 keeps non-overlapping write conflict ranges separate" do
      tx =
        Tx.new()
        |> Tx.add_write_conflict_range("a", "c")
        |> Tx.add_write_conflict_range("f", "j")

      # Should keep separate
      assert tx.range_writes == [{"a", "c"}, {"f", "j"}]
    end

    test "add_read_conflict_key/2 adds read conflict for a single key" do
      tx = Tx.add_read_conflict_key(Tx.new(), "my_key")

      # Should convert to range from key to next_key(key)
      assert tx.range_reads == [{"my_key", "my_key\0"}]
    end

    test "add_read_conflict_key/2 merges with existing read conflict ranges" do
      tx =
        Tx.new()
        |> Tx.add_read_conflict_range("a", "z")
        |> Tx.add_read_conflict_key("m")

      # Key "m" should be merged into existing range
      assert tx.range_reads == [{"a", "z"}]
    end

    test "add_read_conflict_key/2 handles empty string key" do
      tx = Tx.add_read_conflict_key(Tx.new(), "")

      assert tx.range_reads == [{"", "\0"}]
    end

    test "add_read_conflict_key/2 handles binary keys with null bytes" do
      binary_key = "\x00\x01\xFF"
      tx = Tx.add_read_conflict_key(Tx.new(), binary_key)

      expected_end = binary_key <> "\0"
      assert tx.range_reads == [{binary_key, expected_end}]
    end

    test "add_write_conflict_key/2 adds write conflict for a single key" do
      tx = Tx.add_write_conflict_key(Tx.new(), "my_key")

      # Should convert to range from key to next_key(key)
      assert tx.range_writes == [{"my_key", "my_key\0"}]
    end

    test "add_write_conflict_key/2 merges with existing write conflict ranges" do
      tx =
        Tx.new()
        |> Tx.add_write_conflict_range("a", "z")
        |> Tx.add_write_conflict_key("m")

      # Key "m" should be merged into existing range
      assert tx.range_writes == [{"a", "z"}]
    end

    test "add_write_conflict_key/2 handles empty string key" do
      tx = Tx.add_write_conflict_key(Tx.new(), "")

      assert tx.range_writes == [{"", "\0"}]
    end

    test "add_write_conflict_key/2 handles binary keys with null bytes" do
      binary_key = "\x00\x01\xFF"
      tx = Tx.add_write_conflict_key(Tx.new(), binary_key)

      expected_end = binary_key <> "\0"
      assert tx.range_writes == [{binary_key, expected_end}]
    end

    test "conflict helper ranges are included in commit output" do
      tx =
        Tx.new()
        |> Tx.add_read_conflict_range("read_start", "read_end")
        |> Tx.add_write_conflict_range("write_end", "write_start")

      read_version = Bedrock.DataPlane.Version.from_integer(123)

      assert %{
               mutations: [],
               write_conflicts: [{"write_end", "write_start"}],
               read_conflicts: {^read_version, [{"read_start", "read_end"}]}
             } = tx |> Tx.commit(read_version) |> then(&elem(Transaction.decode(&1), 1))
    end

    test "conflict helpers work with normal operations" do
      tx =
        Tx.new()
        |> Tx.set("key1", "value1")
        |> then(&%{&1 | reads: %{"read_key" => "read_value"}})
        |> Tx.add_read_conflict_range("extra_read_start", "extra_read_end")
        |> Tx.add_write_conflict_range("extra_write_a", "extra_write_z")

      read_version = Bedrock.DataPlane.Version.from_integer(456)

      assert %{
               mutations: [{:set, "key1", "value1"}],
               write_conflicts: write_conflicts,
               read_conflicts: {^read_version, read_conflicts}
             } = tx |> Tx.commit(read_version) |> then(&elem(Transaction.decode(&1), 1))

      # Should include both normal operation conflicts and helper conflicts
      assert {"key1", "key1\0"} in write_conflicts
      assert {"extra_write_a", "extra_write_z"} in write_conflicts

      assert {"read_key", "read_key\0"} in read_conflicts
      assert {"extra_read_start", "extra_read_end"} in read_conflicts
    end

    test "all helper functions preserve transaction immutability" do
      original_tx = Tx.new()

      # Each operation should return a new transaction
      tx1 = Tx.add_read_conflict_range(original_tx, "a", "m")
      tx2 = Tx.add_write_conflict_range(tx1, "n", "z")
      # Before "a"
      tx3 = Tx.add_read_conflict_key(tx2, "0")
      # After "z"
      tx4 = Tx.add_write_conflict_key(tx3, "~")

      # Original should be unchanged
      assert original_tx.range_reads == []
      assert original_tx.range_writes == []

      # Each step should be different
      assert tx1 != original_tx
      assert tx2 != tx1
      assert tx3 != tx2
      assert tx4 != tx3

      # Final transaction should have accumulated specific conflicts
      assert tx4.range_writes == [{"n", "z"}, {"~", "~\0"}]
      assert tx4.range_reads == [{"0", "0\0"}, {"a", "m"}]
    end

    test "helper functions work with unicode and binary data" do
      unicode_start = "αβγ"
      unicode_end = "ωψχ"
      binary_key = "\x00\x01\xFF\x02"

      tx =
        Tx.new()
        |> Tx.add_read_conflict_range(unicode_start, unicode_end)
        |> Tx.add_write_conflict_key(binary_key)

      assert tx.range_reads == [{unicode_start, unicode_end}]
      assert tx.range_writes == [{binary_key, binary_key <> "\0"}]
    end
  end

  describe "atomic operations with subsequent reads" do
    test "repeatable_read with add should compute local value" do
      # Create a transaction with existing data in reads (simulating storage read)
      # Use little-endian format: 10 as 1-byte
      tx =
        Tx.new()
        |> Tx.merge_storage_read("counter", <<10>>)
        |> Tx.atomic_operation("counter", :add, <<5>>)

      # When we read the key after add, repeatable_read should compute the value
      result = Tx.repeatable_read(tx, "counter")

      # Should return the computed value (10 + 5 = 15) as little-endian binary
      assert result == <<15>>
    end

    test "repeatable_read with add to non-existent key should default to empty" do
      tx = Tx.atomic_operation(Tx.new(), "counter", :add, <<5>>)

      # When we read a non-existent key after add, it should be treated as empty (returns operand)
      result = Tx.repeatable_read(tx, "counter")

      # Should return the operand value since existing is empty
      assert result == <<5>>
    end

    test "repeatable_read with min should compute local value" do
      tx =
        Tx.new()
        |> Tx.merge_storage_read("min_val", <<10>>)
        |> Tx.atomic_operation("min_val", :min, <<5>>)

      result = Tx.repeatable_read(tx, "min_val")

      # Should return min(10, 5) = 5 as little-endian binary
      assert result == <<5>>
    end

    test "repeatable_read with max should compute local value" do
      tx =
        Tx.new()
        |> Tx.merge_storage_read("max_val", <<10>>)
        |> Tx.atomic_operation("max_val", :max, <<15>>)

      result = Tx.repeatable_read(tx, "max_val")

      # Should return max(10, 15) = 15 as little-endian binary
      assert result == <<15>>
    end
  end
end
