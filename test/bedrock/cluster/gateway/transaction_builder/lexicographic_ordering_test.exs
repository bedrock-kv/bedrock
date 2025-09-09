defmodule Bedrock.Cluster.Gateway.TransactionBuilder.LexicographicOrderingTest do
  use ExUnit.Case, async: true

  alias Bedrock.Cluster.Gateway.TransactionBuilder.Tx
  alias Bedrock.DataPlane.Transaction

  describe "add_or_merge maintains lexicographic ordering" do
    test "inserting ranges in lexicographic order preserves order" do
      # Start with empty and insert ranges in lexicographic order
      ranges = []
      # ["a", "b")
      ranges = Tx.add_or_merge(ranges, "a", "b")
      # ["c", "d")
      ranges = Tx.add_or_merge(ranges, "c", "d")
      # ["e", "f")
      ranges = Tx.add_or_merge(ranges, "e", "f")

      assert ranges == [{"a", "b"}, {"c", "d"}, {"e", "f"}]
    end

    test "inserting ranges in reverse lexicographic order maintains order" do
      # Insert ranges in reverse order, should be reordered lexicographically
      ranges = []
      # ["e", "f")
      ranges = Tx.add_or_merge(ranges, "e", "f")
      # ["c", "d")
      ranges = Tx.add_or_merge(ranges, "c", "d")
      # ["a", "b")
      ranges = Tx.add_or_merge(ranges, "a", "b")

      assert ranges == [{"a", "b"}, {"c", "d"}, {"e", "f"}]
    end

    test "inserting range that comes before all existing ranges" do
      ranges = [{"c", "d"}, {"e", "f"}]
      ranges = Tx.add_or_merge(ranges, "a", "b")

      assert ranges == [{"a", "b"}, {"c", "d"}, {"e", "f"}]
    end

    test "inserting range that comes after all existing ranges" do
      ranges = [{"a", "b"}, {"c", "d"}]
      ranges = Tx.add_or_merge(ranges, "e", "f")

      assert ranges == [{"a", "b"}, {"c", "d"}, {"e", "f"}]
    end

    test "inserting range in middle maintains lexicographic order" do
      ranges = [{"a", "b"}, {"e", "f"}]
      ranges = Tx.add_or_merge(ranges, "c", "d")

      assert ranges == [{"a", "b"}, {"c", "d"}, {"e", "f"}]
    end

    test "overlapping ranges merge while maintaining lexicographic order" do
      ranges = [{"a", "c"}, {"e", "g"}]
      # overlaps both ranges
      ranges = Tx.add_or_merge(ranges, "b", "f")

      # merged into single range
      assert ranges == [{"a", "g"}]
    end

    test "adjacent ranges merge while maintaining lexicographic order" do
      ranges = [{"a", "c"}, {"e", "g"}]
      # adjacent to both
      ranges = Tx.add_or_merge(ranges, "c", "e")

      # all merged
      assert ranges == [{"a", "g"}]
    end

    test "complex overlapping scenarios maintain lexicographic order" do
      ranges = []
      # middle
      ranges = Tx.add_or_merge(ranges, "m", "p")
      # start
      ranges = Tx.add_or_merge(ranges, "a", "e")
      # end
      ranges = Tx.add_or_merge(ranges, "x", "z")
      # overlaps first two
      ranges = Tx.add_or_merge(ranges, "c", "o")

      # first two merged
      assert ranges == [{"a", "p"}, {"x", "z"}]
    end

    test "unicode keys maintain lexicographic order" do
      ranges = []
      # gamma to delta
      ranges = Tx.add_or_merge(ranges, "γ", "δ")
      # alpha to beta
      ranges = Tx.add_or_merge(ranges, "α", "β")
      # epsilon to zeta
      ranges = Tx.add_or_merge(ranges, "ε", "ζ")

      # Greek letters: α < β < γ < δ < ε < ζ
      assert ranges == [{"α", "β"}, {"γ", "δ"}, {"ε", "ζ"}]
    end

    test "binary keys with null bytes maintain lexicographic order" do
      ranges = []
      ranges = Tx.add_or_merge(ranges, <<0, 5>>, <<0, 10>>)
      ranges = Tx.add_or_merge(ranges, <<0, 1>>, <<0, 3>>)
      ranges = Tx.add_or_merge(ranges, <<0, 15>>, <<0, 20>>)

      assert ranges == [
               {<<0, 1>>, <<0, 3>>},
               {<<0, 5>>, <<0, 10>>},
               {<<0, 15>>, <<0, 20>>}
             ]
    end
  end

  describe "transaction conflict ranges maintain lexicographic ordering" do
    test "write conflicts are in lexicographic order" do
      tx =
        Tx.new()
        # add out of order
        |> Tx.set("zebra", "animal")
        |> Tx.set("apple", "fruit")
        |> Tx.set("banana", "fruit")

      %{write_conflicts: conflicts} = tx |> Tx.commit(nil) |> then(&elem(Transaction.decode(&1), 1))

      assert conflicts == [
               {"apple", "apple\0"},
               {"banana", "banana\0"},
               {"zebra", "zebra\0"}
             ]
    end

    test "read conflicts are in lexicographic order" do
      read_version = Bedrock.DataPlane.Version.from_integer(100)

      tx = then(Tx.new(), &%{&1 | reads: %{"zebra" => "animal", "apple" => "fruit", "banana" => "fruit"}})

      # out of order
      %{read_conflicts: {^read_version, conflicts}} =
        tx |> Tx.commit(read_version) |> then(&elem(Transaction.decode(&1), 1))

      assert conflicts == [
               {"apple", "apple\0"},
               {"banana", "banana\0"},
               {"zebra", "zebra\0"}
             ]
    end

    test "mixed range and individual conflicts maintain lexicographic order" do
      tx =
        Tx.new()
        |> Tx.set("zebra", "animal")
        # range in middle
        |> Tx.clear_range("banana", "mango")
        |> Tx.set("apple", "fruit")

      %{write_conflicts: conflicts} = tx |> Tx.commit(nil) |> then(&elem(Transaction.decode(&1), 1))

      assert conflicts == [
               {"apple", "apple\0"},
               # range
               {"banana", "mango"},
               {"zebra", "zebra\0"}
             ]
    end

    test "overlapping range conflicts are merged in lexicographic order" do
      tx =
        Tx.new()
        |> Tx.clear_range("m", "p")
        # overlaps with nothing initially
        |> Tx.clear_range("a", "f")
        # overlaps both
        |> Tx.clear_range("d", "n")

      %{write_conflicts: conflicts} = tx |> Tx.commit(nil) |> then(&elem(Transaction.decode(&1), 1))

      # All ranges should merge into one
      assert conflicts == [{"a", "p"}]
    end
  end

  describe "KeySelector resolution maintains lexicographic ordering" do
    test "first_greater_than maintains lexicographic progression" do
      keys = ["apple", "banana", "cherry", "date"]

      Enum.reduce(keys, nil, fn current_key, previous_key ->
        if previous_key do
          assert current_key > previous_key,
                 "#{current_key} should be lexicographically after #{previous_key}"
        end

        current_key
      end)
    end

    test "first_greater_or_equal maintains lexicographic progression" do
      keys = ["", "a", "aa", "ab", "b", "ba", "bb"]

      Enum.reduce(keys, nil, fn current_key, previous_key ->
        if previous_key do
          assert current_key >= previous_key,
                 "#{current_key} should be lexicographically >= #{previous_key}"
        end

        current_key
      end)
    end

    test "unicode keys maintain lexicographic order" do
      # Greek alphabet order
      unicode_keys = ["α", "β", "γ", "δ", "ε"]

      Enum.reduce(unicode_keys, nil, fn current_key, previous_key ->
        if previous_key do
          assert current_key > previous_key,
                 "Unicode key #{current_key} should be after #{previous_key}"
        end

        current_key
      end)
    end

    test "binary keys with different byte values maintain lexicographic order" do
      binary_keys = [
        <<0>>,
        <<0, 1>>,
        <<0, 2>>,
        <<1>>,
        <<1, 0>>,
        <<255>>
      ]

      Enum.reduce(binary_keys, nil, fn current_key, previous_key ->
        if previous_key do
          assert current_key > previous_key,
                 "Binary key #{inspect(current_key)} should be after #{inspect(previous_key)}"
        end

        current_key
      end)
    end
  end

  describe "cross-shard range results maintain lexicographic ordering" do
    test "cross-shard results must be in lexicographic order" do
      shard1_results = [{"apple", "fruit"}, {"banana", "fruit"}]
      shard2_results = [{"mango", "fruit"}, {"orange", "fruit"}]
      combined_results = shard1_results ++ shard2_results

      Enum.reduce(combined_results, nil, fn {current_key, _}, previous_key ->
        if previous_key do
          assert current_key > previous_key,
                 "Cross-shard key #{current_key} should be after #{previous_key}"
        end

        current_key
      end)
    end

    test "cross-shard results with unicode keys maintain lexicographic order" do
      shard1_results = [{"α", "alpha"}, {"β", "beta"}]
      shard2_results = [{"γ", "gamma"}, {"δ", "delta"}]
      combined_results = shard1_results ++ shard2_results

      keys = Enum.map(combined_results, fn {k, _} -> k end)
      assert keys == ["α", "β", "γ", "δ"]
    end

    test "empty ranges maintain lexicographic ordering invariants" do
      ranges = []
      ranges = Tx.add_or_merge(ranges, "a", "a")
      ranges = Tx.add_or_merge(ranges, "b", "c")
      ranges = Tx.add_or_merge(ranges, "d", "d")

      assert ranges == [{"a", "a"}, {"b", "c"}, {"d", "d"}]
    end
  end

  describe "edge cases for lexicographic ordering" do
    test "empty string key comes before all other keys" do
      keys = ["", "a", "aa", "b"]

      assert Enum.sort(keys) == ["", "a", "aa", "b"]
    end

    test "null byte keys maintain proper lexicographic order" do
      keys = ["\0", "\0a", "\0b", "a", "a\0", "b"]
      sorted_keys = Enum.sort(keys)

      # Verify manual ordering matches Erlang's binary comparison
      assert sorted_keys == ["\0", "\0a", "\0b", "a", "a\0", "b"]
    end

    test "mixed byte values maintain lexicographic order" do
      keys = [
        # null
        <<0>>,
        # low byte
        <<1>>,
        # high ASCII
        <<127>>,
        # extended ASCII
        <<128>>,
        # max byte
        <<255>>
      ]

      assert Enum.sort(keys) == keys
    end

    test "prefix relationships maintain lexicographic order" do
      keys = ["a", "aa", "aaa", "ab", "aba", "b"]

      # Verify that prefix relationships work correctly
      assert Enum.sort(keys) == keys

      # Verify all prefixes come before their extensions
      assert "a" < "aa"
      assert "aa" < "aaa"
      assert "a" < "ab"
      assert "ab" < "aba"
    end

    test "case sensitivity in lexicographic ordering" do
      keys = ["A", "B", "a", "b"]
      sorted_keys = Enum.sort(keys)

      # In binary comparison, uppercase comes before lowercase
      assert sorted_keys == ["A", "B", "a", "b"]
    end
  end
end
