defmodule Bedrock.Cluster.Gateway.TransactionBuilderAtomicOperationsTest do
  @moduledoc """
  Tests for atomic operations in TransactionBuilder.Tx module.

  These tests cover:
  - Atomic add, min, max operations in Tx
  - Integration with existing mutation tracking
  - Error handling for invalid values
  - Transaction commit with atomic operations
  - Range validation and conflict tracking
  """
  use ExUnit.Case, async: true

  alias Bedrock.Cluster.Gateway.TransactionBuilder.Tx
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version

  # Helper functions for testing atomic operations
  defp int_to_binary(n) when n >= 0 and n <= 255, do: <<n>>
  defp int_to_binary(n) when n >= -128 and n < 0, do: <<n::signed-little-8>>
  defp int_to_binary(n) when n >= -32_768 and n < -128, do: <<n::signed-little-16>>
  defp int_to_binary(n) when n > 255 and n <= 65_535, do: <<n::little-16>>
  defp int_to_binary(n) when n >= -2_147_483_648 and n < -32_768, do: <<n::signed-little-32>>
  defp int_to_binary(n) when n > 65_535 and n <= 4_294_967_295, do: <<n::little-32>>
  defp int_to_binary(n) when n >= -9_223_372_036_854_775_808 and n < -2_147_483_648, do: <<n::signed-little-64>>
  defp int_to_binary(n) when n > 4_294_967_295, do: <<n::little-64>>
  # Catch-all for very large numbers - use arbitrary precision
  defp int_to_binary(n), do: :binary.encode_unsigned(abs(n), :little)

  # Helper to commit transaction and decode result with pattern matching
  defp commit_and_decode(tx, read_version \\ nil) do
    assert {:ok, decoded} = tx |> Tx.commit(read_version) |> Transaction.decode()
    decoded
  end

  # Helper to assert atomic mutation with specific operation
  defp assert_atomic_mutation(tx, op, key, value, read_version \\ nil) do
    assert %{mutations: [{:atomic, ^op, ^key, ^value}]} = commit_and_decode(tx, read_version)
  end

  # Helper to assert mutations match expected list (order-independent)
  defp assert_mutations_match(tx, expected_mutations, read_version \\ nil) do
    decoded = commit_and_decode(tx, read_version)
    assert Enum.sort(decoded.mutations) == Enum.sort(expected_mutations)
    decoded
  end

  # Helper to assert no write conflicts for atomic operations
  defp assert_no_write_conflicts(tx, read_version \\ nil) do
    assert %{write_conflicts: []} = commit_and_decode(tx, read_version)
  end

  describe "Tx.add/3" do
    test "adds add mutation to transaction" do
      tx = Tx.atomic_operation(Tx.new(), "counter", :add, <<5>>)
      assert_atomic_mutation(tx, :add, "counter", <<5>>)
    end

    test "handles positive and negative values" do
      tx =
        Tx.new()
        |> Tx.atomic_operation("counter", :add, int_to_binary(10))
        |> Tx.atomic_operation("balance", :add, int_to_binary(-5))
        |> Tx.atomic_operation("zero", :add, int_to_binary(0))

      expected_mutations = [
        {:atomic, :add, "zero", <<0>>},
        # -5 as unsigned byte
        {:atomic, :add, "balance", <<251>>},
        {:atomic, :add, "counter", <<10>>}
      ]

      assert_mutations_match(tx, expected_mutations)
    end

    test "handles 64-bit signed integer boundaries" do
      tx =
        Tx.new()
        |> Tx.atomic_operation("max_val", :add, int_to_binary(9_223_372_036_854_775_807))
        |> Tx.atomic_operation("min_val", :add, int_to_binary(-9_223_372_036_854_775_808))

      expected_mutations = [
        {:atomic, :add, "min_val", int_to_binary(-9_223_372_036_854_775_808)},
        {:atomic, :add, "max_val", int_to_binary(9_223_372_036_854_775_807)}
      ]

      assert_mutations_match(tx, expected_mutations)
    end

    test "accepts any binary value (no longer validates integer range)" do
      tx =
        Tx.new()
        |> Tx.atomic_operation("large_value", :add, int_to_binary(9_223_372_036_854_775_808))
        |> Tx.atomic_operation("small_value", :add, int_to_binary(-9_223_372_036_854_775_809))

      assert %{mutations: mutations} = commit_and_decode(tx)
      assert length(mutations) == 2
    end

    test "overwrites previous operations on same key" do
      tx =
        Tx.new()
        |> Tx.atomic_operation("counter", :add, int_to_binary(5))
        |> Tx.atomic_operation("counter", :add, int_to_binary(10))

      assert_atomic_mutation(tx, :add, "counter", int_to_binary(10))
    end

    test "removes conflicting operations on same key range" do
      tx =
        Tx.new()
        |> Tx.set("counter", "old_value")
        |> Tx.atomic_operation("counter", :add, int_to_binary(5))

      assert_atomic_mutation(tx, :add, "counter", int_to_binary(5))
    end

    test "does not add write conflicts for atomic operations" do
      tx = Tx.atomic_operation(Tx.new(), "counter", :add, int_to_binary(5))
      assert_no_write_conflicts(tx)
    end
  end

  describe "Tx.min/3" do
    test "adds min mutation to transaction" do
      tx = Tx.atomic_operation(Tx.new(), "min_temp", :min, <<25>>)
      assert_atomic_mutation(tx, :min, "min_temp", <<25>>)
    end

    test "handles various integer values" do
      tx =
        Tx.new()
        |> Tx.atomic_operation("temp", :min, int_to_binary(22))
        |> Tx.atomic_operation("depth", :min, int_to_binary(-100))
        |> Tx.atomic_operation("baseline", :min, int_to_binary(0))

      expected_mutations = [
        {:atomic, :min, "baseline", int_to_binary(0)},
        {:atomic, :min, "depth", int_to_binary(-100)},
        {:atomic, :min, "temp", int_to_binary(22)}
      ]

      assert_mutations_match(tx, expected_mutations)
    end

    test "handles 64-bit signed integer boundaries" do
      tx =
        Tx.new()
        |> Tx.atomic_operation("max_bound", :min, int_to_binary(9_223_372_036_854_775_807))
        |> Tx.atomic_operation("min_bound", :min, int_to_binary(-9_223_372_036_854_775_808))

      expected_mutations = [
        {:atomic, :min, "min_bound", int_to_binary(-9_223_372_036_854_775_808)},
        {:atomic, :min, "max_bound", int_to_binary(9_223_372_036_854_775_807)}
      ]

      assert_mutations_match(tx, expected_mutations)
    end

    test "accepts any binary value (no longer validates integer range)" do
      tx =
        Tx.new()
        |> Tx.atomic_operation("large_value", :min, int_to_binary(9_223_372_036_854_775_808))
        |> Tx.atomic_operation("small_value", :min, int_to_binary(-9_223_372_036_854_775_809))

      assert %{mutations: mutations} = commit_and_decode(tx)
      assert length(mutations) == 2
    end

    test "overwrites previous operations on same key" do
      tx =
        Tx.new()
        |> Tx.atomic_operation("min_val", :min, int_to_binary(100))
        |> Tx.atomic_operation("min_val", :min, int_to_binary(50))

      assert_atomic_mutation(tx, :min, "min_val", int_to_binary(50))
    end

    test "removes conflicting operations on same key range" do
      tx =
        Tx.new()
        |> Tx.set("min_val", "old_value")
        |> Tx.atomic_operation("min_val", :min, int_to_binary(25))

      assert_atomic_mutation(tx, :min, "min_val", int_to_binary(25))
    end
  end

  describe "Tx.max/3" do
    test "adds max mutation to transaction" do
      tx = Tx.atomic_operation(Tx.new(), "high_score", :max, int_to_binary(1500))
      assert_atomic_mutation(tx, :max, "high_score", int_to_binary(1500))
    end

    test "handles various integer values" do
      tx =
        Tx.new()
        |> Tx.atomic_operation("score", :max, int_to_binary(100))
        |> Tx.atomic_operation("threshold", :max, int_to_binary(-50))
        |> Tx.atomic_operation("ceiling", :max, int_to_binary(0))

      expected_mutations = [
        {:atomic, :max, "ceiling", int_to_binary(0)},
        {:atomic, :max, "threshold", int_to_binary(-50)},
        {:atomic, :max, "score", int_to_binary(100)}
      ]

      assert_mutations_match(tx, expected_mutations)
    end

    test "handles variable-length binary values" do
      tx =
        Tx.new()
        |> Tx.atomic_operation("single_byte", :max, <<255>>)
        # 256 as little-endian
        |> Tx.atomic_operation("two_bytes", :max, <<0, 1>>)
        # 65537 as little-endian
        |> Tx.atomic_operation("three_bytes", :max, <<1, 0, 1>>)

      expected_mutations = [
        {:atomic, :max, "single_byte", <<255>>},
        {:atomic, :max, "two_bytes", <<0, 1>>},
        {:atomic, :max, "three_bytes", <<1, 0, 1>>}
      ]

      assert_mutations_match(tx, expected_mutations)
    end

    test "validates value is binary" do
      tx = Tx.atomic_operation(Tx.new(), "valid", :max, int_to_binary(42))
      assert_atomic_mutation(tx, :max, "valid", int_to_binary(42))

      # Test invalid value types
      assert_raise FunctionClauseError, fn ->
        # Integer instead of binary
        Tx.atomic_operation(Tx.new(), "invalid", :max, 42)
      end
    end

    test "overwrites previous operations on same key" do
      tx =
        Tx.new()
        |> Tx.atomic_operation("max_val", :max, int_to_binary(50))
        |> Tx.atomic_operation("max_val", :max, int_to_binary(100))

      assert_atomic_mutation(tx, :max, "max_val", int_to_binary(100))
    end

    test "removes conflicting operations on same key range" do
      tx =
        Tx.new()
        |> Tx.set("max_val", "old_value")
        |> Tx.atomic_operation("max_val", :max, int_to_binary(75))

      assert_atomic_mutation(tx, :max, "max_val", int_to_binary(75))
    end
  end

  describe "mixed atomic operations" do
    test "supports all atomic operation types in same transaction" do
      tx =
        Tx.new()
        |> Tx.atomic_operation("counter", :add, int_to_binary(5))
        |> Tx.atomic_operation("min_temp", :min, int_to_binary(15))
        |> Tx.atomic_operation("max_score", :max, int_to_binary(100))
        |> Tx.atomic_operation("balance", :add, int_to_binary(-10))

      expected_mutations = [
        {:atomic, :add, "balance", int_to_binary(-10)},
        {:atomic, :max, "max_score", int_to_binary(100)},
        {:atomic, :min, "min_temp", int_to_binary(15)},
        {:atomic, :add, "counter", int_to_binary(5)}
      ]

      assert_mutations_match(tx, expected_mutations)
    end

    test "mixes atomic operations with regular mutations" do
      tx =
        Tx.new()
        |> Tx.set("name", "Alice")
        |> Tx.atomic_operation("visits", :add, int_to_binary(1))
        |> Tx.clear("temp_data")
        |> Tx.atomic_operation("min_price", :min, int_to_binary(100))
        |> Tx.clear_range("cache/", "cache0")
        |> Tx.atomic_operation("max_users", :max, int_to_binary(50))

      expected_mutations = [
        {:atomic, :max, "max_users", int_to_binary(50)},
        {:clear_range, "cache/", "cache0"},
        {:atomic, :min, "min_price", int_to_binary(100)},
        {:clear, "temp_data"},
        {:atomic, :add, "visits", int_to_binary(1)},
        {:set, "name", "Alice"}
      ]

      assert_mutations_match(tx, expected_mutations)
    end

    test "handles conflicts between different atomic operations on same key" do
      tx =
        Tx.new()
        |> Tx.atomic_operation("value", :add, int_to_binary(5))
        # Should overwrite add
        |> Tx.atomic_operation("value", :min, int_to_binary(10))
        # Should overwrite min
        |> Tx.atomic_operation("value", :max, int_to_binary(15))

      assert_atomic_mutation(tx, :max, "value", int_to_binary(15))
    end
  end

  describe "conflict tracking and ranges" do
    test "atomic operations do not add write conflicts" do
      tx =
        Tx.new()
        |> Tx.atomic_operation("counter1", :add, int_to_binary(1))
        |> Tx.atomic_operation("counter2", :min, int_to_binary(10))
        |> Tx.atomic_operation("counter3", :max, int_to_binary(100))

      assert_no_write_conflicts(tx)
    end

    test "clear_range removes atomic operations within range" do
      tx =
        Tx.new()
        |> Tx.atomic_operation("item_a", :add, int_to_binary(1))
        |> Tx.atomic_operation("item_b", :min, int_to_binary(5))
        |> Tx.atomic_operation("item_c", :max, int_to_binary(10))
        # Should remove all item_* operations
        |> Tx.clear_range("item_", "item~")

      assert %{mutations: [{:clear_range, "item_", "item~"}]} = commit_and_decode(tx)
    end

    test "atomic operations work with read conflicts" do
      tx =
        Tx.new()
        |> Tx.merge_storage_read("existing_key", "existing_value")
        |> Tx.atomic_operation("counter", :add, int_to_binary(5))

      read_version = Version.from_integer(12_345)
      decoded = commit_and_decode(tx, read_version)

      assert %{
               mutations: [{:atomic, :add, "counter", _}],
               read_conflicts: {^read_version, [{"existing_key", "existing_key\0"}]},
               write_conflicts: []
             } = decoded
    end
  end

  describe "binary encoding integration" do
    test "atomic operations can be encoded and decoded" do
      tx =
        Tx.new()
        |> Tx.atomic_operation("counter", :add, int_to_binary(5))
        |> Tx.atomic_operation("min_val", :min, int_to_binary(10))
        |> Tx.atomic_operation("max_val", :max, int_to_binary(100))

      binary = Tx.commit(tx, nil)
      assert {:ok, decoded} = Transaction.decode(binary)

      # Should be idempotent: re-encode and decode
      re_encoded_binary = Transaction.encode(decoded)
      assert {:ok, ^decoded} = Transaction.decode(re_encoded_binary)
    end

    test "large transaction with many atomic operations" do
      tx =
        Enum.reduce(1..100, Tx.new(), fn i, acc_tx ->
          acc_tx
          |> Tx.atomic_operation("counter_#{i}", :add, int_to_binary(i))
          |> Tx.atomic_operation("min_#{i}", :min, int_to_binary(i * 10))
          |> Tx.atomic_operation("max_#{i}", :max, int_to_binary(i * 100))
        end)

      binary = Tx.commit(tx, nil)
      assert {:ok, decoded} = Transaction.decode(binary)

      # Should have 300 mutations (3 per iteration) and re-encode successfully
      assert length(decoded.mutations) == 300
      re_encoded_binary = Transaction.encode(decoded)
      assert {:ok, ^decoded} = Transaction.decode(re_encoded_binary)
    end
  end

  describe "repeatable read with atomic operations" do
    test "repeatable_read computes add values correctly" do
      tx = Tx.atomic_operation(Tx.new(), "counter", :add, int_to_binary(5))
      assert Tx.repeatable_read(tx, "counter") == int_to_binary(5)
    end

    test "repeatable_read handles different atomic operations" do
      tx =
        Tx.new()
        |> Tx.atomic_operation("add_key", :add, int_to_binary(10))
        |> Tx.atomic_operation("min_key", :min, int_to_binary(20))
        |> Tx.atomic_operation("max_key", :max, int_to_binary(30))

      # empty + 10 = 10
      assert Tx.repeatable_read(tx, "add_key") == int_to_binary(10)
      # min(empty, 20) = 20 (when existing is empty, return operand)
      assert Tx.repeatable_read(tx, "min_key") == int_to_binary(20)
      # max(empty, 30) = 30 (when existing is empty, return operand)
      assert Tx.repeatable_read(tx, "max_key") == int_to_binary(30)
      assert Tx.repeatable_read(tx, "nonexistent") == nil
    end

    test "repeatable_read prioritizes writes over reads and computes atomic values" do
      tx =
        Tx.new()
        |> Tx.merge_storage_read("key", int_to_binary(100))
        |> Tx.atomic_operation("key", :add, int_to_binary(42))

      # 100 + 42 = 142
      assert Tx.repeatable_read(tx, "key") == int_to_binary(142)
    end

    test "repeatable_read uses existing read values for atomic computations" do
      tx =
        Tx.new()
        |> Tx.merge_storage_read("counter", int_to_binary(10))
        |> Tx.atomic_operation("counter", :add, int_to_binary(5))

      # 10 + 5 = 15
      assert Tx.repeatable_read(tx, "counter") == int_to_binary(15)
    end

    test "repeatable_read handles min with existing values" do
      tx =
        Tx.new()
        |> Tx.merge_storage_read("min_val", int_to_binary(50))
        |> Tx.atomic_operation("min_val", :min, int_to_binary(30))

      # min(50, 30) = 30
      assert Tx.repeatable_read(tx, "min_val") == int_to_binary(30)
    end

    test "repeatable_read handles max with existing values" do
      tx =
        Tx.new()
        |> Tx.merge_storage_read("max_val", int_to_binary(20))
        |> Tx.atomic_operation("max_val", :max, int_to_binary(35))

      # max(20, 35) = 35
      assert Tx.repeatable_read(tx, "max_val") == int_to_binary(35)
    end
  end

  describe "read conflict behavior with atomic operations" do
    test "atomic operations by themselves do not create read conflicts" do
      tx =
        Tx.new()
        |> Tx.atomic_operation("counter1", :add, int_to_binary(5))
        |> Tx.atomic_operation("counter2", :min, int_to_binary(10))
        |> Tx.atomic_operation("counter3", :max, int_to_binary(15))

      assert %{
               mutations: mutations,
               write_conflicts: [],
               read_conflicts: {nil, []}
             } = commit_and_decode(tx)

      assert length(mutations) == 3
    end

    test "existing reads with atomic operations maintain read conflicts" do
      tx =
        Tx.new()
        |> Tx.merge_storage_read("existing_key", "existing_value")
        |> Tx.atomic_operation("counter", :add, int_to_binary(5))

      read_version = Version.from_integer(12_345)

      assert %{
               read_conflicts: {^read_version, [{"existing_key", "existing_key\0"}]},
               write_conflicts: []
             } = commit_and_decode(tx, read_version)
    end

    test "atomic operation followed by repeatable_read does not add read conflicts to Tx" do
      tx = Tx.atomic_operation(Tx.new(), "counter", :add, int_to_binary(5))

      # Should compute value but not modify transaction's read state
      assert Tx.repeatable_read(tx, "counter") == int_to_binary(5)
      assert tx.reads == %{}

      assert %{read_conflicts: {nil, []}} = commit_and_decode(tx)
    end

    test "atomic operation on key with existing read uses cached value for computation" do
      tx =
        Tx.new()
        |> Tx.merge_storage_read("counter", int_to_binary(100))
        |> Tx.atomic_operation("counter", :add, int_to_binary(25))

      # Should use cached read value (100) for computation
      assert Tx.repeatable_read(tx, "counter") == int_to_binary(125)

      read_version = Version.from_integer(12_345)

      assert %{
               read_conflicts: {^read_version, [{"counter", "counter\0"}]},
               write_conflicts: []
             } = commit_and_decode(tx, read_version)
    end

    test "mixed operations maintain correct read/write conflict separation" do
      tx =
        Tx.new()
        |> Tx.merge_storage_read("read_key1", "value1")
        |> Tx.merge_storage_read("read_key2", "value2")
        |> Tx.set("write_key1", "value3")
        |> Tx.atomic_operation("atomic_key1", :add, int_to_binary(10))
        |> Tx.atomic_operation("atomic_key2", :min, int_to_binary(20))

      read_version = Version.from_integer(12_345)
      decoded = commit_and_decode(tx, read_version)

      expected_read_conflicts = [
        {"read_key1", "read_key1\0"},
        {"read_key2", "read_key2\0"}
      ]

      expected_write_conflicts = [
        {"write_key1", "write_key1\0"}
      ]

      assert {^read_version, read_conflicts} = decoded.read_conflicts
      assert Enum.sort(read_conflicts) == Enum.sort(expected_read_conflicts)
      assert Enum.sort(decoded.write_conflicts) == Enum.sort(expected_write_conflicts)
    end
  end

  describe "edge cases and error conditions" do
    test "validates key is binary" do
      tx = Tx.new()

      for invalid_key <- [:atom_key, 123, ["list"]] do
        assert_raise FunctionClauseError, fn ->
          Tx.atomic_operation(tx, invalid_key, :add, int_to_binary(5))
        end
      end
    end

    test "validates value is binary" do
      tx = Tx.new()

      for invalid_value <- [123, :atom] do
        assert_raise FunctionClauseError, fn ->
          Tx.atomic_operation(tx, "key", :add, invalid_value)
        end
      end

      # Valid binary should work
      final_tx = Tx.atomic_operation(Tx.new(), "key", :min, int_to_binary(3))
      assert_atomic_mutation(final_tx, :min, "key", <<3>>)
    end

    test "handles empty transaction with only atomic operations" do
      tx = Tx.atomic_operation(Tx.new(), "counter", :add, int_to_binary(1))

      assert %{
               mutations: [{:atomic, :add, "counter", _}],
               write_conflicts: [],
               read_conflicts: {nil, []}
             } = commit_and_decode(tx)
    end

    test "handles zero values correctly" do
      tx =
        Tx.new()
        |> Tx.atomic_operation("zero_add", :add, int_to_binary(0))
        |> Tx.atomic_operation("zero_min", :min, int_to_binary(0))
        |> Tx.atomic_operation("zero_max", :max, int_to_binary(0))

      expected_mutations = [
        {:atomic, :max, "zero_max", int_to_binary(0)},
        {:atomic, :min, "zero_min", int_to_binary(0)},
        {:atomic, :add, "zero_add", int_to_binary(0)}
      ]

      assert_mutations_match(tx, expected_mutations)
    end
  end

  describe "Tx.bit_and/3" do
    test "adds bit_and mutation to transaction" do
      tx = Tx.atomic_operation(Tx.new(), "flags", :bit_and, <<0b11110000>>)
      assert_atomic_mutation(tx, :bit_and, "flags", <<0b11110000>>)
    end

    test "handles multiple bit operations" do
      tx =
        Tx.new()
        |> Tx.atomic_operation("flags1", :bit_and, <<0xFF, 0x00>>)
        |> Tx.atomic_operation("flags2", :bit_or, <<0x0F, 0xF0>>)
        |> Tx.atomic_operation("flags3", :bit_xor, <<0xAA, 0x55>>)

      expected_mutations = [
        {:atomic, :bit_xor, "flags3", <<0xAA, 0x55>>},
        {:atomic, :bit_or, "flags2", <<0x0F, 0xF0>>},
        {:atomic, :bit_and, "flags1", <<0xFF, 0x00>>}
      ]

      assert_mutations_match(tx, expected_mutations)
    end

    test "overwrites previous bit operations on same key" do
      tx =
        Tx.new()
        |> Tx.atomic_operation("flags", :bit_and, <<0xFF>>)
        |> Tx.atomic_operation("flags", :bit_or, <<0x0F>>)

      # Should only have the OR operation (overwrites AND)
      assert_atomic_mutation(tx, :bit_or, "flags", <<0x0F>>)
    end
  end

  describe "Tx.bit_or/3" do
    test "adds bit_or mutation to transaction" do
      tx = Tx.atomic_operation(Tx.new(), "permissions", :bit_or, <<0b00001111>>)
      assert_atomic_mutation(tx, :bit_or, "permissions", <<0b00001111>>)
    end

    test "handles empty and multi-byte values" do
      tx =
        Tx.new()
        |> Tx.atomic_operation("empty", :bit_or, <<>>)
        |> Tx.atomic_operation("multi", :bit_or, <<0x12, 0x34, 0x56, 0x78>>)

      expected_mutations = [
        {:atomic, :bit_or, "multi", <<0x12, 0x34, 0x56, 0x78>>},
        {:atomic, :bit_or, "empty", <<>>}
      ]

      assert_mutations_match(tx, expected_mutations)
    end
  end

  describe "Tx.bit_xor/3" do
    test "adds bit_xor mutation to transaction" do
      tx = Tx.atomic_operation(Tx.new(), "toggle", :bit_xor, <<0b10101010>>)
      assert_atomic_mutation(tx, :bit_xor, "toggle", <<0b10101010>>)
    end

    test "handles complex patterns" do
      pattern = <<0xFF, 0x00, 0xAA, 0x55, 0xF0, 0x0F>>
      tx = Tx.atomic_operation(Tx.new(), "pattern", :bit_xor, pattern)
      assert_atomic_mutation(tx, :bit_xor, "pattern", pattern)
    end
  end

  describe "Tx.byte_min/3" do
    test "adds byte_min mutation to transaction" do
      tx = Tx.atomic_operation(Tx.new(), "min_string", :byte_min, "hello")
      assert_atomic_mutation(tx, :byte_min, "min_string", "hello")
    end

    test "handles various string values" do
      tx =
        Tx.new()
        |> Tx.atomic_operation("earliest", :byte_min, "2024-01-01")
        |> Tx.atomic_operation("alpha", :byte_min, "apple")
        |> Tx.atomic_operation("empty", :byte_min, <<>>)

      expected_mutations = [
        {:atomic, :byte_min, "empty", <<>>},
        {:atomic, :byte_min, "alpha", "apple"},
        {:atomic, :byte_min, "earliest", "2024-01-01"}
      ]

      assert_mutations_match(tx, expected_mutations)
    end

    test "overwrites previous operations on same key" do
      tx =
        Tx.new()
        |> Tx.atomic_operation("key", :byte_min, "first")
        |> Tx.atomic_operation("key", :byte_max, "second")

      # Should only have the max operation (overwrites min)
      assert_atomic_mutation(tx, :byte_max, "key", "second")
    end
  end

  describe "Tx.byte_max/3" do
    test "adds byte_max mutation to transaction" do
      tx = Tx.atomic_operation(Tx.new(), "max_version", :byte_max, "1.2.3")
      assert_atomic_mutation(tx, :byte_max, "max_version", "1.2.3")
    end

    test "handles binary vs string values" do
      tx =
        Tx.new()
        |> Tx.atomic_operation("binary", :byte_max, <<1, 2, 3, 4>>)
        |> Tx.atomic_operation("string", :byte_max, "version")

      expected_mutations = [
        {:atomic, :byte_max, "string", "version"},
        {:atomic, :byte_max, "binary", <<1, 2, 3, 4>>}
      ]

      assert_mutations_match(tx, expected_mutations)
    end
  end

  describe "Tx.append_if_fits/3" do
    test "adds append_if_fits mutation to transaction" do
      tx = Tx.atomic_operation(Tx.new(), "log", :append_if_fits, "new entry\n")
      assert_atomic_mutation(tx, :append_if_fits, "log", "new entry\n")
    end

    test "handles various append scenarios" do
      tx =
        Tx.new()
        |> Tx.atomic_operation("log1", :append_if_fits, "first entry")
        |> Tx.atomic_operation("log2", :append_if_fits, " - second entry")
        |> Tx.atomic_operation("empty", :append_if_fits, "")

      expected_mutations = [
        {:atomic, :append_if_fits, "empty", ""},
        {:atomic, :append_if_fits, "log2", " - second entry"},
        {:atomic, :append_if_fits, "log1", "first entry"}
      ]

      assert_mutations_match(tx, expected_mutations)
    end

    test "handles large values within limits" do
      large_value = String.duplicate("x", 1000)
      tx = Tx.atomic_operation(Tx.new(), "large", :append_if_fits, large_value)
      assert_atomic_mutation(tx, :append_if_fits, "large", large_value)
    end

    test "overwrites previous operations on same key" do
      tx =
        Tx.new()
        |> Tx.atomic_operation("buffer", :append_if_fits, "first")
        |> Tx.atomic_operation("buffer", :append_if_fits, "second")

      # Should only have the second operation (overwrites first)
      assert_atomic_mutation(tx, :append_if_fits, "buffer", "second")
    end
  end

  describe "comprehensive atomic operations" do
    test "can combine all atomic operations in one transaction" do
      tx =
        Tx.new()
        |> Tx.atomic_operation("counter", :add, int_to_binary(5))
        |> Tx.atomic_operation("min_val", :min, int_to_binary(10))
        |> Tx.atomic_operation("max_val", :max, int_to_binary(100))
        |> Tx.atomic_operation("flags", :bit_and, <<0xFF>>)
        |> Tx.atomic_operation("permissions", :bit_or, <<0x0F>>)
        |> Tx.atomic_operation("toggle", :bit_xor, <<0xAA>>)
        |> Tx.atomic_operation("earliest", :byte_min, "2024-01-01")
        |> Tx.atomic_operation("latest", :byte_max, "2024-12-31")
        |> Tx.atomic_operation("log", :append_if_fits, "entry")

      decoded = commit_and_decode(tx)
      assert length(decoded.mutations) == 9

      # Verify all operation types are present
      operations = MapSet.new(decoded.mutations, fn {:atomic, op, _key, _value} -> op end)
      expected_operations = [:add, :min, :max, :bit_and, :bit_or, :bit_xor, :byte_min, :byte_max, :append_if_fits]

      assert operations == MapSet.new(expected_operations)
    end
  end
end
