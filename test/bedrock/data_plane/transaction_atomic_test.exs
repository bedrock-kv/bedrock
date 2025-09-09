defmodule Bedrock.DataPlane.TransactionAtomicTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Transaction

  describe "atomic operations encoding/decoding" do
    test "encodes and decodes add with variable-length values" do
      transaction = %{
        mutations: [
          # 5 as 2-byte little-endian
          {:atomic, :add, "counter", <<5, 0>>},
          # 1 as 1-byte
          {:atomic, :add, "small_counter", <<1>>}
        ],
        read_conflicts: {nil, []},
        write_conflicts: []
      }

      encoded = Transaction.encode(transaction)
      {:ok, decoded} = Transaction.decode(encoded)

      assert decoded.mutations == [
               {:atomic, :add, "counter", <<5, 0>>},
               {:atomic, :add, "small_counter", <<1>>}
             ]
    end

    test "encodes and decodes min with variable-length values" do
      transaction = %{
        mutations: [
          # 10 as 3-byte little-endian
          {:atomic, :min, "min_val", <<10, 0, 0>>},
          # 255 as 1-byte
          {:atomic, :min, "other", <<255>>}
        ],
        read_conflicts: {nil, []},
        write_conflicts: []
      }

      encoded = Transaction.encode(transaction)
      {:ok, decoded} = Transaction.decode(encoded)

      assert decoded.mutations == [
               {:atomic, :min, "min_val", <<10, 0, 0>>},
               {:atomic, :min, "other", <<255>>}
             ]
    end

    test "encodes and decodes max with variable-length values" do
      transaction = %{
        mutations: [
          # 256 as 2-byte little-endian
          {:atomic, :max, "max_val", <<0, 1>>},
          # 42 as 1-byte
          {:atomic, :max, "single", <<42>>}
        ],
        read_conflicts: {nil, []},
        write_conflicts: []
      }

      encoded = Transaction.encode(transaction)
      {:ok, decoded} = Transaction.decode(encoded)

      assert decoded.mutations == [
               {:atomic, :max, "max_val", <<0, 1>>},
               {:atomic, :max, "single", <<42>>}
             ]
    end

    test "encodes and decodes mixed mutations including atomic operations" do
      transaction = %{
        mutations: [
          {:set, "key1", "value1"},
          {:atomic, :add, "counter", <<1, 0>>},
          {:clear, "key2"},
          {:atomic, :min, "min_val", <<5>>},
          {:clear_range, "range_start", "range_end"},
          {:atomic, :max, "max_val", <<100, 0, 1>>}
        ],
        read_conflicts: {nil, []},
        write_conflicts: []
      }

      encoded = Transaction.encode(transaction)
      {:ok, decoded} = Transaction.decode(encoded)

      assert decoded.mutations == transaction.mutations
    end

    test "handles empty atomic operands" do
      transaction = %{
        mutations: [
          # Empty operand
          {:atomic, :add, "key", <<>>}
        ],
        read_conflicts: {nil, []},
        write_conflicts: []
      }

      encoded = Transaction.encode(transaction)
      {:ok, decoded} = Transaction.decode(encoded)

      assert decoded.mutations == [{:atomic, :add, "key", <<>>}]
    end
  end
end
