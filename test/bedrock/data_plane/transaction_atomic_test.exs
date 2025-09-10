defmodule Bedrock.DataPlane.TransactionAtomicTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Transaction

  # Helper functions for common test patterns
  defp create_transaction(mutations) do
    %{
      mutations: mutations,
      read_conflicts: {nil, []},
      write_conflicts: []
    }
  end

  defp assert_roundtrip_encoding(transaction, expected_mutations) do
    encoded = Transaction.encode(transaction)
    assert {:ok, %{mutations: ^expected_mutations}} = Transaction.decode(encoded)
  end

  describe "atomic operations encoding/decoding" do
    test "encodes and decodes add with variable-length values" do
      mutations = [
        # 5 as 2-byte little-endian
        {:atomic, :add, "counter", <<5, 0>>},
        # 1 as 1-byte
        {:atomic, :add, "small_counter", <<1>>}
      ]

      transaction = create_transaction(mutations)
      assert_roundtrip_encoding(transaction, mutations)
    end

    test "encodes and decodes min with variable-length values" do
      mutations = [
        # 10 as 3-byte little-endian
        {:atomic, :min, "min_val", <<10, 0, 0>>},
        # 255 as 1-byte
        {:atomic, :min, "other", <<255>>}
      ]

      transaction = create_transaction(mutations)
      assert_roundtrip_encoding(transaction, mutations)
    end

    test "encodes and decodes max with variable-length values" do
      mutations = [
        # 256 as 2-byte little-endian
        {:atomic, :max, "max_val", <<0, 1>>},
        # 42 as 1-byte
        {:atomic, :max, "single", <<42>>}
      ]

      transaction = create_transaction(mutations)
      assert_roundtrip_encoding(transaction, mutations)
    end

    test "encodes and decodes mixed mutations including atomic operations" do
      mutations = [
        {:set, "key1", "value1"},
        {:atomic, :add, "counter", <<1, 0>>},
        {:clear, "key2"},
        {:atomic, :min, "min_val", <<5>>},
        {:clear_range, "range_start", "range_end"},
        {:atomic, :max, "max_val", <<100, 0, 1>>}
      ]

      transaction = create_transaction(mutations)
      assert_roundtrip_encoding(transaction, mutations)
    end

    test "handles empty atomic operands" do
      mutations = [
        # Empty operand
        {:atomic, :add, "key", <<>>}
      ]

      transaction = create_transaction(mutations)
      assert_roundtrip_encoding(transaction, mutations)
    end
  end
end
