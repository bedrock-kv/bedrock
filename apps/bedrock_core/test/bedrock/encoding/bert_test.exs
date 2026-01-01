defmodule Bedrock.Encoding.BERTTest do
  use ExUnit.Case, async: true

  alias Bedrock.Encoding.BERT

  describe "pack/1" do
    test "packs various Erlang terms" do
      # Atoms
      packed = BERT.pack(:test_atom)
      assert is_binary(packed)

      # Integers
      packed = BERT.pack(42)
      assert is_binary(packed)

      # Lists
      packed = BERT.pack([1, 2, 3])
      assert is_binary(packed)

      # Tuples
      packed = BERT.pack({:ok, "value"})
      assert is_binary(packed)

      # Maps
      packed = BERT.pack(%{key: "value"})
      assert is_binary(packed)
    end
  end

  describe "unpack/1" do
    test "unpacks Erlang terms" do
      # Atoms
      packed = :erlang.term_to_binary(:test_atom)
      assert BERT.unpack(packed) == :test_atom

      # Integers
      packed = :erlang.term_to_binary(42)
      assert BERT.unpack(packed) == 42

      # Lists
      packed = :erlang.term_to_binary([1, 2, 3])
      assert BERT.unpack(packed) == [1, 2, 3]

      # Tuples
      packed = :erlang.term_to_binary({:ok, "value"})
      assert BERT.unpack(packed) == {:ok, "value"}

      # Maps
      packed = :erlang.term_to_binary(%{key: "value"})
      assert BERT.unpack(packed) == %{key: "value"}
    end
  end

  describe "round-trip" do
    test "pack and unpack are inverses for all Erlang terms" do
      terms = [
        # Basic types
        :atom,
        42,
        3.14,
        "string",
        <<1, 2, 3>>,

        # Collections
        [],
        [1, 2, 3],
        {},
        {:ok, :value},
        %{},
        %{key: "value", nested: %{a: 1}},

        # Complex nested structures
        {:user, "john", %{age: 30, roles: [:admin, :user]}},
        [a: 1, b: 2, c: [d: 3, e: 4]],

        # Edge cases
        nil,
        true,
        false
      ]

      for term <- terms do
        assert term |> BERT.pack() |> BERT.unpack() == term
      end
    end

    test "preserves complex nested data structures" do
      complex = %{
        users: [
          %{id: 1, name: "Alice", tags: [:admin, :active]},
          %{id: 2, name: "Bob", tags: [:user]}
        ],
        metadata: {
          :version,
          "1.0",
          [feature: :enabled, mode: :production]
        }
      }

      assert complex |> BERT.pack() |> BERT.unpack() == complex
    end
  end
end
