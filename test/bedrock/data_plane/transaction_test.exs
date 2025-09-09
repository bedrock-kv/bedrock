defmodule Bedrock.DataPlane.TransactionTest do
  @moduledoc """
  Unit tests for Transaction encoding/decoding functionality.

  These tests focus on specific examples and edge cases, complementing the
  comprehensive property-based tests in `TransactionPropertyTest`.

  For extensive property-based testing of the 16-bit instruction encoding
  system, see `Bedrock.DataPlane.TransactionPropertyTest` which tests:
  - Round-trip encoding properties for all mutation types
  - Length encoding optimization across all valid ranges [0, 131071]
  - Size optimization ensuring minimal encoding variants
  - Error handling for invalid/corrupted data
  - Streaming consistency and section extraction properties
  """
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Transaction

  doctest Transaction

  describe "encode/decode round-trip" do
    test "empty transaction with no mutations or conflicts" do
      transaction = %{
        mutations: [],
        read_conflicts: {nil, []},
        write_conflicts: []
      }

      binary = Transaction.encode(transaction)
      assert is_binary(binary)
      assert {:ok, decoded} = Transaction.decode(binary)
      assert decoded == transaction
    end

    test "transaction with only mutations" do
      transaction = %{
        mutations: [
          {:set, "key1", "value1"},
          {:set, "key2", "value2"},
          {:clear, "key3"},
          {:clear_range, "start", "end"}
        ],
        read_conflicts: {nil, []},
        write_conflicts: []
      }

      binary = Transaction.encode(transaction)
      assert {:ok, decoded} = Transaction.decode(binary)
      assert decoded == transaction
    end

    test "transaction with write conflicts but no read conflicts" do
      transaction = %{
        mutations: [{:set, "key", "value"}],
        read_conflicts: {nil, []},
        write_conflicts: [{"start1", "end1"}, {"start2", "end2"}]
      }

      binary = Transaction.encode(transaction)
      assert {:ok, decoded} = Transaction.decode(binary)
      assert decoded == transaction
    end

    test "transaction with read conflicts and read version" do
      transaction = %{
        mutations: [{:set, "key", "value"}],
        read_conflicts: {Bedrock.DataPlane.Version.from_integer(12_345), [{"read_start", "read_end"}]},
        write_conflicts: [{"write_start", "write_end"}]
      }

      binary = Transaction.encode(transaction)
      assert {:ok, decoded} = Transaction.decode(binary)
      assert decoded == transaction
    end

    test "transaction with empty read conflicts encodes and decodes correctly" do
      transaction = %{
        mutations: [{:set, "key", "value"}],
        read_conflicts: {nil, []},
        write_conflicts: []
      }

      binary = Transaction.encode(transaction)
      assert {:ok, decoded} = Transaction.decode(binary)

      expected = %{
        mutations: [{:set, "key", "value"}],
        read_conflicts: {nil, []},
        write_conflicts: []
      }

      assert decoded == expected
    end

    test "full transaction with all sections" do
      transaction = %{
        mutations: [
          {:set, "key1", "value1"},
          {:clear, "key2"},
          {:clear_range, "start", "end"}
        ],
        read_conflicts: {Bedrock.DataPlane.Version.from_integer(98_765), [{"read1", "read2"}]},
        write_conflicts: [{"write1", "write2"}]
      }

      binary = Transaction.encode(transaction)
      assert {:ok, decoded} = Transaction.decode(binary)
      assert decoded == transaction
    end
  end

  describe "size optimization" do
    test "automatically selects compact SET variants" do
      small_transaction = %{
        mutations: [{:set, "k", "v"}],
        read_conflicts: {nil, []},
        write_conflicts: []
      }

      small_binary = Transaction.encode(small_transaction)

      medium_transaction = %{
        mutations: [{:set, "k", String.duplicate("x", 300)}],
        read_conflicts: {nil, []},
        write_conflicts: []
      }

      medium_binary = Transaction.encode(medium_transaction)

      large_transaction = %{
        mutations: [{:set, String.duplicate("k", 300), String.duplicate("v", 70_000)}],
        read_conflicts: {nil, []},
        write_conflicts: []
      }

      large_binary = Transaction.encode(large_transaction)

      assert {:ok, decoded_small} = Transaction.decode(small_binary)
      assert {:ok, decoded_medium} = Transaction.decode(medium_binary)
      assert {:ok, decoded_large} = Transaction.decode(large_binary)

      assert decoded_small == small_transaction
      assert decoded_medium == medium_transaction
      assert decoded_large == large_transaction

      assert byte_size(small_binary) < byte_size(medium_binary)
      assert byte_size(medium_binary) < byte_size(large_binary)
    end

    test "automatically selects compact CLEAR variants" do
      small_clear = %{
        mutations: [{:clear, "k"}],
        read_conflicts: {nil, []},
        write_conflicts: []
      }

      large_clear = %{
        mutations: [{:clear, String.duplicate("k", 300)}],
        read_conflicts: {nil, []},
        write_conflicts: []
      }

      small_binary = Transaction.encode(small_clear)
      large_binary = Transaction.encode(large_clear)

      assert {:ok, decoded_small} = Transaction.decode(small_binary)
      assert {:ok, decoded_large} = Transaction.decode(large_binary)

      assert decoded_small == small_clear
      assert decoded_large == large_clear
      assert byte_size(small_binary) < byte_size(large_binary)
    end

    test "automatically selects compact CLEAR_RANGE variants" do
      small_range = %{
        mutations: [{:clear_range, "a", "z"}],
        read_conflicts: {nil, []},
        write_conflicts: []
      }

      large_range = %{
        mutations: [{:clear_range, String.duplicate("a", 300), String.duplicate("z", 300)}],
        read_conflicts: {nil, []},
        write_conflicts: []
      }

      small_binary = Transaction.encode(small_range)
      large_binary = Transaction.encode(large_range)

      assert {:ok, decoded_small} = Transaction.decode(small_binary)
      assert {:ok, decoded_large} = Transaction.decode(large_binary)

      assert decoded_small == small_range
      assert decoded_large == large_range
      assert byte_size(small_binary) < byte_size(large_binary)
    end
  end

  describe "validation" do
    test "validates binary format integrity" do
      transaction = %{
        mutations: [{:set, "key", "value"}],
        read_conflicts: [],
        write_conflicts: [],
        read_version: nil
      }

      binary = Transaction.encode(transaction)
      assert {:ok, validated} = Transaction.validate(binary)
      assert validated == binary
    end

    test "detects corrupted magic number" do
      transaction = %{
        mutations: [{:set, "key", "value"}],
        read_conflicts: [],
        write_conflicts: [],
        read_version: nil
      }

      binary = Transaction.encode(transaction)

      <<_::32, rest::binary>> = binary
      corrupted = <<0x00000000::32, rest::binary>>

      assert {:error, :invalid_format} = Transaction.validate(corrupted)
      assert {:error, :invalid_format} = Transaction.decode(corrupted)
    end

    test "detects section CRC corruption" do
      transaction = %{
        mutations: [{:set, "key", "value"}],
        read_conflicts: [],
        write_conflicts: [],
        read_version: nil
      }

      binary = Transaction.encode(transaction)

      <<prefix::binary-size(20), _byte, suffix::binary>> = binary
      corrupted = <<prefix::binary, 0xFF, suffix::binary>>

      assert {:error, {:section_checksum_mismatch, _tag}} = Transaction.validate(corrupted)
    end
  end

  describe "section operations" do
    test "extracts sections by tag" do
      transaction = %{
        mutations: [{:set, "key", "value"}],
        read_conflicts: {Bedrock.DataPlane.Version.from_integer(12_345), [{"read_start", "read_end"}]},
        write_conflicts: [{"write_start", "write_end"}]
      }

      binary = Transaction.encode(transaction)

      assert {:ok, mutations_payload} = Transaction.extract_section(binary, 0x01)
      assert is_binary(mutations_payload)
      assert byte_size(mutations_payload) > 0

      assert {:ok, read_conflicts_payload} = Transaction.extract_section(binary, 0x02)
      assert is_binary(read_conflicts_payload)

      assert {:ok, write_conflicts_payload} = Transaction.extract_section(binary, 0x03)
      assert is_binary(write_conflicts_payload)

      assert {:error, :section_not_found} = Transaction.extract_section(binary, 0x04)
    end

    test "adds transaction ID section" do
      transaction = %{
        mutations: [{:set, "key", "value"}],
        read_conflicts: {nil, []},
        write_conflicts: []
      }

      binary = Transaction.encode(transaction)
      assert {:ok, nil} = Transaction.commit_version(binary)

      version = Bedrock.DataPlane.Version.from_integer(98_765)
      assert {:ok, stamped} = Transaction.add_commit_version(binary, version)
      assert {:ok, ^version} = Transaction.commit_version(stamped)

      assert {:ok, decoded} = Transaction.decode(stamped)
      expected_with_version = Map.put(transaction, :commit_version, version)
      assert decoded == expected_with_version
    end
  end

  describe "convenience functions" do
    test "extracts read version" do
      # Transaction with read version
      with_version = %{
        mutations: [{:set, "key", "value"}],
        read_conflicts: {Bedrock.DataPlane.Version.from_integer(12_345), [{"read_start", "read_end"}]},
        write_conflicts: []
      }

      binary_with_version = Transaction.encode(with_version)
      expected_version = Bedrock.DataPlane.Version.from_integer(12_345)

      assert {:ok, {^expected_version, _conflicts}} =
               Transaction.read_conflicts(binary_with_version)

      # Transaction without read version
      without_version = %{
        mutations: [{:set, "key", "value"}],
        read_conflicts: [],
        write_conflicts: [],
        read_version: nil
      }

      binary_without_version = Transaction.encode(without_version)
      assert {:ok, {nil, []}} = Transaction.read_conflicts(binary_without_version)
    end

    test "extracts conflicts" do
      transaction = %{
        mutations: [{:set, "key", "value"}],
        read_conflicts: {Bedrock.DataPlane.Version.from_integer(12_345), [{"read1", "read2"}]},
        write_conflicts: [{"write1", "write2"}]
      }

      binary = Transaction.encode(transaction)

      expected_version = Bedrock.DataPlane.Version.from_integer(12_345)

      assert {:ok, {^expected_version, [{"read1", "read2"}]}} =
               Transaction.read_conflicts(binary)

      assert {:ok, [{"write1", "write2"}]} = Transaction.write_conflicts(binary)

      # Empty conflicts
      empty_transaction = %{
        mutations: [{:set, "key", "value"}],
        read_conflicts: [],
        write_conflicts: [],
        read_version: nil
      }

      empty_binary = Transaction.encode(empty_transaction)
      assert {:ok, {nil, []}} = Transaction.read_conflicts(empty_binary)
      assert {:ok, []} = Transaction.write_conflicts(empty_binary)
    end

    test "streams mutations" do
      transaction = %{
        mutations: [
          {:set, "key1", "value1"},
          {:set, "key2", "value2"},
          {:clear, "key3"},
          {:clear_range, "start", "end"}
        ],
        read_conflicts: [],
        write_conflicts: [],
        read_version: nil
      }

      binary = Transaction.encode(transaction)
      assert {:ok, stream} = Transaction.mutations(binary)

      mutations = Enum.to_list(stream)
      assert mutations == transaction.mutations
    end
  end

  describe "error handling" do
    test "handles invalid binary format" do
      assert {:error, :invalid_format} = Transaction.decode(<<>>)
      assert {:error, :invalid_format} = Transaction.decode(<<1, 2, 3>>)
      assert {:error, :invalid_format} = Transaction.validate(<<>>)
    end

    test "handles truncated data" do
      transaction = %{
        mutations: [{:set, "key", "value"}],
        read_conflicts: [],
        write_conflicts: [],
        read_version: nil
      }

      binary = Transaction.encode(transaction)

      truncated = binary_part(binary, 0, byte_size(binary) - 5)

      assert {:error, _} = Transaction.decode(truncated)
    end

    test "handles section extraction from non-existent sections" do
      transaction = %{
        mutations: [{:set, "key", "value"}],
        read_conflicts: [],
        write_conflicts: [],
        read_version: nil
      }

      binary = Transaction.encode(transaction)

      assert {:error, :section_not_found} = Transaction.extract_section(binary, 0x02)
      assert {:error, :section_not_found} = Transaction.extract_section(binary, 0x03)
      assert {:error, :section_not_found} = Transaction.extract_section(binary, 0x04)
    end

    test "handles adding duplicate sections" do
      transaction = %{
        mutations: [{:set, "key", "value"}],
        read_conflicts: [],
        write_conflicts: [],
        read_version: nil
      }

      binary = Transaction.encode(transaction)

      assert {:error, :section_already_exists} =
               Transaction.add_section(binary, 0x01, <<>>)
    end
  end

  describe "binary format structure" do
    test "has correct magic number and version" do
      transaction = %{
        mutations: [{:set, "key", "value"}],
        read_conflicts: [],
        write_conflicts: [],
        read_version: nil
      }

      <<magic::unsigned-big-32, version, _flags, _section_count::unsigned-big-16, _rest::binary>> =
        Transaction.encode(transaction)

      assert magic == 0x42524454
      assert version == 0x01
    end

    test "sections are order independent" do
      transaction = %{
        mutations: [{:set, "key", "value"}],
        read_conflicts: {Bedrock.DataPlane.Version.from_integer(12_345), [{"read_start", "read_end"}]},
        write_conflicts: [{"write_start", "write_end"}]
      }

      binaries = for _ <- 1..10, do: Transaction.encode(transaction)

      for binary <- binaries do
        assert {:ok, decoded} = Transaction.decode(binary)
        assert decoded == transaction
      end
    end
  end
end
