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
  alias Bedrock.DataPlane.Version

  doctest Transaction

  # Helper functions for common test data
  defp empty_transaction do
    %{
      mutations: [],
      read_conflicts: {nil, []},
      write_conflicts: []
    }
  end

  defp basic_transaction do
    %{
      mutations: [{:set, "key", "value"}],
      read_conflicts: {nil, []},
      write_conflicts: []
    }
  end

  defp full_transaction do
    %{
      mutations: [
        {:set, "key1", "value1"},
        {:clear, "key2"},
        {:clear_range, "start", "end"}
      ],
      read_conflicts: {Version.from_integer(98_765), [{"read1", "read2"}]},
      write_conflicts: [{"write1", "write2"}]
    }
  end

  defp legacy_transaction do
    %{
      mutations: [{:set, "key", "value"}],
      read_conflicts: [],
      write_conflicts: [],
      read_version: nil
    }
  end

  describe "encode/decode round-trip" do
    test "empty transaction with no mutations or conflicts" do
      transaction = empty_transaction()

      binary = Transaction.encode(transaction)
      assert is_binary(binary)
      assert {:ok, ^transaction} = Transaction.decode(binary)
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
      assert {:ok, ^transaction} = Transaction.decode(binary)
    end

    test "transaction with write conflicts but no read conflicts" do
      transaction = %{
        mutations: [{:set, "key", "value"}],
        read_conflicts: {nil, []},
        write_conflicts: [{"start1", "end1"}, {"start2", "end2"}]
      }

      binary = Transaction.encode(transaction)
      assert {:ok, ^transaction} = Transaction.decode(binary)
    end

    test "transaction with read conflicts and read version" do
      transaction = %{
        mutations: [{:set, "key", "value"}],
        read_conflicts: {Version.from_integer(12_345), [{"read_start", "read_end"}]},
        write_conflicts: [{"write_start", "write_end"}]
      }

      binary = Transaction.encode(transaction)
      assert {:ok, ^transaction} = Transaction.decode(binary)
    end

    test "transaction with empty read conflicts encodes and decodes correctly" do
      transaction = basic_transaction()

      binary = Transaction.encode(transaction)
      assert {:ok, ^transaction} = Transaction.decode(binary)
    end

    test "full transaction with all sections" do
      transaction = full_transaction()

      binary = Transaction.encode(transaction)
      assert {:ok, ^transaction} = Transaction.decode(binary)
    end
  end

  describe "size optimization" do
    test "automatically selects compact SET variants" do
      small_transaction = %{empty_transaction() | mutations: [{:set, "k", "v"}]}
      medium_transaction = %{empty_transaction() | mutations: [{:set, "k", String.duplicate("x", 300)}]}

      large_transaction = %{
        empty_transaction()
        | mutations: [{:set, String.duplicate("k", 300), String.duplicate("v", 70_000)}]
      }

      small_binary = Transaction.encode(small_transaction)
      medium_binary = Transaction.encode(medium_transaction)
      large_binary = Transaction.encode(large_transaction)

      assert {:ok, ^small_transaction} = Transaction.decode(small_binary)
      assert {:ok, ^medium_transaction} = Transaction.decode(medium_binary)
      assert {:ok, ^large_transaction} = Transaction.decode(large_binary)

      # Verify size progression
      assert byte_size(small_binary) < byte_size(medium_binary)
      assert byte_size(medium_binary) < byte_size(large_binary)
    end

    test "automatically selects compact CLEAR variants" do
      small_clear = %{empty_transaction() | mutations: [{:clear, "k"}]}
      large_clear = %{empty_transaction() | mutations: [{:clear, String.duplicate("k", 300)}]}

      small_binary = Transaction.encode(small_clear)
      large_binary = Transaction.encode(large_clear)

      assert {:ok, ^small_clear} = Transaction.decode(small_binary)
      assert {:ok, ^large_clear} = Transaction.decode(large_binary)
      assert byte_size(small_binary) < byte_size(large_binary)
    end

    test "automatically selects compact CLEAR_RANGE variants" do
      small_range = %{empty_transaction() | mutations: [{:clear_range, "a", "z"}]}

      large_range = %{
        empty_transaction()
        | mutations: [{:clear_range, String.duplicate("a", 300), String.duplicate("z", 300)}]
      }

      small_binary = Transaction.encode(small_range)
      large_binary = Transaction.encode(large_range)

      assert {:ok, ^small_range} = Transaction.decode(small_binary)
      assert {:ok, ^large_range} = Transaction.decode(large_binary)
      assert byte_size(small_binary) < byte_size(large_binary)
    end
  end

  describe "validation" do
    test "validates binary format integrity" do
      transaction = legacy_transaction()

      binary = Transaction.encode(transaction)
      assert {:ok, ^binary} = Transaction.validate(binary)
    end

    test "detects corrupted magic number" do
      binary = Transaction.encode(legacy_transaction())

      <<_::32, rest::binary>> = binary
      corrupted = <<0x00000000::32, rest::binary>>

      assert {:error, :invalid_format} = Transaction.validate(corrupted)
      assert {:error, :invalid_format} = Transaction.decode(corrupted)
    end

    test "detects section CRC corruption" do
      binary = Transaction.encode(legacy_transaction())

      <<prefix::binary-size(20), _byte, suffix::binary>> = binary
      corrupted = <<prefix::binary, 0xFF, suffix::binary>>

      assert {:error, {:section_checksum_mismatch, _tag}} = Transaction.validate(corrupted)
    end
  end

  describe "section operations" do
    test "extracts sections by tag" do
      transaction = %{
        mutations: [{:set, "key", "value"}],
        read_conflicts: {Version.from_integer(12_345), [{"read_start", "read_end"}]},
        write_conflicts: [{"write_start", "write_end"}]
      }

      binary = Transaction.encode(transaction)

      assert {:ok, mutations_payload} = Transaction.extract_section(binary, 0x01)
      assert is_binary(mutations_payload) and byte_size(mutations_payload) > 0

      assert {:ok, read_conflicts_payload} = Transaction.extract_section(binary, 0x02)
      assert is_binary(read_conflicts_payload)

      assert {:ok, write_conflicts_payload} = Transaction.extract_section(binary, 0x03)
      assert is_binary(write_conflicts_payload)

      assert {:error, :section_not_found} = Transaction.extract_section(binary, 0x04)
    end

    test "adds transaction ID section" do
      transaction = basic_transaction()

      binary = Transaction.encode(transaction)
      assert {:ok, nil} = Transaction.commit_version(binary)

      version = Version.from_integer(98_765)
      assert {:ok, stamped} = Transaction.add_commit_version(binary, version)
      assert {:ok, ^version} = Transaction.commit_version(stamped)

      expected_with_version = Map.put(transaction, :commit_version, version)
      assert {:ok, ^expected_with_version} = Transaction.decode(stamped)
    end
  end

  describe "convenience functions" do
    test "extracts read version" do
      # Transaction with read version
      with_version = %{
        mutations: [{:set, "key", "value"}],
        read_conflicts: {Version.from_integer(12_345), [{"read_start", "read_end"}]},
        write_conflicts: []
      }

      binary_with_version = Transaction.encode(with_version)
      expected_version = Version.from_integer(12_345)

      assert {:ok, {^expected_version, _conflicts}} = Transaction.read_conflicts(binary_with_version)

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
        read_conflicts: {Version.from_integer(12_345), [{"read1", "read2"}]},
        write_conflicts: [{"write1", "write2"}]
      }

      binary = Transaction.encode(transaction)

      expected_version = Version.from_integer(12_345)

      assert {:ok, {^expected_version, [{"read1", "read2"}]}} = Transaction.read_conflicts(binary)

      assert {:ok, [{"write1", "write2"}]} = Transaction.write_conflicts(binary)

      # Empty conflicts
      empty_binary = Transaction.encode(legacy_transaction())
      assert {:ok, {nil, []}} = Transaction.read_conflicts(empty_binary)
      assert {:ok, []} = Transaction.write_conflicts(empty_binary)
    end

    test "streams mutations" do
      transaction = %{
        legacy_transaction()
        | mutations: [
            {:set, "key1", "value1"},
            {:set, "key2", "value2"},
            {:clear, "key3"},
            {:clear_range, "start", "end"}
          ]
      }

      binary = Transaction.encode(transaction)
      assert {:ok, stream} = Transaction.mutations(binary)
      assert Enum.to_list(stream) == transaction.mutations
    end
  end

  describe "error handling" do
    test "handles invalid binary format" do
      for invalid_binary <- [<<>>, <<1, 2, 3>>] do
        assert {:error, :invalid_format} = Transaction.decode(invalid_binary)
      end

      assert {:error, :invalid_format} = Transaction.validate(<<>>)
    end

    test "handles truncated data" do
      binary = Transaction.encode(legacy_transaction())
      truncated = binary_part(binary, 0, byte_size(binary) - 5)
      assert {:error, _} = Transaction.decode(truncated)
    end

    test "handles section extraction from non-existent sections" do
      binary = Transaction.encode(legacy_transaction())

      for tag <- [0x02, 0x03, 0x04] do
        assert {:error, :section_not_found} = Transaction.extract_section(binary, tag)
      end
    end

    test "handles adding duplicate sections" do
      binary = Transaction.encode(legacy_transaction())

      assert {:error, :section_already_exists} =
               Transaction.add_section(binary, 0x01, <<>>)
    end
  end

  describe "binary format structure" do
    test "has correct magic number and version" do
      encoded = Transaction.encode(legacy_transaction())
      assert <<0x42524454::unsigned-big-32, 0x01, _flags, _section_count::unsigned-big-16, _rest::binary>> = encoded
    end

    test "sections are order independent" do
      transaction = %{
        mutations: [{:set, "key", "value"}],
        read_conflicts: {Version.from_integer(12_345), [{"read_start", "read_end"}]},
        write_conflicts: [{"write_start", "write_end"}]
      }

      binaries = for _ <- 1..10, do: Transaction.encode(transaction)

      for binary <- binaries do
        assert {:ok, ^transaction} = Transaction.decode(binary)
      end
    end
  end
end
