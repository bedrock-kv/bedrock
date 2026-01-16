defmodule Bedrock.ObjectStorage.ChunkTest do
  use ExUnit.Case, async: true

  alias Bedrock.ObjectStorage.Chunk

  describe "encode/1" do
    test "encodes single transaction" do
      transactions = [{100, "transaction data"}]
      assert {:ok, binary} = Chunk.encode(transactions)
      assert is_binary(binary)
    end

    test "encodes multiple transactions" do
      transactions = [
        {100, "txn 1"},
        {200, "txn 2"},
        {300, "txn 3"}
      ]

      assert {:ok, binary} = Chunk.encode(transactions)
      assert is_binary(binary)
    end

    test "returns error for empty list" do
      assert {:error, :empty_chunk} = Chunk.encode([])
    end

    test "handles binary transaction data" do
      data = <<0, 1, 2, 255, 254, 253>>
      transactions = [{100, data}]
      assert {:ok, _binary} = Chunk.encode(transactions)
    end
  end

  describe "decode_header/1" do
    test "decodes valid header" do
      transactions = [{100, "data"}, {200, "more data"}]
      {:ok, binary} = Chunk.encode(transactions)

      header_binary = binary_part(binary, 0, Chunk.header_size())
      assert {:ok, header} = Chunk.decode_header(header_binary)

      assert header.min_version == 100
      assert header.max_version == 200
      assert header.txn_count == 2
      assert header.format_version == 1
    end

    test "returns error for invalid magic number" do
      invalid = <<0x00, 0x00, 0x00, 0x00, 0::size(224)>>
      assert {:error, {:invalid_magic, 0}} = Chunk.decode_header(invalid)
    end

    test "returns error for truncated header" do
      assert {:error, :invalid_header} = Chunk.decode_header(<<1, 2, 3>>)
    end
  end

  describe "decode/1" do
    test "round-trips single transaction" do
      transactions = [{100, "hello world"}]
      {:ok, binary} = Chunk.encode(transactions)
      {:ok, chunk} = Chunk.decode(binary)

      assert chunk.header.min_version == 100
      assert chunk.header.max_version == 100
      assert chunk.header.txn_count == 1
      assert length(chunk.directory) == 1
    end

    test "round-trips multiple transactions" do
      transactions = [
        {100, "txn 1"},
        {200, "txn 2 longer"},
        {300, "t3"}
      ]

      {:ok, binary} = Chunk.encode(transactions)
      {:ok, chunk} = Chunk.decode(binary)

      assert chunk.header.min_version == 100
      assert chunk.header.max_version == 300
      assert chunk.header.txn_count == 3

      # Verify directory entries
      assert length(chunk.directory) == 3
      [e1, e2, e3] = chunk.directory
      assert e1.version == 100
      assert e2.version == 200
      assert e3.version == 300
    end

    test "returns error for truncated chunk" do
      assert {:error, :truncated_chunk} = Chunk.decode(<<1, 2, 3>>)
    end
  end

  describe "extract_transactions/1" do
    test "extracts all transactions" do
      original = [
        {100, "txn 1"},
        {200, "txn 2 longer"},
        {300, "t3"}
      ]

      {:ok, binary} = Chunk.encode(original)
      {:ok, chunk} = Chunk.decode(binary)

      extracted = Chunk.extract_transactions(chunk)
      assert extracted == original
    end

    test "preserves binary data exactly" do
      data = <<0, 1, 2, 255, 254, 253, 128>>
      original = [{12_345, data}]

      {:ok, binary} = Chunk.encode(original)
      {:ok, chunk} = Chunk.decode(binary)

      [{version, extracted_data}] = Chunk.extract_transactions(chunk)
      assert version == 12_345
      assert extracted_data == data
    end
  end

  describe "find_entry/2" do
    test "finds exact version" do
      transactions = [{100, "a"}, {200, "b"}, {300, "c"}]
      {:ok, binary} = Chunk.encode(transactions)
      {:ok, chunk} = Chunk.decode(binary)

      entry = Chunk.find_entry(chunk.directory, 200)
      assert entry.version == 200
    end

    test "returns nil for missing version" do
      transactions = [{100, "a"}, {200, "b"}, {300, "c"}]
      {:ok, binary} = Chunk.encode(transactions)
      {:ok, chunk} = Chunk.decode(binary)

      assert nil == Chunk.find_entry(chunk.directory, 150)
    end
  end

  describe "find_first_entry_gte/2" do
    test "finds exact match" do
      transactions = [{100, "a"}, {200, "b"}, {300, "c"}]
      {:ok, binary} = Chunk.encode(transactions)
      {:ok, chunk} = Chunk.decode(binary)

      {index, entry} = Chunk.find_first_entry_gte(chunk.directory, 200)
      assert index == 1
      assert entry.version == 200
    end

    test "finds first entry greater than target" do
      transactions = [{100, "a"}, {200, "b"}, {300, "c"}]
      {:ok, binary} = Chunk.encode(transactions)
      {:ok, chunk} = Chunk.decode(binary)

      {index, entry} = Chunk.find_first_entry_gte(chunk.directory, 150)
      assert index == 1
      assert entry.version == 200
    end

    test "finds first entry when target is below all" do
      transactions = [{100, "a"}, {200, "b"}, {300, "c"}]
      {:ok, binary} = Chunk.encode(transactions)
      {:ok, chunk} = Chunk.decode(binary)

      {index, entry} = Chunk.find_first_entry_gte(chunk.directory, 50)
      assert index == 0
      assert entry.version == 100
    end

    test "returns nil when target is above all" do
      transactions = [{100, "a"}, {200, "b"}, {300, "c"}]
      {:ok, binary} = Chunk.encode(transactions)
      {:ok, chunk} = Chunk.decode(binary)

      assert nil == Chunk.find_first_entry_gte(chunk.directory, 400)
    end
  end

  describe "byte_range_from_version/2" do
    test "calculates byte range for middle entry" do
      transactions = [
        {100, String.duplicate("a", 100)},
        {200, String.duplicate("b", 200)},
        {300, String.duplicate("c", 300)}
      ]

      {:ok, binary} = Chunk.encode(transactions)
      {:ok, chunk} = Chunk.decode(binary)

      {:ok, {start_byte, end_byte}} = Chunk.byte_range_from_version(chunk, 200)

      # Verify we can extract the correct data using this range
      data_from_range = binary_part(binary, start_byte, end_byte - start_byte + 1)

      # Should contain txn 2 and txn 3 data
      assert byte_size(data_from_range) == 200 + 300
    end

    test "returns error for version not found" do
      transactions = [{100, "a"}, {200, "b"}]
      {:ok, binary} = Chunk.encode(transactions)
      {:ok, chunk} = Chunk.decode(binary)

      assert {:error, :version_not_found} = Chunk.byte_range_from_version(chunk, 500)
    end
  end

  describe "header and directory sizes" do
    test "header_size is 32 bytes" do
      assert Chunk.header_size() == 32
    end

    test "directory_entry_size is 16 bytes" do
      assert Chunk.directory_entry_size() == 16
    end

    test "encoded chunk has correct structure sizes" do
      transactions = [{100, "data"}, {200, "more"}, {300, "stuff"}]
      {:ok, binary} = Chunk.encode(transactions)
      {:ok, chunk} = Chunk.decode(binary)

      expected_dir_size = 3 * Chunk.directory_entry_size()
      assert chunk.header.directory_size == expected_dir_size

      # Total size = header + directory + data
      data_size = String.length("data") + String.length("more") + String.length("stuff")
      expected_total = Chunk.header_size() + expected_dir_size + data_size
      assert byte_size(binary) == expected_total
    end
  end
end
