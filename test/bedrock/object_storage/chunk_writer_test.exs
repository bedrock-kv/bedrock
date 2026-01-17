defmodule Bedrock.ObjectStorage.ChunkWriterTest do
  use ExUnit.Case, async: true

  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.Chunk
  alias Bedrock.ObjectStorage.ChunkWriter
  alias Bedrock.ObjectStorage.Keys
  alias Bedrock.ObjectStorage.LocalFilesystem

  setup do
    root = Path.join(System.tmp_dir!(), "chunk_writer_test_#{:erlang.unique_integer([:positive])}")
    File.mkdir_p!(root)

    on_exit(fn ->
      File.rm_rf!(root)
    end)

    backend = ObjectStorage.backend(LocalFilesystem, root: root)
    {:ok, backend: backend, root: root}
  end

  describe "new/4" do
    test "creates writer with default options", %{backend: backend} do
      {:ok, writer} = ChunkWriter.new(backend, "cluster", "shard-01")

      assert writer.cluster == "cluster"
      assert writer.shard_tag == "shard-01"
      assert writer.size_threshold == 64 * 1024 * 1024
      assert writer.time_gap_ms == 5 * 60 * 1000
    end

    test "creates writer with custom options", %{backend: backend} do
      {:ok, writer} =
        ChunkWriter.new(backend, "cluster", "shard",
          size_threshold: 1024,
          time_gap_ms: 1000
        )

      assert writer.size_threshold == 1024
      assert writer.time_gap_ms == 1000
    end
  end

  describe "add_transaction/3" do
    test "adds transaction to buffer", %{backend: backend} do
      {:ok, writer} = ChunkWriter.new(backend, "cluster", "shard")
      {:ok, writer} = ChunkWriter.add_transaction(writer, 100, "data")

      assert ChunkWriter.buffer_count(writer) == 1
      assert ChunkWriter.buffer_size(writer) == 4
    end

    test "accumulates multiple transactions", %{backend: backend} do
      {:ok, writer} = ChunkWriter.new(backend, "cluster", "shard")
      {:ok, writer} = ChunkWriter.add_transaction(writer, 100, "data1")
      {:ok, writer} = ChunkWriter.add_transaction(writer, 200, "data2")
      {:ok, writer} = ChunkWriter.add_transaction(writer, 300, "data3")

      assert ChunkWriter.buffer_count(writer) == 3
      assert ChunkWriter.buffer_size(writer) == 15
    end
  end

  describe "maybe_flush/1 - size threshold" do
    test "flushes when size threshold reached", %{backend: backend, root: root} do
      {:ok, writer} = ChunkWriter.new(backend, "cluster", "shard", size_threshold: 10)

      # Add transactions that exceed threshold
      {:ok, writer} = ChunkWriter.add_transaction(writer, 100, "12345")
      {:ok, writer} = ChunkWriter.add_transaction(writer, 200, "67890")

      # Buffer is now 10 bytes, at threshold
      assert ChunkWriter.buffer_size(writer) == 10

      {:ok, writer, status} = ChunkWriter.maybe_flush(writer)
      assert status == :flushed
      assert ChunkWriter.empty?(writer)
      assert ChunkWriter.chunks_written(writer) == 1

      # Verify chunk was written
      chunk_path = Path.join(root, Keys.chunk_path("cluster", "shard", 200))
      assert File.exists?(chunk_path)
    end

    test "does not flush below threshold", %{backend: backend} do
      {:ok, writer} = ChunkWriter.new(backend, "cluster", "shard", size_threshold: 100)
      {:ok, writer} = ChunkWriter.add_transaction(writer, 100, "small")

      {:ok, writer, status} = ChunkWriter.maybe_flush(writer)
      assert status == :not_needed
      refute ChunkWriter.empty?(writer)
    end
  end

  describe "maybe_flush/1 - time gap" do
    test "flushes when time gap exceeded", %{backend: backend} do
      {:ok, writer} = ChunkWriter.new(backend, "cluster", "shard", time_gap_ms: 1)
      {:ok, writer} = ChunkWriter.add_transaction(writer, 100, "data")

      # Wait for time gap
      Process.sleep(5)

      {:ok, writer, status} = ChunkWriter.maybe_flush(writer)
      assert status == :flushed
      assert ChunkWriter.empty?(writer)
    end

    test "does not flush before time gap", %{backend: backend} do
      {:ok, writer} = ChunkWriter.new(backend, "cluster", "shard", time_gap_ms: 60_000)
      {:ok, writer} = ChunkWriter.add_transaction(writer, 100, "data")

      {:ok, _writer, status} = ChunkWriter.maybe_flush(writer)
      assert status == :not_needed
    end
  end

  describe "maybe_flush/1 - empty buffer" do
    test "returns not_needed for empty buffer", %{backend: backend} do
      {:ok, writer} = ChunkWriter.new(backend, "cluster", "shard")
      {:ok, _writer, status} = ChunkWriter.maybe_flush(writer)
      assert status == :not_needed
    end
  end

  describe "flush/1" do
    test "forces flush even below threshold", %{backend: backend, root: root} do
      {:ok, writer} = ChunkWriter.new(backend, "cluster", "shard", size_threshold: 1_000_000)
      {:ok, writer} = ChunkWriter.add_transaction(writer, 100, "small data")

      {:ok, writer} = ChunkWriter.flush(writer)
      assert ChunkWriter.empty?(writer)
      assert ChunkWriter.chunks_written(writer) == 1

      # Verify chunk was written
      chunk_path = Path.join(root, Keys.chunk_path("cluster", "shard", 100))
      assert File.exists?(chunk_path)
    end

    test "does nothing for empty buffer", %{backend: backend} do
      {:ok, writer} = ChunkWriter.new(backend, "cluster", "shard")
      {:ok, writer} = ChunkWriter.flush(writer)
      assert ChunkWriter.chunks_written(writer) == 0
    end
  end

  describe "chunk format integration" do
    test "written chunks are valid and decodable", %{backend: backend} do
      {:ok, writer} = ChunkWriter.new(backend, "cluster", "shard", size_threshold: 10)

      {:ok, writer} = ChunkWriter.add_transaction(writer, 100, "txn1")
      {:ok, writer} = ChunkWriter.add_transaction(writer, 200, "txn2data")
      {:ok, writer} = ChunkWriter.add_transaction(writer, 300, "t3")

      {:ok, _writer} = ChunkWriter.flush(writer)

      # Read and decode the chunk
      key = Keys.chunk_path("cluster", "shard", 300)
      {:ok, chunk_binary} = ObjectStorage.get(backend, key)
      {:ok, chunk} = Chunk.decode(chunk_binary)

      # Verify chunk contents
      assert chunk.header.min_version == 100
      assert chunk.header.max_version == 300
      assert chunk.header.txn_count == 3

      transactions = Chunk.extract_transactions(chunk)
      assert transactions == [{100, "txn1"}, {200, "txn2data"}, {300, "t3"}]
    end
  end

  describe "idempotency" do
    test "duplicate flush succeeds (already_exists is ok)", %{backend: backend} do
      {:ok, writer1} = ChunkWriter.new(backend, "cluster", "shard")
      {:ok, writer1} = ChunkWriter.add_transaction(writer1, 100, "data")
      {:ok, _writer1} = ChunkWriter.flush(writer1)

      # Create another writer and try to write the same chunk
      {:ok, writer2} = ChunkWriter.new(backend, "cluster", "shard")
      {:ok, writer2} = ChunkWriter.add_transaction(writer2, 100, "data")
      {:ok, writer2} = ChunkWriter.flush(writer2)

      # Should succeed (idempotent)
      assert ChunkWriter.chunks_written(writer2) == 1
    end
  end

  describe "multiple flushes" do
    test "writes multiple chunks over time", %{backend: backend, root: root} do
      {:ok, writer} = ChunkWriter.new(backend, "cluster", "shard", size_threshold: 10)

      # First batch
      {:ok, writer} = ChunkWriter.add_transaction(writer, 100, "batch1_a")
      {:ok, writer} = ChunkWriter.add_transaction(writer, 200, "b1b")
      {:ok, writer, :flushed} = ChunkWriter.maybe_flush(writer)

      # Second batch
      {:ok, writer} = ChunkWriter.add_transaction(writer, 300, "batch2_x")
      {:ok, writer} = ChunkWriter.add_transaction(writer, 400, "b2y")
      {:ok, writer, :flushed} = ChunkWriter.maybe_flush(writer)

      assert ChunkWriter.chunks_written(writer) == 2

      # Both chunks should exist
      chunk1_path = Path.join(root, Keys.chunk_path("cluster", "shard", 200))
      chunk2_path = Path.join(root, Keys.chunk_path("cluster", "shard", 400))
      assert File.exists?(chunk1_path)
      assert File.exists?(chunk2_path)
    end
  end

  describe "helper functions" do
    test "buffer_size/1 returns current size", %{backend: backend} do
      {:ok, writer} = ChunkWriter.new(backend, "cluster", "shard")
      assert ChunkWriter.buffer_size(writer) == 0

      {:ok, writer} = ChunkWriter.add_transaction(writer, 100, "hello")
      assert ChunkWriter.buffer_size(writer) == 5
    end

    test "buffer_count/1 returns transaction count", %{backend: backend} do
      {:ok, writer} = ChunkWriter.new(backend, "cluster", "shard")
      assert ChunkWriter.buffer_count(writer) == 0

      {:ok, writer} = ChunkWriter.add_transaction(writer, 100, "a")
      {:ok, writer} = ChunkWriter.add_transaction(writer, 200, "b")
      assert ChunkWriter.buffer_count(writer) == 2
    end

    test "empty?/1 returns buffer status", %{backend: backend} do
      {:ok, writer} = ChunkWriter.new(backend, "cluster", "shard")
      assert ChunkWriter.empty?(writer)

      {:ok, writer} = ChunkWriter.add_transaction(writer, 100, "data")
      refute ChunkWriter.empty?(writer)
    end

    test "chunks_written/1 tracks flush count", %{backend: backend} do
      {:ok, writer} = ChunkWriter.new(backend, "cluster", "shard")
      assert ChunkWriter.chunks_written(writer) == 0

      {:ok, writer} = ChunkWriter.add_transaction(writer, 100, "data")
      {:ok, writer} = ChunkWriter.flush(writer)
      assert ChunkWriter.chunks_written(writer) == 1
    end
  end
end
