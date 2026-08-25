defmodule Bedrock.ObjectStorage.ChunkReaderTest do
  use ExUnit.Case, async: true

  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.Chunk
  alias Bedrock.ObjectStorage.ChunkReader
  alias Bedrock.ObjectStorage.Keys
  alias Bedrock.ObjectStorage.LocalFilesystem

  setup do
    root = Path.join(System.tmp_dir!(), "chunk_reader_test_#{:erlang.unique_integer([:positive])}")
    File.mkdir_p!(root)

    on_exit(fn ->
      File.rm_rf!(root)
    end)

    backend = ObjectStorage.backend(LocalFilesystem, root: root)
    {:ok, backend: backend, root: root}
  end

  # Helper to write a chunk directly
  defp write_chunk(backend, shard, transactions) do
    {:ok, chunk_binary} = Chunk.encode(transactions)
    {max_version, _} = List.last(transactions)
    key = Keys.chunk_path(shard, max_version)
    :ok = ObjectStorage.put(backend, key, chunk_binary)
    key
  end

  defp write_raw_chunk(backend, shard, max_version, data) do
    key = Keys.chunk_path(shard, max_version)
    :ok = ObjectStorage.put(backend, key, data)
    key
  end

  defmodule CountingBackend do
    @moduledoc false
    @behaviour ObjectStorage

    @impl true
    def get(config, key) do
      send(Keyword.fetch!(config, :notify), {:get, key})
      LocalFilesystem.get(config, key)
    end

    @impl true
    def put(config, key, data, opts \\ []), do: LocalFilesystem.put(config, key, data, opts)
    @impl true
    def delete(config, key), do: LocalFilesystem.delete(config, key)
    @impl true
    def list(config, prefix, opts \\ []), do: LocalFilesystem.list(config, prefix, opts)
    @impl true
    def put_if_not_exists(config, key, data, opts \\ []), do: LocalFilesystem.put_if_not_exists(config, key, data, opts)
    @impl true
    def get_with_version(config, key), do: LocalFilesystem.get_with_version(config, key)

    @impl true
    def put_if_version_matches(config, key, version_token, data, opts \\ []),
      do: LocalFilesystem.put_if_version_matches(config, key, version_token, data, opts)
  end

  describe "chunk selection from key names alone" do
    test "read_from_version fetches only the chunks it needs", %{backend: backend, root: root} do
      # Five chunks; a read from 450 needs only the last two (maxes 500, 700)
      write_chunk(backend, "s", [{100, "a"}, {150, "b"}])
      write_chunk(backend, "s", [{200, "c"}])
      write_chunk(backend, "s", [{300, "d"}, {350, "e"}])
      needed_1 = write_chunk(backend, "s", [{400, "f"}, {500, "g"}])
      needed_2 = write_chunk(backend, "s", [{600, "h"}, {700, "i"}])

      counting = ObjectStorage.backend(CountingBackend, root: root, notify: self())
      reader = ChunkReader.new(counting, "s")

      versions =
        reader
        |> ChunkReader.read_from_version(450)
        |> Enum.map(fn {v, _} -> v end)

      assert versions == [500, 600, 700]

      # The names alone decided which chunks to touch: exactly the two
      # needed, never the three below the target.
      fetched = collect_gets()
      assert Enum.sort(fetched) == Enum.sort([needed_1, needed_2])
    end

    test "find_chunk_for_version reads at most one chunk", %{backend: backend, root: root} do
      write_chunk(backend, "s", [{100, "a"}])
      covering = write_chunk(backend, "s", [{200, "b"}, {300, "c"}])
      write_chunk(backend, "s", [{400, "d"}])

      counting = ObjectStorage.backend(CountingBackend, root: root, notify: self())
      reader = ChunkReader.new(counting, "s")

      assert ChunkReader.find_chunk_for_version(reader, 250) == covering
      assert collect_gets() == [covering]

      # A version in the gap between chunks: the boundary chunk's min is
      # above it, so there is no containing chunk — one read to learn that.
      assert ChunkReader.find_chunk_for_version(reader, 150) == nil
      assert collect_gets() == [covering]
    end

    defp collect_gets(acc \\ []) do
      receive do
        {:get, key} -> collect_gets([key | acc])
      after
        0 -> Enum.reverse(acc)
      end
    end
  end

  describe "new/2" do
    test "creates reader", %{backend: backend} do
      reader = ChunkReader.new(backend, "shard")
      assert reader.shard_tag == "shard"
    end
  end

  describe "list_chunks/2" do
    test "returns empty for no chunks", %{backend: backend} do
      reader = ChunkReader.new(backend, "shard")
      assert [] == reader |> ChunkReader.list_chunks() |> Enum.to_list()
    end

    test "lists chunks in newest-first order", %{backend: backend} do
      # Write chunks with different max versions
      write_chunk(backend, "shard", [{100, "a"}])
      write_chunk(backend, "shard", [{200, "b"}])
      write_chunk(backend, "shard", [{300, "c"}])

      reader = ChunkReader.new(backend, "shard")
      keys = reader |> ChunkReader.list_chunks() |> Enum.to_list()

      # Should be newest first (300, 200, 100)
      versions =
        Enum.map(keys, fn key ->
          {:ok, v} = Keys.extract_version(key)
          v
        end)

      assert versions == [300, 200, 100]
    end

    test "respects limit option", %{backend: backend} do
      write_chunk(backend, "shard", [{100, "a"}])
      write_chunk(backend, "shard", [{200, "b"}])
      write_chunk(backend, "shard", [{300, "c"}])

      reader = ChunkReader.new(backend, "shard")
      keys = reader |> ChunkReader.list_chunks(limit: 2) |> Enum.to_list()

      assert length(keys) == 2
    end
  end

  describe "list_chunk_metadata/2" do
    test "returns version ranges", %{backend: backend} do
      write_chunk(backend, "shard", [{100, "a"}, {150, "b"}])
      write_chunk(backend, "shard", [{200, "c"}, {250, "d"}])

      reader = ChunkReader.new(backend, "shard")
      metadata = reader |> ChunkReader.list_chunk_metadata() |> Enum.to_list()

      # Newest first
      assert length(metadata) == 2
      {_, min1, max1} = Enum.at(metadata, 0)
      {_, min2, max2} = Enum.at(metadata, 1)

      assert {min1, max1} == {200, 250}
      assert {min2, max2} == {100, 150}
    end

    test "raises on corrupted chunk headers by default", %{backend: backend} do
      write_raw_chunk(backend, "shard", 200, <<1, 2, 3, 4>>)

      reader = ChunkReader.new(backend, "shard")

      assert_raise ChunkReader.ReadError, fn ->
        reader |> ChunkReader.list_chunk_metadata() |> Enum.to_list()
      end
    end

    test "can skip corrupted chunk headers with on_error: :skip", %{backend: backend} do
      write_chunk(backend, "shard", [{100, "ok"}])
      write_raw_chunk(backend, "shard", 200, <<1, 2, 3, 4>>)

      reader = ChunkReader.new(backend, "shard")
      metadata = reader |> ChunkReader.list_chunk_metadata(on_error: :skip) |> Enum.to_list()

      assert length(metadata) == 1
      {_, min_version, max_version} = hd(metadata)
      assert {min_version, max_version} == {100, 100}
    end
  end

  describe "read_chunk/2" do
    test "reads and decodes chunk", %{backend: backend} do
      transactions = [{100, "data1"}, {200, "data2"}]
      key = write_chunk(backend, "shard", transactions)

      reader = ChunkReader.new(backend, "shard")
      {:ok, chunk} = ChunkReader.read_chunk(reader, key)

      assert chunk.header.min_version == 100
      assert chunk.header.max_version == 200
      assert Chunk.extract_transactions(chunk) == transactions
    end

    test "returns error for missing chunk", %{backend: backend} do
      reader = ChunkReader.new(backend, "shard")
      assert {:error, :not_found} = ChunkReader.read_chunk(reader, "nonexistent")
    end
  end

  describe "read_chunk_header/2" do
    test "reads only header", %{backend: backend} do
      transactions = [{100, String.duplicate("x", 10_000)}]
      key = write_chunk(backend, "shard", transactions)

      reader = ChunkReader.new(backend, "shard")
      {:ok, header} = ChunkReader.read_chunk_header(reader, key)

      assert header.min_version == 100
      assert header.max_version == 100
      assert header.txn_count == 1
    end
  end

  describe "find_chunk_for_version/2" do
    test "finds chunk containing version", %{backend: backend} do
      write_chunk(backend, "shard", [{100, "a"}, {150, "b"}])
      write_chunk(backend, "shard", [{200, "c"}, {250, "d"}])

      reader = ChunkReader.new(backend, "shard")

      # Find in first chunk
      key1 = ChunkReader.find_chunk_for_version(reader, 125)
      {:ok, v1} = Keys.extract_version(key1)
      assert v1 == 150

      # Find in second chunk
      key2 = ChunkReader.find_chunk_for_version(reader, 225)
      {:ok, v2} = Keys.extract_version(key2)
      assert v2 == 250
    end

    test "returns nil when version not in any chunk", %{backend: backend} do
      write_chunk(backend, "shard", [{100, "a"}, {150, "b"}])

      reader = ChunkReader.new(backend, "shard")
      assert nil == ChunkReader.find_chunk_for_version(reader, 50)
      assert nil == ChunkReader.find_chunk_for_version(reader, 200)
    end
  end

  describe "read_from_version/3" do
    test "reads from exact version", %{backend: backend} do
      write_chunk(backend, "shard", [{100, "a"}, {200, "b"}, {300, "c"}])

      reader = ChunkReader.new(backend, "shard")
      transactions = reader |> ChunkReader.read_from_version(200) |> Enum.to_list()

      assert transactions == [{200, "b"}, {300, "c"}]
    end

    test "reads from version between transactions", %{backend: backend} do
      write_chunk(backend, "shard", [{100, "a"}, {200, "b"}, {300, "c"}])

      reader = ChunkReader.new(backend, "shard")
      transactions = reader |> ChunkReader.read_from_version(150) |> Enum.to_list()

      assert transactions == [{200, "b"}, {300, "c"}]
    end

    test "reads across multiple chunks", %{backend: backend} do
      write_chunk(backend, "shard", [{100, "a"}, {200, "b"}])
      write_chunk(backend, "shard", [{300, "c"}, {400, "d"}])

      reader = ChunkReader.new(backend, "shard")
      transactions = reader |> ChunkReader.read_from_version(150) |> Enum.to_list()

      assert transactions == [{200, "b"}, {300, "c"}, {400, "d"}]
    end

    test "returns empty when version beyond all chunks", %{backend: backend} do
      write_chunk(backend, "shard", [{100, "a"}, {200, "b"}])

      reader = ChunkReader.new(backend, "shard")
      transactions = reader |> ChunkReader.read_from_version(500) |> Enum.to_list()

      assert transactions == []
    end

    test "respects limit option", %{backend: backend} do
      write_chunk(backend, "shard", [{100, "a"}, {200, "b"}, {300, "c"}])

      reader = ChunkReader.new(backend, "shard")
      transactions = reader |> ChunkReader.read_from_version(100, limit: 2) |> Enum.to_list()

      assert transactions == [{100, "a"}, {200, "b"}]
    end

    test "is lazy - doesn't read all chunks upfront", %{backend: backend} do
      # Write many chunks
      for i <- 1..10 do
        write_chunk(backend, "shard", [{i * 100, "data#{i}"}])
      end

      reader = ChunkReader.new(backend, "shard")
      stream = ChunkReader.read_from_version(reader, 100)

      # Taking just first 2 should work
      transactions = Enum.take(stream, 2)
      assert length(transactions) == 2
    end

    test "raises when encountering corrupted chunk data", %{backend: backend} do
      write_chunk(backend, "shard", [{100, "a"}])
      write_raw_chunk(backend, "shard", 200, <<1, 2, 3, 4>>)

      reader = ChunkReader.new(backend, "shard")

      assert_raise ChunkReader.ReadError, fn ->
        reader |> ChunkReader.read_from_version(0) |> Enum.to_list()
      end
    end
  end

  describe "read_all_transactions/2" do
    test "reads all transactions in version order", %{backend: backend} do
      write_chunk(backend, "shard", [{100, "a"}, {200, "b"}])
      write_chunk(backend, "shard", [{300, "c"}, {400, "d"}])

      reader = ChunkReader.new(backend, "shard")
      transactions = reader |> ChunkReader.read_all_transactions() |> Enum.to_list()

      versions = Enum.map(transactions, fn {v, _} -> v end)
      assert versions == [100, 200, 300, 400]
    end

    test "returns empty for no chunks", %{backend: backend} do
      reader = ChunkReader.new(backend, "shard")
      assert [] == reader |> ChunkReader.read_all_transactions() |> Enum.to_list()
    end

    test "respects limit option", %{backend: backend} do
      write_chunk(backend, "shard", [{100, "a"}, {200, "b"}])
      write_chunk(backend, "shard", [{300, "c"}, {400, "d"}])

      reader = ChunkReader.new(backend, "shard")
      transactions = reader |> ChunkReader.read_all_transactions(limit: 3) |> Enum.to_list()

      assert length(transactions) == 3
    end
  end

  describe "latest_version/1" do
    test "returns highest version", %{backend: backend} do
      write_chunk(backend, "shard", [{100, "a"}])
      write_chunk(backend, "shard", [{500, "b"}])
      write_chunk(backend, "shard", [{300, "c"}])

      reader = ChunkReader.new(backend, "shard")
      assert 500 == ChunkReader.latest_version(reader)
    end

    test "returns nil for empty shard", %{backend: backend} do
      reader = ChunkReader.new(backend, "shard")
      assert nil == ChunkReader.latest_version(reader)
    end
  end

  describe "oldest_version/1" do
    test "returns lowest version", %{backend: backend} do
      write_chunk(backend, "shard", [{100, "a"}, {150, "b"}])
      write_chunk(backend, "shard", [{200, "c"}, {250, "d"}])

      reader = ChunkReader.new(backend, "shard")
      assert 100 == ChunkReader.oldest_version(reader)
    end

    test "returns nil for empty shard", %{backend: backend} do
      reader = ChunkReader.new(backend, "shard")
      assert nil == ChunkReader.oldest_version(reader)
    end
  end

  describe "replay workflow" do
    test "cold start playback scenario", %{backend: backend} do
      # Simulate demux writing chunks over time
      write_chunk(backend, "shard", [{1000, "txn1"}, {2000, "txn2"}])
      write_chunk(backend, "shard", [{3000, "txn3"}, {4000, "txn4"}])
      write_chunk(backend, "shard", [{5000, "txn5"}, {6000, "txn6"}])

      reader = ChunkReader.new(backend, "shard")

      # Materializer cold starts, wants to replay from version 2500
      transactions =
        reader
        |> ChunkReader.read_from_version(2500)
        |> Enum.to_list()

      # Should get txn3 onwards
      versions = Enum.map(transactions, fn {v, _} -> v end)
      assert versions == [3000, 4000, 5000, 6000]
    end
  end
end
