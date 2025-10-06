# Run with: mix run benchmarks/storage/olivine/page_splitting.exs

defmodule Benchmarks.OlivinePageSplittingBench do
  @moduledoc """
  Benchmark focused on page splitting performance during large insertions.

  This benchmark isolates the encode/decode overhead that occurs when
  mutations cause pages to overflow and require splitting. By inserting
  large batches of keys in a single operation, we force page splits and
  measure the cost of:

  1. Building oversized page binary (encode)
  2. Decoding that binary to extract key-locators
  3. Re-encoding split pages

  Expected behavior:
  - 200 keys: No split (baseline)
  - 300 keys: 2 pages (minimal split)
  - 500 keys: 2-3 pages (typical split)
  - 1000 keys: 4-5 pages (heavy split)
  - 2000 keys: 8-9 pages (extreme split)
  """

  alias Bedrock.DataPlane.Storage.Olivine.IdAllocator
  alias Bedrock.DataPlane.Storage.Olivine.Index
  alias Bedrock.DataPlane.Storage.Olivine.IndexUpdate
  alias Bedrock.DataPlane.Version
  alias Benchee.Formatters.Console

  defmodule MockDatabase do
    @moduledoc """
    Mock database implementation that avoids actual I/O operations
    for isolated benchmarking of index manipulation.
    """

    defmodule MockDataDatabase do
      @moduledoc false
      defstruct [
        :file,
        :file_offset,
        :file_name,
        :window_size_in_microseconds,
        :buffer
      ]

      def new do
        %__MODULE__{
          file: nil,
          file_offset: 0,
          file_name: nil,
          window_size_in_microseconds: 5_000_000,
          buffer: :ets.new(:mock_buffer, [:ordered_set, :public, {:read_concurrency, true}])
        }
      end

      def store_value(data_db, _key, _version, value) do
        offset = data_db.file_offset
        size = byte_size(value)
        locator = <<offset::47, size::17>>
        :ets.insert(data_db.buffer, {locator, value})
        {:ok, locator, %{data_db | file_offset: offset + size}}
      end

      def load_value(data_db, locator) do
        case locator do
          <<_offset::47, 0::17>> ->
            {:ok, <<>>}

          <<_offset::47, _size::17>> = locator ->
            case :ets.lookup(data_db.buffer, locator) do
              [{^locator, value}] -> {:ok, value}
              [] -> {:error, :not_found}
            end
        end
      end
    end

    defmodule MockIndexDatabase do
      @moduledoc false
      defstruct [
        :dets_storage,
        :durable_version
      ]

      def new do
        %__MODULE__{
          dets_storage: :ets.new(:mock_dets, [:set, :public]),
          durable_version: Version.zero()
        }
      end

      def store_page(index_db, page_id, page_tuple) do
        :ets.insert(index_db.dets_storage, {page_id, page_tuple})
        :ok
      end

      def load_page(index_db, page_id) do
        case :ets.lookup(index_db.dets_storage, page_id) do
          [{^page_id, page_tuple}] -> {:ok, page_tuple}
          [] -> {:error, :not_found}
        end
      end

      def durable_version(index_db), do: index_db.durable_version
    end

    def new do
      data_db = MockDataDatabase.new()
      index_db = MockIndexDatabase.new()
      {data_db, index_db}
    end

    def store_value({data_db, index_db}, key, version, value) do
      {:ok, locator, updated_data_db} = MockDataDatabase.store_value(data_db, key, version, value)
      {:ok, locator, {updated_data_db, index_db}}
    end

    def load_value({data_db, _index_db}, locator) do
      MockDataDatabase.load_value(data_db, locator)
    end

    def store_page({_data_db, index_db}, page_id, page_tuple) do
      MockIndexDatabase.store_page(index_db, page_id, page_tuple)
    end

    def load_page({_data_db, index_db}, page_id) do
      MockIndexDatabase.load_page(index_db, page_id)
    end

    def durable_version({_data_db, index_db}) do
      MockIndexDatabase.durable_version(index_db)
    end
  end

  defp generate_key(i) do
    # Generate keys that will distribute to same page initially
    # Use zero-padded integers to ensure proper ordering
    key_base = i |> Integer.to_string() |> String.pad_leading(10, "0")
    "key_#{key_base}"
  end

  defp generate_value(i) do
    # Generate reasonably sized values
    "value_#{i}_" <> String.duplicate("x", 100)
  end

  # Create large batch of mutations - all inserts
  defp create_large_batch_mutations(base_index, key_count) do
    for i <- base_index..(base_index + key_count - 1) do
      {:set, generate_key(i), generate_value(i)}
    end
  end

  # Apply single large batch to empty index
  defp apply_large_batch(index, id_allocator, database, mutations, version) do
    index_update = IndexUpdate.new(index, version, id_allocator, database)

    updated_index_update =
      index_update
      |> IndexUpdate.apply_mutations(mutations)
      |> IndexUpdate.process_pending_operations()

    {updated_index, _database, updated_id_allocator, _modified_pages} = IndexUpdate.finish(updated_index_update)
    {updated_index, updated_id_allocator}
  end

  # Setup for large insertion benchmark
  defp setup_large_insertion(key_count) do
    index = Index.new()
    id_allocator = IdAllocator.new(0, [])
    database = MockDatabase.new()
    mutations = create_large_batch_mutations(0, key_count)
    base_version = Version.zero()

    %{
      index: index,
      id_allocator: id_allocator,
      database: database,
      mutations: mutations,
      version: base_version
    }
  end

  def run do
    "=" |> String.duplicate(80) |> IO.puts()
    IO.puts("Olivine Page Splitting Performance Benchmark")
    "=" |> String.duplicate(80) |> IO.puts()
    IO.puts("")
    IO.puts("This benchmark measures the cost of page splitting during large insertions.")
    IO.puts("All keys are inserted in a single batch to concentrate split operations.")
    IO.puts("")
    IO.puts("Expected page splits:")
    IO.puts("  - 200 keys:  0 splits (baseline)")
    IO.puts("  - 300 keys:  1 split → 2 pages")
    IO.puts("  - 500 keys:  2 splits → 2-3 pages")
    IO.puts("  - 1000 keys: 4 splits → 4-5 pages")
    IO.puts("  - 2000 keys: 8 splits → 8-9 pages")
    IO.puts("")

    Benchee.run(
      %{
        "large_batch_insertion" => fn %{
                                        index: index,
                                        id_allocator: id_allocator,
                                        database: database,
                                        mutations: mutations,
                                        version: version
                                      } ->
          apply_large_batch(index, id_allocator, database, mutations, version)
        end
      },
      before_each: fn key_count ->
        setup_large_insertion(key_count)
      end,
      inputs: %{
        "200 keys (no split)" => 200,
        "300 keys (minimal split)" => 300,
        "500 keys (typical split)" => 500,
        "1000 keys (heavy split)" => 1000,
        "2000 keys (extreme split)" => 2000
      },
      time: 5,
      memory_time: 2,
      formatters: [Console]
    )

    IO.puts("")
    "=" |> String.duplicate(80) |> IO.puts()
    IO.puts("Analysis")
    "=" |> String.duplicate(80) |> IO.puts()
    IO.puts("")
    IO.puts("Look for non-linear scaling as key count increases - this indicates")
    IO.puts("the encode/decode overhead from page splitting.")
    IO.puts("")
    IO.puts("After optimization, expect:")
    IO.puts("  - More linear time scaling")
    IO.puts("  - Reduced memory allocations")
    IO.puts("  - Larger improvements for bigger batches")
    IO.puts("")
  end
end

# Run the benchmark
Benchmarks.OlivinePageSplittingBench.run()
