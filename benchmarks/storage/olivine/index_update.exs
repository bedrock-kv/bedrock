# Run with: mix run benchmarks/storage/olivine/index_update.exs

defmodule Benchmarks.OlivineIndexUpdateBench do
  @moduledoc """
  Benchee micro benchmark for the olivine index_update module.

  This benchmark isolates pure index operations by pre-processing all data
  and measuring only the core IndexUpdate operations without transaction
  decoding or other overhead.
  """

  alias Bedrock.DataPlane.Storage.Olivine.IdAllocator
  alias Bedrock.DataPlane.Storage.Olivine.Index
  alias Bedrock.DataPlane.Storage.Olivine.IndexUpdate
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version
  alias Benchee.Formatters.Console

  defmodule MockDatabase do
    @moduledoc """
    Mock database implementation that avoids actual I/O operations
    for isolated benchmarking of index manipulation.
    """

    defstruct [
      :dets_storage,
      :data_file,
      :data_file_offset,
      :data_file_name,
      :window_size_in_microseconds,
      :buffer,
      :durable_version
    ]

    def new do
      %__MODULE__{
        dets_storage: :ets.new(:mock_dets, [:set, :public]),
        data_file: nil,
        data_file_offset: 0,
        data_file_name: nil,
        window_size_in_microseconds: 5_000_000,
        buffer: :ets.new(:mock_buffer, [:ordered_set, :public, {:read_concurrency, true}]),
        durable_version: Version.zero()
      }
    end

    def store_value(database, key, version, value) do
      :ets.insert(database.buffer, {{version, key}, value})
      :ok
    end

    def load_value(database, key, version) do
      if version > database.durable_version do
        case :ets.lookup(database.buffer, {version, key}) do
          [{_key_version, value}] -> {:ok, value}
          [] -> {:error, :not_found}
        end
      else
        case :ets.lookup(database.dets_storage, key) do
          [{^key, value}] -> {:ok, value}
          [] -> {:error, :not_found}
        end
      end
    end

    def store_modified_pages(_database, _version, _pages), do: :ok
  end

  defp generate_key(i) do
    # Generate keys that will distribute across pages
    # Use zero-padded integers to ensure proper ordering
    key_base = i |> Integer.to_string() |> String.pad_leading(10, "0")
    "key_#{key_base}"
  end

  defp generate_value(i) do
    # Generate reasonably sized values
    "value_#{i}_" <> String.duplicate("x", 100)
  end

  # Pre-process mutations for clean benchmarking
  defp create_mutations(_base_index, 0), do: []

  defp create_mutations(base_index, key_count) when key_count > 0 do
    for i <- base_index..(base_index + key_count - 1) do
      {:set, generate_key(i), generate_value(i)}
    end
  end

  # Core index operation - what we actually want to benchmark
  defp apply_mutations_to_index(index, id_allocator, database, mutations, version) do
    index_update = IndexUpdate.new(index, version, id_allocator, database)

    updated_index_update =
      index_update
      |> IndexUpdate.apply_mutations(mutations)
      |> IndexUpdate.process_pending_operations()

    {updated_index, _database, updated_id_allocator, stats} = IndexUpdate.finish(updated_index_update)
    {{updated_index, updated_id_allocator}, stats}
  end

  # Apply multiple batches of mutations sequentially
  defp apply_mutation_batches(setup_data) do
    %{
      index: initial_index,
      id_allocator: initial_id_allocator,
      database: database,
      mutation_batches: mutation_batches,
      base_version: base_version
    } = setup_data

    {final_state, _all_stats} =
      mutation_batches
      |> Enum.with_index()
      |> Enum.reduce({{initial_index, initial_id_allocator}, []}, fn {mutations, idx},
                                                                     {{current_index, current_id_allocator}, stats_acc} ->
        version =
          if idx == 0 do
            base_version
          else
            Enum.reduce(1..idx, base_version, fn _i, v -> Version.increment(v) end)
          end

        {updated_state, batch_stats} =
          apply_mutations_to_index(current_index, current_id_allocator, database, mutations, version)

        {updated_state, [batch_stats | stats_acc]}
      end)

    final_state
  end

  # Setup for single mutation batch benchmark
  defp setup_single_batch_data do
    index = Index.new()
    id_allocator = IdAllocator.new(0, [])
    database = MockDatabase.new()
    mutations = create_mutations(0, 200)
    base_version = Version.zero()

    %{
      index: index,
      id_allocator: id_allocator,
      database: database,
      mutations: mutations,
      base_version: base_version
    }
  end

  # Setup for multiple batches benchmark - pre-process all data
  defp setup_multiple_batches_data(num_batches, keys_per_batch) do
    index = Index.new()
    id_allocator = IdAllocator.new(0, [])
    database = MockDatabase.new()
    base_version = Version.zero()

    # Pre-create all mutation batches to avoid measuring data generation
    mutation_batches =
      for i <- 0..(num_batches - 1) do
        create_mutations(i * keys_per_batch, keys_per_batch)
      end

    %{
      index: index,
      id_allocator: id_allocator,
      database: database,
      mutation_batches: mutation_batches,
      base_version: base_version
    }
  end

  # Setup for pre-populated index (to test performance on existing data)
  defp setup_prepopulated_index_data(prepopulate_keys, new_batch_keys) do
    index = Index.new()
    id_allocator = IdAllocator.new(0, [])
    database = MockDatabase.new()
    base_version = Version.zero()

    # Pre-populate the index
    prepopulate_mutations = create_mutations(0, prepopulate_keys)

    {{populated_index, populated_id_allocator}, _stats} =
      apply_mutations_to_index(index, id_allocator, database, prepopulate_mutations, base_version)

    # Create new batch to insert
    new_mutations = create_mutations(prepopulate_keys, new_batch_keys)

    %{
      index: populated_index,
      id_allocator: populated_id_allocator,
      database: database,
      mutations: new_mutations,
      base_version: Version.increment(base_version)
    }
  end

  def run do
    IO.puts("=== Single Batch Performance (200 keys on empty index) ===")

    Benchee.run(
      %{
        "single_200key_batch" => fn setup_data ->
          %{index: index, id_allocator: id_allocator, database: database, mutations: mutations, base_version: version} =
            setup_data

          apply_mutations_to_index(index, id_allocator, database, mutations, version)
        end
      },
      before_each: fn _input -> setup_single_batch_data() end,
      time: 5,
      memory_time: 2,
      formatters: [Console]
    )

    IO.puts("\n=== Multiple Batches Performance (10 batches × 200 keys) ===")

    Benchee.run(
      %{
        "apply_10_batches_200keys" => fn setup_data ->
          apply_mutation_batches(setup_data)
        end
      },
      before_each: fn _input -> setup_multiple_batches_data(10, 200) end,
      time: 5,
      memory_time: 2,
      formatters: [Console]
    )

    IO.puts("\n=== Scaling Test (different batch counts) ===")

    Benchee.run(
      %{
        "1_batch_200keys" => fn setup_data ->
          apply_mutation_batches(setup_data)
        end,
        "5_batches_200keys" => fn setup_data ->
          apply_mutation_batches(setup_data)
        end,
        "20_batches_200keys" => fn setup_data ->
          apply_mutation_batches(setup_data)
        end
      },
      before_each: fn input ->
        case input do
          "1_batch_200keys" -> setup_multiple_batches_data(1, 200)
          "5_batches_200keys" -> setup_multiple_batches_data(5, 200)
          "20_batches_200keys" -> setup_multiple_batches_data(20, 200)
        end
      end,
      inputs: %{
        "1_batch_200keys" => "1_batch_200keys",
        "5_batches_200keys" => "5_batches_200keys",
        "20_batches_200keys" => "20_batches_200keys"
      },
      time: 3,
      memory_time: 2,
      formatters: [Console]
    )

    IO.puts("\n=== Performance on Pre-populated Index ===")

    Benchee.run(
      %{
        "insert_into_empty_index" => fn setup_data ->
          %{index: index, id_allocator: id_allocator, database: database, mutations: mutations, base_version: version} =
            setup_data

          apply_mutations_to_index(index, id_allocator, database, mutations, version)
        end,
        "insert_into_1k_key_index" => fn setup_data ->
          %{index: index, id_allocator: id_allocator, database: database, mutations: mutations, base_version: version} =
            setup_data

          apply_mutations_to_index(index, id_allocator, database, mutations, version)
        end
      },
      before_each: fn input ->
        case input do
          "insert_into_empty_index" -> setup_prepopulated_index_data(0, 200)
          "insert_into_1k_key_index" -> setup_prepopulated_index_data(1000, 200)
        end
      end,
      inputs: %{
        "insert_into_empty_index" => "insert_into_empty_index",
        "insert_into_1k_key_index" => "insert_into_1k_key_index"
      },
      time: 5,
      memory_time: 2,
      formatters: [Console]
    )
  end
end

# Run the benchmark
IO.puts("Starting Olivine IndexUpdate benchmark...")
IO.puts("This benchmark isolates pure index operations from transaction processing overhead.")
IO.puts("All data is pre-processed to measure only IndexUpdate performance.\n")

Benchmarks.OlivineIndexUpdateBench.run()
