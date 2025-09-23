# Run with: mix run benchmarks/olivine_index_update_bench.exs

defmodule Benchmarks.OlivineIndexUpdateBench do
  @moduledoc """
  Benchee micro benchmark for the olivine index_update module.

  This benchmark creates 10,000 transactions with ~200 keys each in setup and measures
  the time to apply them all to the index with mocked database operations
  to isolate just the page/index manipulation.
  """

  alias Bedrock.DataPlane.Storage.Olivine.Index
  alias Bedrock.DataPlane.Storage.Olivine.IndexUpdate
  alias Bedrock.DataPlane.Storage.Olivine.PageAllocator
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version
  alias Benchee.Formatters.Console

  defmodule MockDatabase do
    @moduledoc """
    Mock database implementation that avoids actual I/O operations
    for isolated benchmarking of index manipulation.

    This matches the structure of the real Database module but uses
    in-memory ETS tables instead of DETS for performance.
    """

    defstruct [
      :dets_storage,
      :window_size_in_microseconds,
      :buffer,
      :durable_version
    ]

    def new do
      %__MODULE__{
        dets_storage: :ets.new(:mock_dets, [:set, :public]),
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

  defp create_transaction_with_keys(base_index, key_count, commit_version \\ nil) do
    mutations =
      for i <- base_index..(base_index + key_count - 1) do
        {:set, generate_key(i), generate_value(i)}
      end

    # Create a properly encoded transaction with a real commit version
    version = commit_version || Version.from_integer(System.os_time(:microsecond) + base_index)

    transaction_map = %{
      commit_version: version,
      mutations: mutations
    }

    Transaction.encode(transaction_map)
  end

  defp setup_benchmark_data do
    # Create initial empty index
    index = Index.new()

    page_allocator = PageAllocator.new(0, [])
    version = Version.zero()
    database = MockDatabase.new()

    # Create 10,000 transactions, each with ~200 keys
    transactions =
      for i <- 0..9999 do
        create_transaction_with_keys(i * 200, 200)
      end

    %{
      index: index,
      page_allocator: page_allocator,
      version: version,
      database: database,
      transactions: transactions
    }
  end

  defp apply_all_transactions(setup_data) do
    %{index: index, page_allocator: page_allocator, version: version, database: database, transactions: transactions} =
      setup_data

    # Apply all transactions to the index
    Enum.reduce(transactions, {index, page_allocator}, fn encoded_transaction,
                                                          {current_index, current_page_allocator} ->
      {:ok, transaction} = Transaction.decode(encoded_transaction)

      # Create an IndexUpdate
      index_update = IndexUpdate.new(current_index, version, current_page_allocator)

      # Apply all mutations from the transaction
      updated_index_update = IndexUpdate.apply_mutations(index_update, transaction.mutations, database)

      # Process pending operations
      processed_index_update = IndexUpdate.process_pending_operations(updated_index_update)

      # Finish and get the updated index and page allocator
      IndexUpdate.finish(processed_index_update)
    end)
  end

  defp apply_single_transaction(setup_data, transaction_index) do
    %{index: index, page_allocator: page_allocator, version: version, database: database, transactions: transactions} =
      setup_data

    encoded_transaction = Enum.at(transactions, transaction_index)
    {:ok, transaction} = Transaction.decode(encoded_transaction)

    # Create an IndexUpdate
    index_update = IndexUpdate.new(index, version, page_allocator)

    # Apply all mutations from the transaction
    updated_index_update = IndexUpdate.apply_mutations(index_update, transaction.mutations, database)

    # Process pending operations
    processed_index_update = IndexUpdate.process_pending_operations(updated_index_update)

    # Finish and get the updated index and page allocator
    IndexUpdate.finish(processed_index_update)
  end

  defp setup_single_transaction_data do
    # Create initial empty index with some pre-existing data to simulate real conditions
    index = Index.new()
    page_allocator = PageAllocator.new(0, [])
    version = Version.zero()
    database = MockDatabase.new()

    # Create just one 200-key transaction
    transaction = create_transaction_with_keys(0, 200)

    %{
      index: index,
      page_allocator: page_allocator,
      version: version,
      database: database,
      transactions: [transaction]
    }
  end

  def run do
    # First run just the single transaction test to isolate per-transaction performance
    IO.puts("=== Testing single 200-key transaction performance ===")

    Benchee.run(
      %{
        "single_200key_transaction" => fn setup_data ->
          apply_single_transaction(setup_data, 0)
        end
      },
      before_each: fn _input -> setup_single_transaction_data() end,
      time: 5,
      memory_time: 2,
      formatters: [Console]
    )

    # Then test a smaller batch to see how it scales
    IO.puts("\n=== Testing 100 transactions (20k keys total) ===")

    small_batch_setup = fn ->
      index = Index.new()
      page_allocator = PageAllocator.new(0, [])
      version = Version.zero()
      database = MockDatabase.new()

      # Create 100 transactions, each with 200 keys
      transactions =
        for i <- 0..99 do
          create_transaction_with_keys(i * 200, 200)
        end

      %{
        index: index,
        page_allocator: page_allocator,
        version: version,
        database: database,
        transactions: transactions
      }
    end

    Benchee.run(
      %{
        "apply_100_200key_transactions" => fn setup_data ->
          apply_all_transactions(setup_data)
        end
      },
      before_each: fn _input -> small_batch_setup.() end,
      time: 10,
      memory_time: 2,
      formatters: [Console]
    )
  end
end

# Run the benchmark
IO.puts("Starting Olivine IndexUpdate benchmark...")
IO.puts("This will create 10,000 transactions with 200 keys each (2,000,000 total keys)")
IO.puts("and measure the time to apply them all to the index.\n")

Benchmarks.OlivineIndexUpdateBench.run()
