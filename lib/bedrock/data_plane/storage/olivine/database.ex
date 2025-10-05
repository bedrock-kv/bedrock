defmodule Bedrock.DataPlane.Storage.Olivine.Database do
  @moduledoc false

  alias Bedrock.DataPlane.Storage.Olivine.DataDatabase
  alias Bedrock.DataPlane.Storage.Olivine.Index.Page
  alias Bedrock.DataPlane.Storage.Olivine.IndexDatabase

  @type t :: {DataDatabase.t(), IndexDatabase.t()}
  @type locator :: DataDatabase.locator()

  @spec open(otp_name :: atom(), file_path :: String.t(), opts :: keyword()) ::
          {:ok, t()} | {:error, :system_limit | :badarg | File.posix()}
  def open(otp_name, file_path, opts \\ []) when is_atom(otp_name) and is_list(opts) do
    with {:ok, data_db} <- DataDatabase.open(file_path, opts),
         {:ok, index_db} <- IndexDatabase.open(otp_name, file_path) do
      {:ok, {data_db, index_db}}
    end
  end

  @spec close(t()) :: :ok
  def close({data_db, index_db}) do
    DataDatabase.close(data_db)
    IndexDatabase.close(index_db)
    :ok
  end

  @spec load_value(t(), locator()) :: {:ok, Bedrock.value()} | {:error, :not_found}
  def load_value({data_db, _index_db}, locator), do: DataDatabase.load_value(data_db, locator)

  @doc """
  Store a value in the lookaside buffer for the given version and key.
  This is used during transaction application for values within the window.
  """
  @spec store_value(t(), key :: Bedrock.key(), version :: Bedrock.version(), value :: Bedrock.value()) ::
          {:ok, locator(), database :: t()}
  def store_value({data_db, index_db}, key, version, value) do
    {:ok, locator, updated_data_db} = DataDatabase.store_value(data_db, key, version, value)
    {:ok, locator, {updated_data_db, index_db}}
  end

  @doc """
  Returns a value loader function that captures only the minimal data needed
  for async value resolution tasks. Avoids copying the entire Database struct.
  """
  @spec value_loader(t()) :: (locator() -> {:ok, Bedrock.value()} | {:error, :not_found} | {:error, :shutting_down})
  def value_loader({data_db, _index_db}), do: DataDatabase.value_loader(data_db)

  @spec many_value_loader(t()) ::
          ([locator()] ->
             {:ok, %{locator() => Bedrock.value()}}
             | {:error, :not_found}
             | {:error, :shutting_down})
  def many_value_loader({data_db, _index_db}), do: DataDatabase.many_value_loader(data_db)

  @spec durable_version(t()) :: Bedrock.version()
  def durable_version({_data_db, index_db}), do: IndexDatabase.durable_version(index_db)

  @doc """
  Load durable version directly from storage.
  This is useful for background processes that may have a stale database struct.
  """
  @spec load_current_durable_version(t()) ::
          {:ok, Bedrock.version()} | {:error, :not_found}
  def load_current_durable_version({_data_db, index_db}), do: IndexDatabase.load_durable_version(index_db)

  @spec info(t(), :n_keys | :utilization | :size_in_bytes | :key_ranges) :: any() | :undefined
  def info({_data_db, index_db}, stat), do: IndexDatabase.info(index_db, stat)

  @spec advance_durable_version(
          t(),
          version :: Bedrock.version(),
          previous_durable_version :: Bedrock.version(),
          data_size_in_bytes :: pos_integer(),
          collected_pages :: [%{Page.id() => {Page.t(), Page.id()}}]
        ) ::
          {:ok, t(), metadata :: map()} | {:error, term()}
  def advance_durable_version(
        {data_db, index_db},
        version,
        previous_durable_version,
        data_size_in_bytes,
        collected_pages
      ) do
    start_time = System.monotonic_time(:microsecond)

    {write_time_μs, updated_data_db} = :timer.tc(fn -> DataDatabase.flush(data_db, data_size_in_bytes) end)

    {insert_time_μs, updated_index_db} =
      :timer.tc(fn -> IndexDatabase.flush(index_db, version, previous_durable_version, collected_pages) end)

    total_duration_μs = System.monotonic_time(:microsecond) - start_time

    metadata = %{
      insert_time_μs: insert_time_μs,
      write_time_μs: write_time_μs,
      total_duration_μs: total_duration_μs
    }

    {:ok, {updated_data_db, updated_index_db}, metadata}
  end

  @doc """
  Compacts the database files by building new files with sequential data layout.

  Returns compacted file handles and the page_map built during compaction.
  The page_map can be used directly to construct in-memory structures without re-reading.

  This is run in a background task and should not block normal operations.
  """
  @spec compact(t(), complete_page_map :: %{Page.id() => {Page.t(), Page.id()}}) ::
          {:ok, compact_data_fd :: :file.fd(), compact_idx_fd :: :file.fd(), compact_data_path :: charlist(),
           compact_idx_path :: charlist(), new_data_offset :: non_neg_integer(),
           compacted_pages :: %{Page.id() => {Page.t(), Page.id()}}, durable_version :: Bedrock.version()}
          | {:error, term()}
  def compact({data_db, index_db}, complete_page_map) do
    durable_version = IndexDatabase.durable_version(index_db)

    # Use actual file paths from database structs
    data_file_path = data_db.file_name
    idx_file_path = index_db.file_name

    compact_data_path = data_file_path ++ ~c".compact"
    compact_idx_path = idx_file_path ++ ~c".compact"

    with {:ok, compact_data_fd} <- :file.open(compact_data_path, [:write, :raw, :binary]),
         {:ok, compact_idx_fd} <- :file.open(compact_idx_path, [:write, :raw, :binary]) do
      # Build compacted data file and updated pages
      {compacted_pages, final_offset} = build_compacted_data(data_db, complete_page_map, compact_data_fd)

      # Write snapshot index block
      :ok = IndexDatabase.write_snapshot_block(compact_idx_fd, durable_version, compacted_pages)

      # Sync files to disk
      :ok = :file.sync(compact_data_fd)
      :ok = :file.sync(compact_idx_fd)

      {:ok, compact_data_fd, compact_idx_fd, compact_data_path, compact_idx_path, final_offset, compacted_pages,
       durable_version}
    end
  end

  # Build compacted data file by iterating pages in key order
  @spec build_compacted_data(DataDatabase.t(), %{Page.id() => {Page.t(), Page.id()}}, :file.fd()) ::
          {%{Page.id() => {Page.t(), Page.id()}}, non_neg_integer()}
  defp build_compacted_data(data_db, page_map, compact_fd) do
    # Sort pages by their first key for better read locality
    sorted_pages =
      Enum.sort_by(page_map, fn {_id, {page, _next}} ->
        Page.left_key(page) || <<>>
      end)

    # Process each page, accumulating writes
    Enum.reduce(sorted_pages, {%{}, 0}, fn {page_id, {page, next_id}}, {pages_acc, offset} ->
      # Process all keys in this page
      {new_kvs, new_offset} =
        page
        |> Page.key_locators()
        |> Enum.reduce({[], offset}, fn {key, old_locator}, {kvs_acc, current_offset} ->
          # Load value from old location (buffer or disk)
          {:ok, value} = DataDatabase.load_value(data_db, old_locator)

          # Write to compacted file
          :ok = :file.write(compact_fd, value)

          # Create new locator for compacted position
          size = byte_size(value)
          new_locator = <<current_offset::47, size::17>>

          {[{key, new_locator} | kvs_acc], current_offset + size}
        end)

      # Build page with new locators
      compacted_page = Page.new(page_id, Enum.reverse(new_kvs))

      {Map.put(pages_acc, page_id, {compacted_page, next_id}), new_offset}
    end)
  end
end
