defmodule Bedrock.DataPlane.Storage.Olivine.Database do
  @moduledoc false

  alias Bedrock.DataPlane.Storage.Olivine.Index.Page
  alias Bedrock.DataPlane.Version

  @opaque t :: %__MODULE__{
            dets_storage: :dets.tab_name(),
            data_file: :file.fd(),
            data_file_offset: non_neg_integer(),
            data_file_name: [char()],
            window_size_in_microseconds: pos_integer(),
            buffer: :ets.tab(),
            durable_version: Bedrock.version()
          }
  defstruct dets_storage: nil,
            data_file: nil,
            data_file_offset: 0,
            data_file_name: nil,
            window_size_in_microseconds: 5_000_000,
            buffer: nil,
            durable_version: nil

  @type locator :: <<_::64>>

  @spec open(otp_name :: atom(), file_path :: String.t()) ::
          {:ok, t()} | {:error, :system_limit | :badarg | File.posix()}
  @spec open(otp_name :: atom(), file_path :: String.t(), window_in_ms :: pos_integer()) ::
          {:ok, t()} | {:error, :system_limit | :badarg | File.posix()}
  @spec open(otp_name :: atom(), file_path :: String.t(), opts :: keyword()) ::
          {:ok, t()} | {:error, :system_limit | :badarg | File.posix()}
  def open(otp_name, file_path, opts_or_window \\ 5_000)

  def open(otp_name, file_path, window_in_ms) when is_atom(otp_name) and is_integer(window_in_ms) do
    do_open(otp_name, file_path, window_in_ms)
  end

  def open(otp_name, file_path, opts) when is_atom(otp_name) and is_list(opts) do
    window_in_ms = Keyword.get(opts, :window_in_ms, 5_000)
    do_open(otp_name, file_path, window_in_ms)
  end

  defp do_open(otp_name, file_path, window_in_ms) do
    storage_opts = [
      {:type, :set},
      {:access, :read_write},
      {:auto_save, :infinity},
      {:estimated_no_objects, 1_000_000}
    ]

    data_file_name = String.to_charlist(file_path <> ".data")
    {:ok, data_file} = :file.open(data_file_name, [:raw, :binary, :read, :append])
    {:ok, offset} = :file.position(data_file, {:eof, 0})

    case :dets.open_file(otp_name, [{:file, String.to_charlist(file_path <> ".idx")} | storage_opts]) do
      {:ok, dets_table} ->
        buffer = :ets.new(:buffer, [:ordered_set, :protected, {:read_concurrency, true}])

        durable_version =
          case load_current_durable_version(%{dets_storage: dets_table}) do
            {:ok, version} -> version
            {:error, :not_found} -> Version.zero()
          end

        {:ok,
         %__MODULE__{
           dets_storage: dets_table,
           data_file: data_file,
           data_file_offset: offset,
           data_file_name: data_file_name,
           window_size_in_microseconds: window_in_ms * 1_000,
           buffer: buffer,
           durable_version: durable_version
         }}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec close(t()) :: :ok
  def close(database) do
    try do
      :ets.delete(database.buffer)
    catch
      _, _ -> :ok
    end

    try do
      :file.close(database.data_file)
    catch
      _, _ -> :ok
    end

    try do
      :dets.sync(database.dets_storage)
    catch
      _, _ -> :ok
    end

    try do
      :dets.close(database.dets_storage)
    catch
      :exit, _ -> :ok
    end

    :ok
  end

  @spec store_page(t(), page_id :: Page.id(), page_tuple :: {Page.t(), Page.id()}) :: :ok | {:error, term()}
  def store_page(database, page_id, {page, next_id}) do
    case :dets.insert(database.dets_storage, {page_id, {page, next_id}}) do
      :ok -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @spec load_page(t(), page_id :: Page.id()) :: {:ok, {binary(), Page.id()}} | {:error, :not_found}
  def load_page(database, page_id) do
    case :dets.lookup(database.dets_storage, page_id) do
      [{^page_id, {page_binary, next_id}}] -> {:ok, {page_binary, next_id}}
      [] -> {:error, :not_found}
    end
  end

  @spec load_value(t(), locator()) :: {:ok, Bedrock.value()} | {:error, :not_found}
  def load_value(database, locator) do
    case locator do
      <<_offset::47, 0::17>> ->
        {:ok, <<>>}

      <<offset::47, size::17>> = locator ->
        case :ets.lookup(database.buffer, locator) do
          [{^locator, value}] -> {:ok, value}
          [] -> load_from_data_file(database.data_file_name, offset, size)
        end
    end
  end

  @doc """
  Store a value in the lookaside buffer for the given version and key.
  This is used during transaction application for values within the window.
  """
  @spec store_value(t(), key :: Bedrock.key(), version :: Bedrock.version(), value :: Bedrock.value()) ::
          {:ok, locator(), database :: t()}
  def store_value(database, _key, _version, value) do
    offset = database.data_file_offset
    size = byte_size(value)
    locator = <<offset::47, size::17>>
    :ets.insert(database.buffer, {locator, value})
    {:ok, locator, %{database | data_file_offset: offset + size}}
  end

  @doc """
  Returns a value loader function that captures only the minimal data needed
  for async value resolution tasks. Avoids copying the entire Database struct.
  """
  @spec value_loader(t()) :: (locator() -> {:ok, Bedrock.value()} | {:error, :not_found} | {:error, :shutting_down})
  def value_loader(database) do
    data_file_name = database.data_file_name
    buffer = database.buffer

    fn
      <<_offset::47, 0::17>> ->
        {:ok, <<>>}

      <<offset::47, size::17>> = locator ->
        case :ets.lookup(buffer, locator) do
          [{^locator, value}] -> {:ok, value}
          [] -> load_from_data_file(data_file_name, offset, size)
        end
    end
  end

  defp load_from_data_file(data_file_name, offset, size) do
    data_file_name
    |> :file.open([:raw, :binary, :read])
    |> case do
      {:ok, file} ->
        try do
          :file.pread(file, offset, size)
        after
          :file.close(file)
        end

      error ->
        error
    end
  end

  @doc """
  Returns a value loader function that captures only the minimal data needed
  for async value resolution tasks. Avoids copying the entire Database struct.
  """
  @spec many_value_loader(t()) ::
          ([locator()] ->
             {:ok, %{locator() => Bedrock.value()}}
             | {:error, :not_found}
             | {:error, :shutting_down})
  def many_value_loader(database) do
    data_file_name = database.data_file_name
    buffer = database.buffer

    fn
      locators when is_list(locators) ->
        load_many_values(locators, buffer, data_file_name)
    end
  end

  defp load_many_values(locators, buffer, data_file_name) do
    {result, not_found} = partition_locators_by_availability(locators, buffer)
    merge_with_disk_values(result, not_found, data_file_name)
  end

  defp partition_locators_by_availability(locators, buffer) do
    Enum.reduce(locators, {%{}, []}, fn
      <<_::47, 0::17>> = locator, {result, not_found} ->
        {Map.put(result, locator, <<>>), not_found}

      locator, {result, not_found} ->
        case :ets.lookup(buffer, locator) do
          [{^locator, value}] -> {Map.put(result, locator, value), not_found}
          [] -> {result, [locator | not_found]}
        end
    end)
  end

  defp merge_with_disk_values(result, [], _data_file_name), do: {:ok, result}

  defp merge_with_disk_values(result, not_found, data_file_name) do
    {:ok, values} = load_many_from_data_file(data_file_name, not_found)
    {:ok, Map.merge(result, not_found |> Enum.zip(values) |> Map.new())}
  end

  defp load_many_from_data_file(data_file_name, locators) do
    data_file_name
    |> :file.open([:raw, :binary, :read])
    |> case do
      {:ok, file} ->
        try do
          :file.pread(file, Enum.map(locators, fn <<offset::47, size::17>> -> {offset, size} end))
        after
          :file.close(file)
        end

      error ->
        error
    end
  end

  @spec durable_version(t()) :: Bedrock.version()
  def durable_version(database), do: database.durable_version

  @doc """
  Load durable version directly from DETS storage.
  This is useful for background processes that may have a stale database struct.
  """
  @spec load_current_durable_version(t() | %{dets_storage: :dets.tab_name()}) ::
          {:ok, Bedrock.version()} | {:error, :not_found}
  def load_current_durable_version(%{dets_storage: dets_storage}) do
    case :dets.lookup(dets_storage, :durable_version) do
      [{:durable_version, version}] -> {:ok, version}
      [] -> {:error, :not_found}
    end
  end

  @spec info(t(), :n_keys | :utilization | :size_in_bytes | :key_ranges) :: any() | :undefined
  def info(database, stat) do
    case stat do
      :n_keys ->
        :dets.info(database.dets_storage, :no_objects) || 0

      :size_in_bytes ->
        :dets.info(database.dets_storage, :file_size) || 0

      :utilization ->
        calculate_utilization(database.dets_storage)

      # Key range tracking will be implemented in a future phase.
      # This will require maintaining metadata about the range of keys
      # stored in the database, supporting efficient range queries and
      # partition management across distributed storage nodes.
      :key_ranges ->
        []

      _ ->
        :undefined
    end
  end

  defp calculate_utilization(dets_storage) do
    case :dets.info(dets_storage, :no_objects) do
      nil -> 0.0
      0 -> 0.0
      objects -> calculate_utilization_ratio(objects, dets_storage)
    end
  end

  defp calculate_utilization_ratio(objects, dets_storage) do
    file_size = :dets.info(dets_storage, :file_size) || 1
    min(1.0, objects / max(1, file_size / 1000))
  end

  @spec advance_durable_version(
          t(),
          version :: Bedrock.version(),
          data_size_in_bytes :: pos_integer()
        ) ::
          {:ok, t(), metadata :: map()} | {:error, term()}
  def advance_durable_version(database, new_durable_version, data_size_in_bytes) do
    %{
      start_time: System.monotonic_time(:microsecond),
      database: database,
      new_durable_version: new_durable_version,
      data_size_in_bytes: data_size_in_bytes
    }
    |> build_transactions()
    |> insert_dets_tx()
    |> write_data_file()
    |> dets_sync()
    |> cleanup_buffer()
    |> then(fn metadata ->
      total_duration_μs = System.monotonic_time(:microsecond) - metadata.start_time
      metadata = Map.put(metadata, :total_duration_μs, total_duration_μs)

      case metadata.sync_result do
        :ok ->
          {:ok, %{metadata.database | durable_version: metadata.new_durable_version}, metadata}

        error ->
          error
      end
    end)
  end

  defp build_transactions(metadata) do
    {build_time_μs, {dets_tx, write_iolist}} =
      :timer.tc(fn ->
        dets_tx = build_dets_tx(metadata.database, metadata.new_durable_version)
        write_iolist = build_write_iolist(metadata.database, metadata.data_size_in_bytes)
        {dets_tx, write_iolist}
      end)

    tx_size_bytes = :erlang.iolist_size(write_iolist)
    tx_count = length(write_iolist)

    Map.merge(metadata, %{
      dets_tx: dets_tx,
      write_iolist: write_iolist,
      build_time_μs: build_time_μs,
      tx_size_bytes: tx_size_bytes,
      tx_count: tx_count
    })
  end

  defp insert_dets_tx(metadata) do
    {insert_time_μs, :ok} =
      :timer.tc(fn -> :dets.insert(metadata.database.dets_storage, metadata.dets_tx) end)

    Map.put(metadata, :insert_time_μs, insert_time_μs)
  end

  defp write_data_file(metadata) do
    {write_time_μs, :ok} =
      :timer.tc(fn ->
        :file.pwrite(
          metadata.database.data_file,
          metadata.data_size_in_bytes - metadata.tx_size_bytes,
          metadata.write_iolist
        )
      end)

    Map.put(metadata, :write_time_μs, write_time_μs)
  end

  defp dets_sync(metadata) do
    {sync_time_μs, result} =
      :timer.tc(fn ->
        try do
          :dets.sync(metadata.database.dets_storage)
          :ok
        catch
          _, _ -> :ok
        end
      end)

    Map.merge(metadata, %{sync_time_μs: sync_time_μs, sync_result: result})
  end

  defp cleanup_buffer(metadata) do
    {cleanup_time_μs, :ok} =
      :timer.tc(fn ->
        mark = <<metadata.data_size_in_bytes::47, 0::17>>
        # Clean up page entries with new key format
        :ets.select_delete(metadata.database.buffer, [
          {{{:page, :"$1", :"$2"}, :_}, [{:"=<", :"$1", metadata.new_durable_version}], [true]}
        ])

        # Clean up value entries with locators below high water mark
        :ets.select_delete(metadata.database.buffer, [{{:"$1", :_}, [{:<, :"$1", mark}], [true]}])
        :ok
      end)

    Map.put(metadata, :cleanup_time_μs, cleanup_time_μs)
  end

  # Extract deduplicated values and pages from the lookaside buffer for versions up to the durable version.
  # Uses efficient last-writer-wins semantics: processes entries from newest to oldest version,
  # keeping only the first occurrence of each key/page_id. This eliminates redundant persistence.
  # Returns a single list of {key, value} tuples where keys can be integers (page_ids) or binaries.
  @spec build_dets_tx(t(), new_durable_version :: Bedrock.version()) ::
          [{:durable_version, binary()} | {Bedrock.key(), Bedrock.value()} | {Page.id(), {Page.t(), Page.id()}}]
  def build_dets_tx(database, new_durable_version) do
    [
      {:durable_version, new_durable_version}
      | database.buffer
        |> :ets.select_reverse([
          # Match page entries with new key format
          {{{:page, :"$1", :"$2"}, :"$3"}, [{:"=<", :"$1", new_durable_version}], [{{:"$2", :"$3"}}]}
        ])
        |> Enum.reduce(%{}, fn {page_id, page_tuple}, data_map -> Map.put_new(data_map, page_id, page_tuple) end)
        |> Map.to_list()
    ]
  end

  def build_write_iolist(database, data_size_in_bytes) do
    mark = <<data_size_in_bytes::47, 0::17>>

    database.buffer
    |> :ets.select([{{:"$1", :"$2"}, [{:"=<", :"$1", mark}], [{{:"$1", :"$2"}}]}])
    |> Enum.reduce([], fn
      {locator, value}, iolist when is_binary(locator) and is_binary(value) ->
        [value | iolist]

      _, iolist ->
        iolist
    end)
    |> Enum.reverse()
  end

  def store_modified_pages(database, version, modified_pages) do
    :ets.insert(
      database.buffer,
      Enum.map(modified_pages, fn {page_id, page_tuple} ->
        # Use a tuple key to avoid collision with locators
        {{:page, version, page_id}, page_tuple}
      end)
    )

    :ok
  end
end
