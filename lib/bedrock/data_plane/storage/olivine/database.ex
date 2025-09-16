defmodule Bedrock.DataPlane.Storage.Olivine.Database do
  @moduledoc false

  alias Bedrock.DataPlane.Storage.Olivine.Index.Page
  alias Bedrock.DataPlane.Version
  alias Exqlite.Sqlite3

  @opaque t :: %__MODULE__{
            sqlite_conn: reference(),
            window_size_in_microseconds: pos_integer(),
            buffer: :ets.tab(),
            durable_version: Bedrock.version(),
            prepared_statements: %{atom() => reference()}
          }
  defstruct sqlite_conn: nil,
            window_size_in_microseconds: 5_000_000,
            buffer: nil,
            durable_version: nil,
            prepared_statements: %{}

  @spec open(otp_name :: atom(), file_path :: String.t()) ::
          {:ok, t()} | {:error, :system_limit | :badarg | File.posix()}
  @spec open(otp_name :: atom(), file_path :: String.t(), window_in_ms :: pos_integer()) ::
          {:ok, t()} | {:error, :system_limit | :badarg | File.posix()}
  def open(_otp_name, file_path, window_in_ms \\ 5_000) do
    case Sqlite3.open(file_path) do
      {:ok, conn} ->
        with :ok <- setup_database(conn),
             {:ok, prepared_statements} <- prepare_statements(conn) do
          buffer = :ets.new(:buffer, [:ordered_set, :protected, {:read_concurrency, true}])

          durable_version =
            case load_durable_version(prepared_statements[:select_metadata], conn) do
              {:ok, version} -> version
              {:error, :not_found} -> Version.zero()
            end

          {:ok,
           %__MODULE__{
             sqlite_conn: conn,
             window_size_in_microseconds: window_in_ms * 1_000,
             buffer: buffer,
             durable_version: durable_version,
             prepared_statements: prepared_statements
           }}
        else
          {:error, reason} ->
            Sqlite3.close(conn)
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  # Setup SQLite database schema and configuration
  defp setup_database(conn) do
    with :ok <- Sqlite3.execute(conn, "PRAGMA journal_mode=MEMORY"),
         :ok <- Sqlite3.execute(conn, "PRAGMA synchronous=NORMAL"),
         :ok <- Sqlite3.execute(conn, "PRAGMA temp_store=MEMORY") do
      create_tables(conn)
    end
  end

  defp create_tables(conn) do
    Enum.reduce_while(
      [
        "CREATE TABLE IF NOT EXISTS pages (page_id INTEGER PRIMARY KEY, page_data BLOB NOT NULL, next_id INTEGER NOT NULL)",
        "CREATE TABLE IF NOT EXISTS kv_pairs (key BLOB PRIMARY KEY, value BLOB NOT NULL)",
        "CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value BLOB NOT NULL)"
      ],
      :ok,
      fn sql, :ok ->
        case Sqlite3.execute(conn, sql) do
          :ok -> {:cont, :ok}
          {:error, reason} -> {:halt, {:error, reason}}
        end
      end
    )
  end

  defp prepare_statements(conn) do
    statements = [
      {:insert_page, "INSERT OR REPLACE INTO pages (page_id, page_data, next_id) VALUES (?, ?, ?)"},
      {:select_page, "SELECT page_data, next_id FROM pages WHERE page_id = ?"},
      {:insert_value, "INSERT OR REPLACE INTO kv_pairs (key, value) VALUES (?, ?)"},
      {:select_value, "SELECT value FROM kv_pairs WHERE key = ?"},
      {:insert_metadata, "INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)"},
      {:select_metadata, "SELECT value FROM metadata WHERE key = ?"},
      {:select_all_pages, "SELECT page_id, page_data, next_id FROM pages ORDER BY page_id"},
      {:count_records, "SELECT (SELECT COUNT(*) FROM pages) + (SELECT COUNT(*) FROM kv_pairs)"},
      {:pragma_page_size, "PRAGMA page_size"},
      {:pragma_page_count, "PRAGMA page_count"},
      {:key_range_min_max, "SELECT MIN(key), MAX(key) FROM kv_pairs"}
    ]

    prepared =
      Enum.reduce_while(statements, %{}, fn {name, sql}, acc ->
        case Sqlite3.prepare(conn, sql) do
          {:ok, stmt} -> {:cont, Map.put(acc, name, stmt)}
          {:error, reason} -> {:halt, {:error, reason}}
        end
      end)

    case prepared do
      %{} = stmts when map_size(stmts) == length(statements) -> {:ok, stmts}
      {:error, reason} -> {:error, reason}
    end
  end

  # Helper functions for common statement execution patterns

  # Execute a statement expecting :done (for INSERT/UPDATE operations)
  defp execute_insert(database, stmt_key, params) do
    stmt = database.prepared_statements[stmt_key]
    Sqlite3.reset(stmt)
    :ok = Sqlite3.bind(stmt, params)

    case Sqlite3.step(database.sqlite_conn, stmt) do
      :done -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  # Execute a statement expecting a single row result
  defp execute_select_one(database, stmt_key, params) do
    stmt = database.prepared_statements[stmt_key]
    Sqlite3.reset(stmt)
    :ok = Sqlite3.bind(stmt, params)

    case Sqlite3.step(database.sqlite_conn, stmt) do
      {:row, [value]} -> {:ok, value}
      :done -> {:error, :not_found}
      {:error, reason} -> {:error, reason}
    end
  end

  # Execute a statement expecting a single row with multiple columns
  defp execute_select_row(database, stmt_key, params) do
    stmt = database.prepared_statements[stmt_key]
    Sqlite3.reset(stmt)
    :ok = Sqlite3.bind(stmt, params)

    case Sqlite3.step(database.sqlite_conn, stmt) do
      {:row, row_data} -> {:ok, row_data}
      :done -> {:error, :not_found}
      {:error, reason} -> {:error, reason}
    end
  end

  # Execute a statement expecting a single value result (for statistics)
  defp execute_select_value(database, stmt_key) do
    stmt = database.prepared_statements[stmt_key]
    Sqlite3.reset(stmt)

    case Sqlite3.step(database.sqlite_conn, stmt) do
      {:row, [value]} -> value
      _ -> 0
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
      Sqlite3.close(database.sqlite_conn)
    catch
      _, _ -> :ok
    end

    :ok
  end

  @doc """
  Store multiple modified pages in the lookaside buffer for the given version.
  This is used during transaction application for efficient batch storage of modified pages.
  """
  @spec store_modified_pages(t(), version :: Bedrock.version(), page_tuples :: [{Page.t(), Page.id()}]) :: :ok
  def store_modified_pages(database, version, page_tuples) do
    Enum.each(page_tuples, fn {page, next_id} ->
      page_id = Page.id(page)
      :ok = store_page_version(database, page_id, version, {page, next_id})
    end)

    :ok
  end

  @doc """
  Store a page in the lookaside buffer for the given version and page_id.
  This is used during transaction application for modified pages within the window.
  """
  @spec store_page_version(t(), Page.id(), version :: Bedrock.version(), page_tuple :: {Page.t(), Page.id()}) :: :ok
  def store_page_version(database, page_id, version, {page, next_id}) do
    :ets.insert(database.buffer, {{version, {:page, page_id}}, {page, next_id}})
    :ok
  end

  @spec store_page(t(), page_id :: Page.id(), page_tuple :: {Page.t(), Page.id()}) :: :ok | {:error, term()}
  def store_page(database, page_id, {page, next_id}),
    do: execute_insert(database, :insert_page, [page_id, {:blob, page}, next_id])

  @spec load_page(t(), page_id :: Page.id()) :: {:ok, {binary(), Page.id()}} | {:error, :not_found}
  def load_page(database, page_id) do
    case execute_select_row(database, :select_page, [page_id]) do
      {:ok, [page_binary, next_id]} -> {:ok, {page_binary, next_id}}
      {:error, reason} -> {:error, reason}
    end
  end

  @spec load_value(t(), key :: Bedrock.key()) :: {:ok, Bedrock.value()} | {:error, :not_found}
  def load_value(database, key), do: execute_select_one(database, :select_value, [{:blob, key}])

  @doc """
  Unified value load that handles both lookaside buffer and SQLite storage.
  Routes between hot (ETS) and cold (SQLite) storage based on version vs durable_version.
  """
  @spec load_value(t(), key :: Bedrock.key(), version :: Bedrock.version()) ::
          {:ok, Bedrock.value()} | {:error, :not_found}
  def load_value(database, key, version) when version > database.durable_version do
    case :ets.lookup(database.buffer, {version, key}) do
      [{_key_version, value}] -> {:ok, value}
      [] -> {:error, :not_found}
    end
  end

  def load_value(database, key, _version), do: load_value(database, key)

  @spec store_value(t(), key :: Bedrock.key(), value :: Bedrock.value()) :: :ok | {:error, term()}
  def store_value(database, key, value), do: execute_insert(database, :insert_value, [{:blob, key}, {:blob, value}])

  @doc """
  Store a value in the lookaside buffer for the given version and key.
  This is used during transaction application for values within the window.
  """
  @spec store_value(t(), key :: Bedrock.key(), version :: Bedrock.version(), value :: Bedrock.value()) :: :ok
  def store_value(database, key, version, value) do
    :ets.insert(database.buffer, {{version, key}, value})
    :ok
  end

  @doc """
  Batch store values and pages in the lookaside buffer for a given version.
  This enables atomic writes during transaction application.
  """
  @spec batch_store_version_data(
          t(),
          version :: Bedrock.version(),
          values :: [{Bedrock.key(), Bedrock.value()}],
          pages :: [{Page.id(), {binary(), Page.id()}}]
        ) :: :ok | {:error, term()}
  def batch_store_version_data(database, version, values, pages) do
    page_entries = Enum.map(pages, fn {page_id, page_tuple} -> {{version, {:page, page_id}}, page_tuple} end)

    all_entries =
      Enum.reduce(values, page_entries, fn {key, value}, acc ->
        [{{version, key}, value} | acc]
      end)

    if :ets.insert_new(database.buffer, all_entries) do
      :ok
    else
      {:error, :insert_failed}
    end
  end

  @doc """
  Returns a value loader function that captures only the minimal data needed
  for async value resolution tasks. Avoids copying the entire Database struct.
  """
  @spec value_loader(t()) :: (Bedrock.key(), Bedrock.version() ->
                                {:ok, Bedrock.value()} | {:error, :not_found} | {:error, :shutting_down})
  def value_loader(database) do
    sqlite_conn = database.sqlite_conn
    select_value_stmt = database.prepared_statements[:select_value]
    buffer = database.buffer
    durable_version = database.durable_version

    fn
      key, version when version > durable_version ->
        case :ets.lookup(buffer, {version, key}) do
          [{_key_version, value}] -> {:ok, value}
          [] -> {:error, :not_found}
        end

      key, _version ->
        Sqlite3.reset(select_value_stmt)
        :ok = Sqlite3.bind(select_value_stmt, [{:blob, key}])

        case Sqlite3.step(sqlite_conn, select_value_stmt) do
          {:row, [value]} -> {:ok, value}
          :done -> {:error, :not_found}
          {:error, _reason} -> {:error, :shutting_down}
        end
    end
  end

  @spec get_all_page_ids(t()) :: [Page.id()]
  def get_all_page_ids(database),
    do: execute_select_all_no_params(database, :select_all_pages, fn [page_id, _page_data, _next_id] -> page_id end)

  @spec batch_store_values(t(), [{Bedrock.key(), Bedrock.value()}]) :: :ok | {:error, term()}
  def batch_store_values(database, key_value_tuples) do
    with :ok <- Sqlite3.execute(database.sqlite_conn, "BEGIN TRANSACTION"),
         :ok <- insert_values_batch(database, key_value_tuples),
         :ok <- Sqlite3.execute(database.sqlite_conn, "COMMIT") do
      :ok
    else
      {:error, reason} ->
        Sqlite3.execute(database.sqlite_conn, "ROLLBACK")
        {:error, reason}
    end
  end

  defp insert_values_batch(_database, []), do: :ok

  defp insert_values_batch(database, [{key, value} | rest]) do
    case store_value(database, key, value) do
      :ok -> insert_values_batch(database, rest)
      {:error, reason} -> {:error, reason}
    end
  end

  @spec store_durable_version(t(), version :: Bedrock.version()) :: {:ok, t()} | {:error, term()}
  def store_durable_version(database, version) do
    with :ok <- execute_insert(database, :insert_metadata, ["durable_version", {:blob, version}]) do
      updated_database = %{database | durable_version: version}
      {:ok, updated_database}
    end
  end

  @spec get_durable_version(t()) :: {:ok, Bedrock.version()} | {:error, :not_found}
  def get_durable_version(database), do: {:ok, database.durable_version}

  # Internal function for loading durable version using prepared statement
  @spec load_durable_version(reference(), reference()) ::
          {:ok, Bedrock.version()} | {:error, :not_found}
  defp load_durable_version(stmt, sqlite_conn), do: execute_select_one_direct(stmt, sqlite_conn, ["durable_version"])

  # Direct execute helper for cases where we have stmt and conn separately
  defp execute_select_one_direct(stmt, sqlite_conn, params) do
    Sqlite3.reset(stmt)
    :ok = Sqlite3.bind(stmt, params)

    case Sqlite3.step(sqlite_conn, stmt) do
      {:row, [value]} -> {:ok, value}
      :done -> {:error, :not_found}
      {:error, _reason} -> {:error, :not_found}
    end
  end

  # Helper to collect all rows from a prepared statement with no parameters
  defp execute_select_all_no_params(database, stmt_key, row_mapper) do
    stmt = database.prepared_statements[stmt_key]
    Sqlite3.reset(stmt)
    collect_rows(database.sqlite_conn, stmt, [], row_mapper)
  end

  defp collect_rows(conn, stmt, acc, row_mapper) do
    case Sqlite3.step(conn, stmt) do
      {:row, row_data} ->
        mapped_data = row_mapper.(row_data)
        collect_rows(conn, stmt, [mapped_data | acc], row_mapper)

      :done ->
        Enum.reverse(acc)

      {:error, _reason} ->
        Enum.reverse(acc)
    end
  end

  @spec load_durable_version(t()) :: {:ok, Bedrock.version()}
  def load_durable_version(database), do: {:ok, database.durable_version}

  @doc """
  Load durable version directly from SQLite storage.
  This is useful for background processes that may have a stale database struct.
  """
  @spec load_current_durable_version(t()) :: {:ok, Bedrock.version()} | {:error, :not_found}
  def load_current_durable_version(database),
    do: load_durable_version(database.prepared_statements[:select_metadata], database.sqlite_conn)

  @spec info(t(), :n_keys | :utilization | :size_in_bytes | :key_ranges) ::
          any() | :undefined
  def info(database, stat) do
    case stat do
      :n_keys ->
        get_total_record_count(database)

      :size_in_bytes ->
        get_database_size(database)

      :utilization ->
        calculate_utilization(database)

      :key_ranges ->
        get_key_ranges(database)

      _ ->
        :undefined
    end
  end

  defp get_total_record_count(database), do: execute_select_value(database, :count_records)

  defp get_database_size(database) do
    page_size = execute_select_value(database, :pragma_page_size)
    page_count = execute_select_value(database, :pragma_page_count)

    # Ensure we don't divide by zero and have reasonable defaults
    page_size = if page_size == 0, do: 1, else: page_size
    page_count = if page_count == 0, do: 0, else: page_count

    page_size * page_count
  end

  defp calculate_utilization(database) do
    case get_total_record_count(database) do
      0 -> 0.0
      count -> min(1.0, count / max(1, get_database_size(database) / 1000))
    end
  end

  defp get_key_ranges(database) do
    stmt = database.prepared_statements[:key_range_min_max]
    Sqlite3.reset(stmt)

    case Sqlite3.step(database.sqlite_conn, stmt) do
      {:row, [min_key, max_key]} when min_key != nil and max_key != nil ->
        [{min_key, max_key}]

      _ ->
        []
    end
  end

  @spec sync(t()) :: :ok
  def sync(database) do
    case Sqlite3.execute(database.sqlite_conn, "PRAGMA synchronous=FULL") do
      :ok -> :ok
      {:error, _reason} -> :ok
    end
  catch
    _, _ -> :ok
  end

  @doc """
  Advances the durable version with full persistence handling.
  This handles the complete persistence process:
  - Extracts data for the specified versions from lookaside buffer
  - Persists all values and pages atomically to SQLite
  - Cleans up lookaside buffer
  - Updates durable version
  """
  @spec advance_durable_version(t(), version :: Bedrock.version(), versions_to_persist :: [Bedrock.version()]) ::
          {:ok, t()} | {:error, term()}
  def advance_durable_version(database, new_durable_version, _versions_to_persist) do
    with :ok <- Sqlite3.execute(database.sqlite_conn, "BEGIN TRANSACTION"),
         :ok <- execute_sqlite_tx(database, new_durable_version),
         :ok <- cleanup_buffer(database, new_durable_version),
         :ok <- Sqlite3.execute(database.sqlite_conn, "COMMIT") do
      {:ok, %{database | durable_version: new_durable_version}}
    else
      {:error, reason} ->
        Sqlite3.execute(database.sqlite_conn, "ROLLBACK")
        {:error, reason}
    end
  end

  # Efficiently removes all entries for versions older than or equal to the durable version.
  # This is a more efficient way to clean up the lookaside buffer when advancing the durable version.
  # Uses a single select_delete operation to remove all obsolete entries at once.
  @spec cleanup_buffer(t(), version :: Bedrock.version()) :: :ok
  defp cleanup_buffer(database, durable_version) do
    :ets.select_delete(database.buffer, [{{{:"$1", :_}, :_}, [{:"=<", :"$1", durable_version}], [true]}])
    :ok
  end

  @doc """
  Builds transaction data from the lookaside buffer for testing purposes.
  Returns the same format as the original DETS implementation for test compatibility.
  """
  @spec build_dets_tx(t(), new_durable_version :: Bedrock.version()) ::
          [{:durable_version, binary()} | {Bedrock.key(), Bedrock.value()} | {Page.id(), {Page.t(), Page.id()}}]
  def build_dets_tx(database, new_durable_version) do
    buffer_data = extract_buffer_data(database, new_durable_version)
    all_data = Map.merge(buffer_data.values, buffer_data.pages)

    [{:durable_version, new_durable_version} | Map.to_list(all_data)]
  end

  # Extracts deduplicated values and pages from the lookaside buffer for versions up to the durable version.
  # Uses efficient last-writer-wins semantics: processes entries from newest to oldest version,
  # keeping only the first occurrence of each key/page_id. This eliminates redundant persistence.
  defp extract_buffer_data(database, new_durable_version) do
    database.buffer
    |> :ets.select_reverse([{{{:"$1", :"$2"}, :"$3"}, [{:"=<", :"$1", new_durable_version}], [{{:"$2", :"$3"}}]}])
    |> Enum.reduce(%{pages: %{}, values: %{}}, fn
      {{:page, page_id}, page_tuple}, acc ->
        %{acc | pages: Map.put_new(acc.pages, page_id, page_tuple)}

      {key, value}, acc when is_binary(key) ->
        %{acc | values: Map.put_new(acc.values, key, value)}
    end)
  end

  # Executes the transaction against SQLite by inserting values and pages separately.
  @spec execute_sqlite_tx(t(), new_durable_version :: Bedrock.version()) :: :ok | {:error, term()}
  defp execute_sqlite_tx(database, new_durable_version) do
    buffer_data = extract_buffer_data(database, new_durable_version)

    with :ok <- store_metadata(database, "durable_version", new_durable_version),
         :ok <- store_buffered_pages(database, buffer_data.pages) do
      store_buffered_values(database, buffer_data.values)
    end
  end

  defp store_metadata(database, key, value), do: execute_insert(database, :insert_metadata, [key, {:blob, value}])

  defp store_buffered_pages(_database, pages) when map_size(pages) == 0, do: :ok

  defp store_buffered_pages(database, pages) do
    Enum.reduce_while(pages, :ok, fn {page_id, {page_binary, next_id}}, :ok ->
      case store_page(database, page_id, {page_binary, next_id}) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp store_buffered_values(_database, values) when map_size(values) == 0, do: :ok

  defp store_buffered_values(database, values) do
    Enum.reduce_while(values, :ok, fn {key, value}, :ok ->
      case store_value(database, key, value) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end
end
