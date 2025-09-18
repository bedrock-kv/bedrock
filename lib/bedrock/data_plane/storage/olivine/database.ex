defmodule Bedrock.DataPlane.Storage.Olivine.Database do
  @moduledoc false

  alias Bedrock.DataPlane.Storage.Olivine.Index.Page
  alias Bedrock.DataPlane.Version

  @opaque t :: %__MODULE__{
            pool: pid(),
            window_size_in_microseconds: pos_integer(),
            buffer: :ets.tab(),
            durable_version: Bedrock.version()
          }
  defstruct pool: nil,
            window_size_in_microseconds: 5_000_000,
            buffer: nil,
            durable_version: nil

  @spec open(otp_name :: atom(), file_path :: String.t()) ::
          {:ok, t()} | {:error, :system_limit | :badarg | File.posix()}
  @spec open(otp_name :: atom(), file_path :: String.t(), window_in_ms :: pos_integer()) ::
          {:ok, t()} | {:error, :system_limit | :badarg | File.posix()}
  @spec open(otp_name :: atom(), file_path :: String.t(), opts :: keyword()) ::
          {:ok, t()} | {:error, :system_limit | :badarg | File.posix()}
  def open(otp_name, file_path, opts_or_window \\ [])

  def open(otp_name, file_path, window_in_ms) when is_integer(window_in_ms) do
    open(otp_name, file_path, window_in_ms: window_in_ms)
  end

  def open(_otp_name, file_path, opts) when is_list(opts) do
    window_in_ms = Keyword.get(opts, :window_in_ms, 5_000)
    pool_size = Keyword.get(opts, :pool_size, 5)

    pool_options = [
      database: file_path,
      pool_size: pool_size,
      journal_mode: :wal,
      synchronous: :normal,
      temp_store: :memory,
      foreign_keys: :on,
      cache_size: -64_000,
      # Increased timeouts for WAL mode concurrent access
      # 5 seconds to handle WAL lock contention
      busy_timeout: 5_000,
      # Default queue target
      queue_target: 50,
      # Default queue processing
      queue_interval: 1_000,
      # Connection retry settings
      # Exponential backoff
      backoff_type: :exp,
      # Start at 10ms
      backoff_min: 10,
      # Max 1s backoff
      backoff_max: 1000,
      # Pool startup configuration
      # Don't allow overflow connections
      pool_overflow: 0,
      # Allow connection restarts
      max_restarts: 5,
      # Time window for restart limit
      max_seconds: 10
    ]

    case DBConnection.start_link(Exqlite.Connection, pool_options) do
      {:ok, pool_pid} ->
        case setup_database_schema(pool_pid) do
          :ok ->
            durable_version = load_durable_version_from_db(pool_pid)
            buffer = :ets.new(:buffer, [:ordered_set, :protected, {:read_concurrency, true}])

            {:ok,
             %__MODULE__{
               pool: pool_pid,
               window_size_in_microseconds: window_in_ms * 1_000,
               buffer: buffer,
               durable_version: durable_version
             }}

          {:error, reason} ->
            GenServer.stop(pool_pid)
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  # Setup SQLite database schema (PRAGMAs handled by Exqlite.Connection)
  defp setup_database_schema(pool) do
    sql_statements = [
      "CREATE TABLE IF NOT EXISTS pages (page_id INTEGER PRIMARY KEY, page_data BLOB NOT NULL, next_id INTEGER NOT NULL)",
      "CREATE TABLE IF NOT EXISTS kv_pairs (key BLOB PRIMARY KEY, value BLOB NOT NULL)",
      "CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value BLOB NOT NULL)"
    ]

    DBConnection.run(pool, fn conn ->
      sql_statements
      |> Enum.map(&%Exqlite.Query{statement: &1})
      |> Enum.each(
        &case DBConnection.execute(conn, &1, []) do
          {:ok, _, _} -> :ok
          {:error, reason} -> {:halt, {:error, reason}}
        end
      )
    end)
  end

  defp load_durable_version_from_db(pool) do
    DBConnection.run(pool, fn conn ->
      case DBConnection.execute(conn, %Exqlite.Query{statement: "SELECT value FROM metadata WHERE key = $1"}, [
             "durable_version"
           ]) do
        {:ok, _, %{rows: [[version]]}} -> version
        {:ok, _, %{rows: []}} -> Version.zero()
        {:error, _} -> Version.zero()
      end
    end)
  end

  # Helper functions using DBConnection pool

  @spec close(t()) :: :ok
  def close(database) do
    try do
      :ets.delete(database.buffer)
    catch
      _, _ -> :ok
    end

    try do
      # Monitor the pool process to wait for it to actually die
      ref = Process.monitor(database.pool)

      # Stop the pool
      :ok = GenServer.stop(database.pool, :normal, 5_000)

      # Wait for the :DOWN message to ensure process is fully terminated
      receive do
        {:DOWN, ^ref, :process, _, _} -> :ok
      after
        # Timeout after 1 second
        1_000 -> :ok
      end
    catch
      :exit, _ -> :ok
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

  @spec load_page(t(), page_id :: Page.id()) :: {:ok, {binary(), Page.id()}} | {:error, :not_found}
  def load_page(database, page_id) do
    query = "SELECT page_data, next_id FROM pages WHERE page_id = $1"

    DBConnection.run(database.pool, fn conn ->
      case DBConnection.execute(conn, %Exqlite.Query{statement: query}, [page_id]) do
        {:ok, _, %{rows: [[page_binary, next_id]]}} -> {:ok, {page_binary, next_id}}
        {:ok, _, %{rows: []}} -> {:error, :not_found}
        {:error, %DBConnection.ConnectionError{}} -> {:error, :unavailable}
        {:error, reason} -> {:error, reason}
      end
    end)
  end

  @spec load_value(t(), key :: Bedrock.key()) :: {:ok, Bedrock.value()} | {:error, :not_found}
  def load_value(database, key) do
    query = "SELECT value FROM kv_pairs WHERE key = $1"

    DBConnection.run(database.pool, fn conn ->
      case DBConnection.execute(conn, %Exqlite.Query{statement: query}, [{:blob, key}]) do
        {:ok, _, %{rows: [[value]]}} -> {:ok, value}
        {:ok, _, %{rows: []}} -> {:error, :not_found}
        {:error, %DBConnection.ConnectionError{}} -> {:error, :unavailable}
        {:error, reason} -> {:error, reason}
      end
    end)
  end

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
    pool_pid = database.pool
    buffer = database.buffer

    fn key, version ->
      # Always check both ETS and SQLite since we can't track durable_version updates
      # Try ETS first (for hot data)
      case :ets.lookup(buffer, {version, key}) do
        [{_key_version, value}] ->
          {:ok, value}

        [] ->
          # Not in ETS, try SQLite (for cold data)
          try do
            DBConnection.run(pool_pid, fn conn ->
              query =
                DBConnection.prepare!(conn, %Exqlite.Query{statement: "SELECT value FROM kv_pairs WHERE key = $1"})

              case DBConnection.execute(conn, query, [{:blob, key}]) do
                {:ok, _, %{rows: [[value]]}} -> {:ok, value}
                {:ok, _, %{rows: []}} -> {:error, :not_found}
                {:error, %DBConnection.ConnectionError{}} -> {:error, :unavailable}
                {:error, _reason} -> {:error, :shutting_down}
              end
            end)
          catch
            :exit, _ -> {:error, :shutting_down}
          end
      end
    end
  end

  @spec get_durable_version(t()) :: {:ok, Bedrock.version()} | {:error, :not_found}
  def get_durable_version(database), do: {:ok, database.durable_version}

  # Helper functions removed - using DBConnection directly

  @spec load_durable_version(t()) :: {:ok, Bedrock.version()}
  def load_durable_version(database), do: {:ok, database.durable_version}

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

  defp get_total_record_count(database) do
    query = "SELECT (SELECT COUNT(*) FROM pages) + (SELECT COUNT(*) FROM kv_pairs)"

    DBConnection.run(database.pool, fn conn ->
      case DBConnection.execute(conn, %Exqlite.Query{statement: query}, []) do
        {:ok, _, %{rows: [[count]]}} -> count
        _ -> 0
      end
    end)
  end

  defp get_database_size(database) do
    page_size_query = "PRAGMA page_size"
    page_count_query = "PRAGMA page_count"

    DBConnection.run(database.pool, fn conn ->
      with {:ok, _, %{rows: [[page_size]]}} <-
             DBConnection.execute(conn, %Exqlite.Query{statement: page_size_query}, []),
           {:ok, _, %{rows: [[page_count]]}} <-
             DBConnection.execute(conn, %Exqlite.Query{statement: page_count_query}, []) do
        # Ensure we don't divide by zero and have reasonable defaults
        page_size = if page_size == 0, do: 1, else: page_size
        page_count = if page_count == 0, do: 0, else: page_count
        page_size * page_count
      else
        _ -> 0
      end
    end)
  end

  defp calculate_utilization(database) do
    case get_total_record_count(database) do
      0 -> 0.0
      count -> min(1.0, count / max(1, get_database_size(database) / 1000))
    end
  end

  defp get_key_ranges(database) do
    query = "SELECT MIN(key), MAX(key) FROM kv_pairs"

    DBConnection.run(database.pool, fn conn ->
      case DBConnection.execute(conn, %Exqlite.Query{statement: query}, []) do
        {:ok, _, %{rows: [[min_key, max_key]]}} when min_key != nil and max_key != nil ->
          [{min_key, max_key}]

        _ ->
          []
      end
    end)
  end

  @spec sync(t()) :: :ok
  def sync(database) do
    DBConnection.run(database.pool, fn conn ->
      case DBConnection.execute(conn, %Exqlite.Query{statement: "PRAGMA wal_checkpoint(FULL)"}, []) do
        {:ok, _, _} -> :ok
        {:error, _reason} -> :ok
      end
    end)
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
    database.pool
    |> DBConnection.transaction(fn conn ->
      buffer_data = extract_buffer_data(database, new_durable_version)

      with :ok <- persist_metadata(conn, "durable_version", new_durable_version),
           :ok <- persist_pages(conn, buffer_data.pages),
           :ok <- persist_kv_pairs(conn, buffer_data.values) do
        :ok
      else
        {:error, reason} -> DBConnection.rollback(conn, reason)
      end
    end)
    |> case do
      {:ok, :ok} ->
        cleanup_buffer(database, new_durable_version)
        {:ok, %{database | durable_version: new_durable_version}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp persist_metadata(conn, key, value) do
    query = "INSERT OR REPLACE INTO metadata (key, value) VALUES ($1, $2)"
    DBConnection.execute!(conn, %Exqlite.Query{statement: query}, [key, {:blob, value}])
  end

  defp persist_pages(_conn, pages) when map_size(pages) == 0, do: :ok

  defp persist_pages(conn, pages) do
    upsert_page =
      DBConnection.prepare!(conn, %Exqlite.Query{
        statement: "INSERT OR REPLACE INTO pages (page_id, page_data, next_id) VALUES ($1, $2, $3)"
      })

    Enum.each(pages, fn {page_id, {page_binary, next_id}} ->
      DBConnection.execute!(conn, upsert_page, [page_id, {:blob, page_binary}, next_id])
    end)
  end

  defp persist_kv_pairs(_conn, kv_pairs) when map_size(kv_pairs) == 0, do: :ok

  defp persist_kv_pairs(conn, kv_pairs) do
    upsert_kv_pair =
      DBConnection.prepare!(conn, %Exqlite.Query{
        statement: "INSERT OR REPLACE INTO kv_pairs (key, value) VALUES ($1, $2)"
      })

    Enum.each(kv_pairs, fn {key, value} ->
      DBConnection.execute!(conn, upsert_kv_pair, [{:blob, key}, {:blob, value}])
    end)
  end

  # Efficiently removes all entries for versions older than or equal to the durable version.
  # This is a more efficient way to clean up the lookaside buffer when advancing the durable version.
  # Uses a single select_delete operation to remove all obsolete entries at once.
  @spec cleanup_buffer(t(), version :: Bedrock.version()) :: :ok
  defp cleanup_buffer(database, durable_version) do
    :ets.select_delete(database.buffer, [{{{:"$1", :_}, :_}, [{:"=<", :"$1", durable_version}], [true]}])
    :ok
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
end
