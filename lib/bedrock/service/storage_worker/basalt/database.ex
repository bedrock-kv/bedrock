defmodule Bedrock.Service.StorageWorker.Basalt.Database do
  use Bedrock, :types
  use Bedrock.Cluster, :types
  use Bedrock.Service.StorageWorker, :types

  defstruct ~w[mvcc keyspace pkv waiting_list]a
  @type t :: %__MODULE__{}

  alias Bedrock.Service.StorageWorker.Basalt.PersistentKeyValues
  alias Bedrock.Service.StorageWorker.Basalt.MultiversionConcurrencyControl, as: MVCC
  alias Bedrock.Service.StorageWorker.Basalt.Keyspace
  alias Bedrock.Service.StorageWorker.Basalt.WaitingList

  @spec open(otp_name :: atom(), file_path :: String.t()) :: {:ok, t()} | {:error, term()}
  def open(otp_name, file_path) when is_atom(otp_name) do
    with {:ok, pkv} <- PersistentKeyValues.open(:"#{otp_name}_pkv", file_path),
         last_durable_version <- PersistentKeyValues.last_version(pkv),
         mvcc <- MVCC.new(:"#{otp_name}_mvcc", last_durable_version),
         keyspace <- Keyspace.new(:"#{otp_name}_keyspace"),
         {:ok, waiting_list} <- WaitingList.start_link(last_durable_version) do
      {:ok,
       %__MODULE__{
         mvcc: mvcc,
         keyspace: keyspace,
         pkv: pkv,
         waiting_list: waiting_list
       }}
    end
  end

  @spec close(database :: t()) :: :ok
  def close(database) do
    with :ok <- ensure_durability_to_latest_version(database),
         :ok <- PersistentKeyValues.close(database.pkv),
         :ok <- Keyspace.close(database.keyspace),
         :ok <- MVCC.close(database.mvcc) do
    end

    :ok
  end

  @spec last_durable_version(database :: t()) :: version() | :undefined
  def last_durable_version(database), do: database.pkv |> PersistentKeyValues.last_version()

  @spec key_range(database :: t()) :: key_range() | :undefined
  def key_range(database), do: database.pkv |> PersistentKeyValues.key_range()

  @spec load_keys(t()) :: :ok
  def load_keys(database) do
    %{keyspace: keyspace, pkv: pkv} = database

    :telemetry.span(
      [:bedrock, :storage, :basalt, :database, :load_keys],
      %{database: database},
      fn ->
        PersistentKeyValues.stream_keys(pkv)
        |> Stream.chunk_every(500)
        |> Stream.map(fn keys -> :ok = Keyspace.insert_many(keyspace, keys) end)
        |> Stream.run()

        {:ok, %{}}
      end
    )
  end

  @spec apply_transactions(database :: t(), transactions :: [transaction()]) :: version()
  def apply_transactions(database, transactions) do
    latest_committed_version = MVCC.apply_transactions!(database.mvcc, transactions)
    WaitingList.notify_version_committed(database.waiting_list, latest_committed_version)
    latest_committed_version
  end

  def last_committed_version(database),
    do: MVCC.newest_version(database.mvcc)

  @spec lookup(database :: t(), key(), version()) ::
          {:ok, value()}
          | {:error, :not_found | :transaction_too_old | :transaction_too_new | :timeout}
  @spec lookup(database :: t(), key(), version(), timeout_in_ms :: non_neg_integer()) ::
          {:ok, value()}
          | {:error, :not_found | :transaction_too_old | :transaction_too_new | :timeout}
  def lookup(database, key, version, timeout_in_ms \\ 0) do
    lookup_in_mvcc(database, key, version)
    |> case do
      {:error, :transaction_too_new} ->
        wait_for_version(database, version, timeout_in_ms)
        |> case do
          :ok -> lookup_in_mvcc(database, key, version)
          {:error, :timeout} -> {:error, :transaction_too_new}
        end

      other ->
        other
    end
  end

  defp wait_for_version(database, version, timeout),
    do: database.waiting_list |> WaitingList.wait_for_version(version, timeout)

  defp lookup_in_mvcc(database, key, version) do
    MVCC.lookup(database.mvcc, key, version)
    |> case do
      {:error, :not_found} -> lookup_in_pkv(database, key, version)
      result -> result
    end
  end

  defp lookup_in_pkv(database, key, version) do
    %{keyspace: keyspace, pkv: pkv, mvcc: mvcc} = database

    value =
      if Keyspace.key_exists?(keyspace, key) do
        PersistentKeyValues.lookup(pkv, key)
      end

    :ok = MVCC.insert_read(mvcc, key, version, value)

    if value do
      {:ok, value}
    else
      {:error, :not_found}
    end
  end

  @doc """
  Returns information about the database. The following statistics are
  available:

  * `:n_keys` - the number of keys in the store
  * `:size_in_bytes` - the size of the database in bytes
  * `:utilization` - the utilization of the database (as a percentage, expressed
    as a float between 0.0 and 1.0)
  """
  @spec info(database :: t(), :n_keys | :utilization | :size_in_bytes) :: any() | :undefined
  def info(database, stat),
    do: database.pkv |> PersistentKeyValues.info(stat)

  @doc """
  Ensures that the database is durable up to the latest version.
  """
  @spec ensure_durability_to_latest_version(db :: t()) :: :ok
  def ensure_durability_to_latest_version(db),
    do: ensure_durability_to_version(db, MVCC.newest_version(db.mvcc))

  @doc """
  Ensures that the database is durable up to the given version. This is done by
  applying all transactions up to the given version to the the underlying
  persistent key value store. Versions of values older than the given version
  are pruned from the store.
  """
  @spec ensure_durability_to_version(db :: t(), version()) :: :ok
  def ensure_durability_to_version(db, version) do
    if transaction = MVCC.transaction_at_version(db.mvcc, version) do
      PersistentKeyValues.apply_transaction(db.pkv, transaction)
      Keyspace.apply_transaction(db.keyspace, transaction)

      {:ok, _n_purged} = MVCC.purge_keys_older_than_version(db.mvcc, version)
    end

    :ok
  end
end
