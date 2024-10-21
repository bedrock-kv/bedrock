defmodule Bedrock.DataPlane.Storage.Basalt.Database do
  @moduledoc """
  """

  defstruct ~w[mvcc keyspace pkv key_range]a
  @type t :: %__MODULE__{}

  alias Bedrock.DataPlane.Storage
  alias Bedrock.DataPlane.Storage.Basalt.PersistentKeyValues
  alias Bedrock.DataPlane.Storage.Basalt.MultiversionConcurrencyControl, as: MVCC
  alias Bedrock.DataPlane.Storage.Basalt.Keyspace
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version

  @spec open(otp_name :: atom(), file_path :: String.t()) :: {:ok, t()} | {:error, term()}
  def open(otp_name, file_path) when is_atom(otp_name) do
    with {:ok, pkv} <- PersistentKeyValues.open(:"#{otp_name}_pkv", file_path),
         last_durable_version <- PersistentKeyValues.last_version(pkv),
         mvcc <- MVCC.new(:"#{otp_name}_mvcc", last_durable_version),
         keyspace <- Keyspace.new(:"#{otp_name}_keyspace"),
         key_range <- PersistentKeyValues.key_range(pkv),
         :ok <- load_keys_into_keyspace(pkv, keyspace) do
      {:ok,
       %__MODULE__{
         mvcc: mvcc,
         keyspace: keyspace,
         pkv: pkv,
         key_range: key_range
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

  @spec last_durable_version(database :: t()) :: Bedrock.version() | :undefined
  def last_durable_version(database), do: database.pkv |> PersistentKeyValues.last_version()

  @spec key_range(database :: t()) :: Storage.key_range() | :undefined
  def key_range(database), do: database.key_range

  @spec load_keys_into_keyspace(PersistentKeyValues.t(), Keyspace.t()) :: :ok
  def load_keys_into_keyspace(pkv, keyspace) do
    PersistentKeyValues.stream_keys(pkv)
    |> Stream.chunk_every(1_000)
    |> Stream.map(fn keys -> :ok = Keyspace.insert_many(keyspace, keys) end)
    |> Stream.run()
  end

  @spec apply_transactions(database :: t(), transactions :: [Transaction.t()]) ::
          Bedrock.version()
  def apply_transactions(database, transactions),
    do: MVCC.apply_transactions!(database.mvcc, transactions)

  def last_committed_version(database),
    do: MVCC.newest_version(database.mvcc)

  @spec fetch(database :: t(), Bedrock.key(), Bedrock.version()) ::
          {:ok, Bedrock.value()}
          | {:error,
             :not_found
             | :key_out_of_range}
  def fetch(%{key_range: {min_key, max_key}}, key, _version)
      when key < min_key or key >= max_key,
      do: {:error, :key_out_of_range}

  def fetch(database, key, version) do
    MVCC.fetch(database.mvcc, key, version)
    |> case do
      {:error, :not_found} ->
        if Keyspace.key_exists?(database.keyspace, key) and
             not Version.older?(version, MVCC.oldest_version(database.mvcc)) do
          fetch_from_persistence_and_write_back_to_mvcc(database, key, version)
        else
          {:error, :tx_too_old}
        end

      result ->
        result
    end
  end

  @spec fetch_from_persistence_and_write_back_to_mvcc(t(), Bedrock.key(), Bedrock.version()) ::
          {:ok, Bedrock.value()} | {:error, :not_found}
  defp fetch_from_persistence_and_write_back_to_mvcc(database, key, version) do
    PersistentKeyValues.fetch(database.pkv, key)
    |> case do
      {:ok, value} = result ->
        :ok = MVCC.insert_read(database.mvcc, key, version, value)
        result

      {:error, :not_found} = result ->
        result
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
  @spec ensure_durability_to_version(db :: t(), Bedrock.version()) :: :ok
  def ensure_durability_to_version(db, version) do
    if transaction = MVCC.transaction_at_version(db.mvcc, version) do
      PersistentKeyValues.apply_transaction(db.pkv, transaction)
      Keyspace.apply_transaction(db.keyspace, transaction)

      {:ok, _n_purged} = MVCC.purge_keys_older_than_version(db.mvcc, version)
    end

    :ok
  end
end
