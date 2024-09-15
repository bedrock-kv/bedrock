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
    with :ok <- ensure_durability_to_version(database, :latest),
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
      [:bedrock, :storage, :alice, :database, :load_keys],
      %{database: database},
      fn ->
        PersistentKeyValues.stream_keys(pkv)
        |> Stream.chunk_every(500)
        |> Stream.map(fn keys -> :ok = Keyspace.load_keys(keyspace, keys) end)
        |> Stream.run()

        {:ok, %{}}
      end
    )
  end

  @spec apply_transactions(database :: t(), transactions :: [transaction()]) :: version()
  def apply_transactions(database, transactions) do
    latest_committed_version = MVCC.apply_transactions!(database.mvcc, transactions)
    database.waiting_list |> WaitingList.notify_version_committed(latest_committed_version)
    latest_committed_version
  end

  @spec lookup(database :: t(), key(), version()) ::
          {:ok, value()} | {:error, :not_found | :transaction_too_old | :transaction_too_new}
  @spec lookup(database :: t(), key(), version(), opts :: keyword()) ::
          {:ok, value()} | {:error, :not_found | :transaction_too_old | :transaction_too_new}
  def lookup(database, key, version, opts \\ []) do
    wait_if_necessary(database, version, opts[:timeout])
    |> case do
      :ok -> lookup_in_mvcc(database, key, version)
      {:error, _reason} = error -> error
    end
  end

  defp wait_if_necessary(database, version, timeout) do
    if version > latest_committed_version(database) do
      wait_for_version(database, version, timeout)
    else
      :ok
    end
  end

  defp latest_committed_version(database),
    do: database.mvcc |> MVCC.last_version()

  defp wait_for_version(_database, _version, nil),
    do: {:error, :transaction_too_new}

  defp wait_for_version(database, version, timeout),
    do: database.waiting_list |> WaitingList.wait_for_version(version, timeout)

  defp lookup_in_mvcc(database, key, version) do
    case database.mvcc |> MVCC.lookup(key, version) do
      {:error, :not_found} -> lookup_in_pkv(database, key, version)
      {:ok, _value} = result -> result
    end
  end

  defp lookup_in_pkv(database, key, version) do
    value =
      if database.keyspace |> Keyspace.key_exists?(key) do
        database.pkv |> PersistentKeyValues.lookup(key)
      end

    database.mvcc |> MVCC.insert_read(key, version, value)

    if value do
      {:ok, value}
    else
      {:error, :not_found}
    end
  end

  @spec info(database :: t(), :n_objects | :utilization | :size_in_bytes) :: any() | :undefined
  def info(database, stat),
    do: database.pkv |> PersistentKeyValues.info(stat)

  @spec ensure_durability_to_version(database :: t(), :latest | version()) :: :ok
  def ensure_durability_to_version(database, version) do
    if transaction = database.mvcc |> MVCC.transaction_at_version(version) do
      database.pkv |> PersistentKeyValues.apply_transaction(transaction)
      database.keyspace |> Keyspace.apply_transaction(transaction)
    end

    :ok
  end
end
