defmodule Bedrock.Client do
  alias Bedrock.Client.Transaction
  alias Bedrock.ControlPlane.Coordinator
  alias Bedrock.ControlPlane.DataDistributor
  alias Bedrock.Service.Storage, as: StorageSystemEngine
  alias Bedrock.DataPlane.TransactionSystem.ReadVersionProxy

  defstruct [
    :coordinator,
    :read_version_proxy,
    :data_distributor,
    :transaction_window_in_ms
  ]

  @type t :: %__MODULE__{}
  @type transaction_fn :: (Transaction.t() -> any())

  @spec new(coordinator :: pid() | {atom(), node()}) :: {:ok, t()} | {:error, :not_found}
  def new(coordinator) do
    coordinator
    |> Coordinator.get_nearest_read_version_proxy()
    |> case do
      {:ok, read_version_proxy} ->
        {:ok,
         %__MODULE__{
           coordinator: coordinator,
           read_version_proxy: read_version_proxy,
           transaction_window_in_ms: 5_000
         }}

      {:error, :not_found} ->
        {:error, :no_coordinators}
    end
  end

  @doc """
  Begin and return a transaction. Transactions are short-lived, and should be
  committed as soon as possible
  """
  @spec transaction(client :: t()) :: {:ok, Transaction.t()}
  @spec transaction(client :: t(), transaction_fn()) :: any()
  def transaction(%__MODULE__{} = client) do
    with {:ok, read_version} <- client.read_version_proxy |> ReadVersionProxy.next_read_version() do
      Transaction.start_link(client, read_version)
    end
  end

  @doc """
  Begin a transaction, and call given function with the new transaction as
  a parameter. It is expected that the function will return a tuple of the
  form:

   `{:commit, transaction, result}` if the transaction is to be committed and
   the result will be returned.

   `{:cancel, reason}` if the transaction is to be discarded and the reason
   returned.
  """
  def transaction(client, transaction_fn) do
    with {:ok, txn} <- transaction(client) do
      txn
      |> transaction_fn.()
      |> case do
        {:commit, txn, result} ->
          :ok = commit(txn)
          result

        {:cancel, reason} ->
          reason
      end
    end
  end

  @doc """
  Read a value for the given key. If no value has been set, then return nil.
  """
  @spec get(txn :: pid(), key :: binary()) :: nil | binary()
  defdelegate get(txn, key),
    to: Transaction

  @doc """
  Read a value for the given key. If no value has been set, return the given
  default value.
  """
  @spec get(txn :: pid(), key :: binary(), default_value :: binary()) :: binary()
  def get(txn, key, default_value),
    do: Transaction.get(txn, key) || default_value

  @doc """
  Put a key/value pair into a transaction.
  """
  @spec put(txn :: pid(), key :: binary(), value :: binary()) :: :ok
  defdelegate put(txn, key, value),
    to: Transaction

  @doc """
  Commits a transaction.
  """
  @spec commit(txn :: pid()) :: :ok | {:error, :transaction_expired}
  defdelegate commit(txn),
    to: Transaction

  @spec storage_engine_for_key(transaction :: t(), key :: binary()) :: StorageSystemEngine.t()
  def storage_engine_for_key(client, key) do
    client.data_distributor
    |> DataDistributor.storage_team_for_key(key)
    |> case do
      {:ok, storage_team} ->
        storage_team |> StorageSystemEngine.storage_engines_for_key(key)

      {:error, :not_found} ->
        raise "No storage team found for key #{inspect(key)}"
    end
  end
end
