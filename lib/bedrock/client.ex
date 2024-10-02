defmodule Bedrock.Client do
  @moduledoc """
  The client module provides a simple interface for interacting with the
  Bedrock system. It provides a simple API for starting transactions, reading
  and writing values, and committing transactions.
  """

  alias Bedrock.Client.Transaction
  alias Bedrock.ControlPlane.Coordinator
  alias Bedrock.ControlPlane.DataDistributor
  alias Bedrock.DataPlane.Storage
  alias Bedrock.DataPlane.Proxy
  alias Bedrock.ControlPlane.DataDistributor.Team

  @type t :: %__MODULE__{
          coordinator: coordinator(),
          read_version_proxy: pid(),
          data_distributor: pid(),
          transaction_window_in_ms: non_neg_integer()
        }
  defstruct coordinator: nil,
            read_version_proxy: nil,
            data_distributor: nil,
            transaction_window_in_ms: 0

  @typep coordinator :: pid() | {atom(), node()}
  @type transaction_fn :: (Transaction.t() -> any())

  @spec new(coordinator()) :: {:ok, t()} | {:error, :no_coordinators}
  def new(coordinator) do
    coordinator
    |> Coordinator.fetch_proxy()
    |> case do
      {:ok, read_version_proxy} ->
        {:ok,
         %__MODULE__{
           coordinator: coordinator,
           read_version_proxy: read_version_proxy,
           transaction_window_in_ms: 5_000
         }}

      {:error, :unavailable} ->
        {:error, :no_coordinators}
    end
  end

  @doc """
  Begin and return a transaction. Transactions are short-lived, and should be
  committed as soon as possible
  """
  @spec transaction(client :: t()) :: {:ok, Transaction.t()}
  def transaction(%__MODULE__{} = client) do
    with {:ok, read_version} <- client.read_version_proxy |> Proxy.next_read_version() do
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
  @spec transaction(client :: t(), transaction_fn()) :: any()
  def transaction(%__MODULE__{} = client, transaction_fn) do
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
  @spec get(txn :: Transaction.t(), Bedrock.key()) :: nil | Bedrock.value()
  defdelegate get(txn, key),
    to: Transaction

  @doc """
  Read a value for the given key. If no value has been set, return the given
  default value.
  """
  @spec get(txn :: Transaction.t(), Bedrock.key(), default :: Bedrock.value()) :: Bedrock.value()
  def get(txn, key, default_value),
    do: Transaction.get(txn, key) || default_value

  @doc """
  Put a key/value pair into a transaction.
  """
  @spec put(txn :: Transaction.t(), Bedrock.key(), Bedrock.value()) :: :ok
  defdelegate put(txn, key, value),
    to: Transaction

  @doc """
  Commits a transaction.
  """
  @spec commit(txn :: Transaction.t()) :: :ok | {:error, :transaction_expired}
  defdelegate commit(txn),
    to: Transaction

  @spec storage_workers_for_key(client :: t(), Bedrock.key()) :: [Storage.ref()]
  def storage_workers_for_key(client, key) do
    client.data_distributor
    |> DataDistributor.storage_team_for_key(key)
    |> case do
      {:ok, storage_team} ->
        Team.storage_workers(storage_team)

      {:error, :not_found} ->
        raise "No storage team found for key #{inspect(key)}"
    end
  end
end
