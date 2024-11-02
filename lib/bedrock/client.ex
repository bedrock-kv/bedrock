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
  alias Bedrock.ControlPlane.DataDistributor.Team

  @type t :: %__MODULE__{
          coordinator: coordinator(),
          read_version_proxy: pid() | nil,
          data_distributor: pid() | nil
        }
  defstruct coordinator: nil,
            read_version_proxy: nil,
            data_distributor: nil

  @typep coordinator :: pid() | {atom(), node()}
  @type transaction_fn :: (Transaction.t() -> any())

  @spec new(coordinator()) :: {:ok, t()}
  def new(coordinator) do
    {:ok, %__MODULE__{coordinator: coordinator}}
  end

  @doc """
  Begin and return a transaction. Transactions are short-lived, and should be
  committed as soon as possible
  """
  @spec transaction(t :: t()) :: {:ok, Transaction.t()}
  def transaction(%__MODULE__{} = t) do
    with {:ok, config} <- Coordinator.fetch_config(t.coordinator) do
      Transaction.start_link(t, config)
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
      do_transaction(txn, transaction_fn, 5)
    end
  end

  @type transaction_fn(result) :: (Transaction.t() ->
                                     {:commit, Transaction.t(), result} | {:cancel, result})

  @spec do_transaction(
          Transaction.t(),
          transaction_fn(result),
          n_retries :: non_neg_integer()
        ) :: {:error, :aborted} | result
        when result: term()
  defp do_transaction(_txn, _transaction_fn, 0), do: {:error, :aborted}

  defp do_transaction(txn, transaction_fn, n_retries) do
    case transaction_fn.(txn) do
      {:commit, txn, result} ->
        case commit(txn) do
          :ok -> result
          {:error, :aborted} -> do_transaction(txn, transaction_fn, n_retries - 1)
        end

      {:cancel, result} ->
        result
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
  defdelegate put(txn, key, value),
    to: Transaction

  @doc """
  Commits a transaction.
  """
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
