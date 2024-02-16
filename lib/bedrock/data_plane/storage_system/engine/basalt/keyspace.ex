defmodule Bedrock.DataPlane.StorageSystem.Engine.Basalt.Keyspace do
  use Bedrock.Cluster, :types
  alias Bedrock.DataPlane.Transaction

  @type t :: :ets.tid()

  @spec new(atom()) :: t()
  def new(name) when is_atom(name),
    do: :ets.new(name, [:ordered_set, :public, read_concurrency: true])

  @spec close(pkv :: t()) :: :ok
  def close(mvcc) do
    :ets.delete(mvcc)
    :ok
  end

  @spec apply_transaction(keyspace :: t(), transaction()) :: :ok
  def apply_transaction(keyspace, transaction) do
    with true <-
           :ets.insert(keyspace, [
             {:last_version, Transaction.version(transaction)}
             | Transaction.key_values(transaction)
               |> Enum.map(fn
                 {key, nil} -> {key, false}
                 {key, _value} -> {key, true}
               end)
           ]) do
      :ok
    end
  end

  @spec load_keys(keyspace :: t(), keys :: [key()]) :: :ok
  def load_keys(keyspace, keys) do
    true = :ets.insert_new(keyspace, keys |> Enum.map(fn key -> {key, true} end))
    :ok
  end

  @spec prune(keyspace :: t()) :: {:ok, n_pruned :: non_neg_integer()}
  def prune(keyspace) do
    n_pruned = :ets.select_delete(keyspace, [{{:_, :"$1"}, [{:"=:=", false, :"$1"}], [true]}])
    {:ok, n_pruned}
  end

  @spec key_exists?(keyspace :: t(), key()) :: boolean()
  def key_exists?(keyspace, key) when is_binary(key) do
    :ets.lookup(keyspace, key)
    |> case do
      [] -> false
      [{_, present}] -> present
    end
  end
end
