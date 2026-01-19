defmodule Bedrock.DataPlane.Materializer.Basalt.Keyspace do
  @moduledoc """
  A keyspace is an ordered set of keys.

  Under the hood, an ordered_set ETS table is used to store the keys. It is
  intended that the keyspace can be read and written concurrently by multiple
  processes, so we rely on the fact that ETS insert operations are atomic to
  ensure that the keyspace is always in a consistent state. As such, each key
  is stored along with a boolean "presence" value. This allows us to determine
  whether a key has been deleted or not (while still relying on the atomicity
  of the insert operation).
  """

  alias Bedrock.DataPlane.Transaction

  @opaque t :: :ets.tid()

  @spec new(atom()) :: t()
  def new(name) when is_atom(name), do: :ets.new(name, [:ordered_set, :public, read_concurrency: true])

  @spec close(pkv :: t()) :: :ok
  def close(mvcc) do
    :ets.delete(mvcc)
    :ok
  end

  @spec apply_transaction(keyspace :: t(), Transaction.encoded()) :: :ok
  def apply_transaction(keyspace, encoded_transaction) do
    {:ok, version} = Transaction.commit_version(encoded_transaction)
    {:ok, mutations_stream} = Transaction.mutations(encoded_transaction)

    # Convert mutations to key presence indicators
    key_entries =
      Enum.map(mutations_stream, fn
        {:set, key, _value} -> {key, true}
        # Treat as single key clear for simplicity
        {:clear_range, key, _end} -> {key, false}
      end)

    true = :ets.insert(keyspace, [{:last_version, version} | key_entries])
    :ok
  end

  @spec insert_many(keyspace :: t(), keys :: [Bedrock.key()]) :: :ok
  def insert_many(keyspace, keys) do
    true = :ets.insert_new(keyspace, Enum.map(keys, fn key -> {key, true} end))
    :ok
  end

  @spec prune(keyspace :: t()) :: {:ok, n_pruned :: non_neg_integer()}
  def prune(keyspace) do
    n_pruned = :ets.select_delete(keyspace, [{{:_, :"$1"}, [{:"=:=", false, :"$1"}], [true]}])
    {:ok, n_pruned}
  end

  @spec key_exists?(keyspace :: t(), Bedrock.key()) :: boolean()
  def key_exists?(keyspace, key) when is_binary(key) do
    keyspace
    |> :ets.lookup(key)
    |> case do
      [] -> false
      [{_, present}] -> present
    end
  end
end
