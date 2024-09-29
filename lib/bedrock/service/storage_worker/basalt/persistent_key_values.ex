defmodule Bedrock.Service.StorageWorker.Basalt.PersistentKeyValues do
  @moduledoc """
  """
  use Bedrock, :types
  use Bedrock.Cluster, :types

  alias Bedrock.Service.StorageWorker
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version

  @type t :: :dets.tab_name()

  @doc """
  Opens a persistent key-value store.
  """
  @spec open(atom(), String.t()) :: {:ok, t()} | {:error, any()}
  def open(name, file_path) when is_atom(name) do
    :dets.open_file(name,
      access: :read_write,
      auto_save: :infinity,
      type: :set,
      file: file_path |> String.to_charlist()
    )
  end

  @doc """
  Closes a persistent key-value store.
  """
  @spec close(t()) :: :ok
  def close(dets),
    do: :dets.close(dets)

  @doc """
  Returns the last version of the key-value store.
  """
  @spec last_version(t()) :: version() | :undefined
  def last_version(pkv) do
    fetch(pkv, :last_version)
    |> case do
      {:error, :not_found} -> :undefined
      {:ok, version} -> version
    end
  end

  @doc """
  Returns the key range of the key-value store.
  """
  @spec key_range(t()) :: StorageWorker.key_range() | :undefined
  def key_range(pkv) do
    fetch(pkv, :key_range)
    |> case do
      {:error, :not_found} -> :undefined
      {:ok, {_min, _max}} = key_range -> key_range
    end
  end

  @doc """
  Apply a transaction to the key-value store, atomically. The transaction must
  be applied in order.
  """
  @spec apply_transaction(pkv :: t(), Transaction.t()) :: :ok | {:error, term()}
  def apply_transaction(pkv, transaction) do
    with transaction_version <- Transaction.version(transaction),
         true <-
           Version.newer?(transaction_version, last_version(pkv)) ||
             {:error, :transaction_too_old},
         :ok <-
           :dets.insert(pkv, [
             {:last_version, transaction_version}
             | Transaction.key_values(transaction)
           ]) do
      :dets.sync(pkv)
    end
  end

  @doc """
  Attempt to find the value for the given key in the key-value store. Returns
  `nil` if the key is not found.
  """
  @spec fetch(pkv :: t(), key :: term()) :: {:ok, term()} | {:error, :not_found}
  def fetch(pkv, key) do
    pkv
    |> :dets.lookup(key)
    |> case do
      [] -> {:error, :not_found}
      [{_, value}] -> {:ok, value}
    end
  end

  @doc """
  Interrogate the key-value store for specific metadata. Supported queries are:

  * `:n_keys` - the number of keys in the store
  * `:size_in_bytes` - the size of the store in bytes
  * `:utilization` - the utilization of the database (as a percentage, expressed
    as a float between 0.0 and 1.0)
  """
  @spec info(pkv :: t(), :n_keys | :size_in_bytes | :utilization) :: any() | :undefined
  def info(pkv, :n_keys) do
    # We don't count the :last_version key
    pkv
    |> :dets.info(:no_objects)
    |> case do
      0 -> 0
      n_keys -> n_keys - 1
    end
  end

  def info(pkv, :utilization) do
    pkv
    |> :dets.info(:no_slots)
    |> case do
      {min, used, max} -> Float.ceil((used - min) / max, 1)
      :undefined -> :undefined
    end
  end

  def info(pkv, :size_in_bytes), do: pkv |> :dets.info(:file_size)

  def info(_pkv, _query), do: :undefined

  @doc """
  Prune the key-value store of any keys that have a `nil` value.
  """
  @spec prune(pkv :: t()) :: {:ok, n_pruned :: non_neg_integer()}
  def prune(pkv) do
    n_pruned = :dets.select_delete(pkv, [{{:_, :"$1"}, [{:is_nil}], [true]}])
    {:ok, n_pruned}
  end

  @doc """
  Return a stream of all keys in the key-value store. The keys are not
  guaranteed to be in any particular order.
  """
  @spec stream_keys(pkv :: t()) :: Enumerable.t()
  def stream_keys(pkv) do
    Stream.resource(
      fn -> :dets.first(pkv) end,
      fn
        :"$end_of_table" -> {:halt, :ok}
        key -> {[key], :dets.next(pkv, key)}
      end,
      fn _ -> :ok end
    )
    |> Stream.filter(&is_binary/1)
  end
end
