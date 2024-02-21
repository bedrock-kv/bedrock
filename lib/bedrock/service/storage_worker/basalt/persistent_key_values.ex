defmodule Bedrock.Service.StorageWorker.Basalt.PersistentKeyValues do
  use Bedrock, :types
  use Bedrock.Cluster, :types
  use Bedrock.Service.StorageWorker, :types

  alias Bedrock.DataPlane.Transaction

  @type t :: :dets.tid()

  @spec open(atom(), String.t()) :: {:ok, t()} | {:error, any()}
  def open(name, file_path) when is_atom(name) do
    :dets.open_file(name,
      access: :read_write,
      auto_save: :infinity,
      type: :set,
      file: file_path |> String.to_charlist()
    )
  end

  @spec close(t()) :: :ok
  def close(dets),
    do: :dets.close(dets)

  @spec last_version(t()) :: version() | :undefined
  def last_version(pkv) do
    lookup(pkv, :last_version)
    |> case do
      nil -> :undefined
      {_, version} -> version
    end
  end

  @spec key_range(t()) :: StorageSystem.Engine.key_range() | :undefined
  def key_range(pkv) do
    lookup(pkv, :key_range)
    |> case do
      nil -> :undefined
      {_min, _max} = key_range -> key_range
    end
  end

  @spec apply_transaction(pkv :: t(), {version(), [key_value()]}) :: :ok | {:error, term()}
  def apply_transaction(pkv, transaction) do
    with :ok <-
           :dets.insert(pkv, [
             {:last_version, Transaction.version(transaction)}
             | Transaction.key_values(transaction)
           ]),
         :ok <- :dets.sync(pkv) do
    end
  end

  @spec lookup(pkv :: t(), term()) :: term() | nil
  def lookup(pkv, key) do
    pkv
    |> :dets.lookup(key)
    |> case do
      [] -> nil
      [{_, value}] -> value
    end
  end

  def info(pkv, :n_objects), do: pkv |> :dets.info(:no_objects)

  def info(pkv, :utilization) do
    pkv
    |> :dets.info(:no_slots)
    |> case do
      {min, used, max} ->
        Float.ceil((used - min) / max, 1)

      :undefined ->
        :undefined
    end
  end

  def info(pkv, :size_in_bytes), do: pkv |> :dets.info(:file_size)

  @spec prune(pkv :: t()) :: {:ok, n_pruned :: non_neg_integer()}
  def prune(pkv) do
    n_pruned = :dets.select_delete(pkv, [{{:_, :"$1"}, [{:is_nil}], [true]}])
    {:ok, n_pruned}
  end

  @spec stream_keys(pkv :: t()) :: Stream.t()
  def stream_keys(pkv) do
    Stream.resource(
      fn -> :dets.first(pkv) end,
      fn
        <<0xFF>> <> _ -> {:halt, :ok}
        :"$end_of_table" -> {:halt, :ok}
        key -> {[key], :dets.next(pkv, key)}
      end,
      fn _ -> :ok end
    )
  end
end
