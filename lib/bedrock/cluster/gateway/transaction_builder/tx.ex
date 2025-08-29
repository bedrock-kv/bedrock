defmodule Bedrock.Cluster.Gateway.TransactionBuilder.Tx do
  @moduledoc """
  Opaque transaction type for building and committing database operations.

  This module provides an immutable transaction structure that accumulates
  reads, writes, and range operations. Transactions can be committed to
  produce the final mutation list and conflict ranges for resolution.
  """

  alias Bedrock.DataPlane.Transaction

  @type key :: binary()
  @type value :: binary()
  @type range :: {start :: binary(), end_ex :: binary()}

  @type mutation ::
          {:set, key(), value()}
          | {:clear, key()}
          | {:clear_range, start :: binary(), end_ex :: binary()}

  @type t :: %__MODULE__{
          mutations: [mutation()],
          writes: :gb_trees.tree(key(), value() | :clear),
          reads: %{key() => value() | :clear},
          range_writes: [range()],
          range_reads: [range()]
        }
  defstruct mutations: [],
            writes: :gb_trees.empty(),
            reads: %{},
            range_writes: [],
            range_reads: []

  def new, do: %__MODULE__{}

  @spec get(
          t(),
          key(),
          fetch_fn :: (key(), state -> {{:ok, value()} | {:error, reason}, state}),
          state :: any()
        ) :: {t(), {:ok, value()} | {:error, reason}, state}
        when reason: term(), state: term()
  def get(t, k, fetch_fn, state) when is_binary(k) do
    case get_write(t, k) || get_read(t, k) do
      nil -> fetch(t, k, fetch_fn, state)
      :clear -> {t, {:error, :not_found}, state}
      value -> {t, {:ok, value}, state}
    end
  end

  def set(t, k, v) when is_binary(k) and is_binary(v) do
    t
    |> remove_ops_in_range(k, next_key(k))
    |> put_write(k, v)
    |> record_mutation({:set, k, v})
  end

  def clear(t, k) when is_binary(k) do
    t
    |> remove_ops_in_range(k, next_key(k))
    |> put_clear(k)
    |> record_mutation({:clear, k})
  end

  def clear_range(t, s, e) when is_binary(s) and is_binary(e) do
    t
    |> remove_ops_in_range(s, e)
    |> remove_writes_in_range(s, e)
    |> clear_reads_in_range(s, e)
    |> add_write_range(s, e)
    |> record_mutation({:clear_range, s, e})
  end

  @doc """
  Commits the transaction and returns the transaction map format.

  This is useful for testing and cases where the raw transaction structure
  is needed without binary encoding.
  """
  def commit(t, read_version \\ nil) do
    write_conflicts =
      t.writes
      |> :gb_trees.keys()
      |> Enum.reduce(t.range_writes, fn k, acc -> add_or_merge(acc, k, next_key(k)) end)

    read_conflicts =
      t.reads
      |> Map.keys()
      |> Enum.reduce(t.range_reads, fn k, acc -> add_or_merge(acc, k, next_key(k)) end)

    # Enforce read_version/read_conflicts coupling: if no reads, ignore read_version
    read_conflicts_tuple =
      case read_conflicts do
        [] -> {nil, []}
        non_empty when read_version != nil -> {read_version, non_empty}
        _non_empty when read_version == nil -> {nil, []}
      end

    %{
      mutations: Enum.reverse(t.mutations),
      write_conflicts: write_conflicts,
      read_conflicts: read_conflicts_tuple
    }
  end

  @doc """
  Commits the transaction and returns binary encoded format.

  This is useful for commit proxy operations and other cases where
  efficient binary format is preferred.
  """
  def commit_binary(t, read_version \\ nil), do: t |> commit(read_version) |> Transaction.encode()

  defp remove_ops_in_range(t, s, e) do
    %{
      t
      | mutations:
          Enum.reject(t.mutations, fn
            {:set, k, _} -> k >= s && k < e
            {:clear, k} -> k >= s && k < e
            _ -> false
          end)
    }
  end

  defp remove_writes_in_range(t, s, e) do
    # Get all keys in range and delete them
    keys_to_remove =
      s
      |> :gb_trees.iterator_from(t.writes)
      |> gb_trees_range_keys(e, [])

    new_writes =
      Enum.reduce(keys_to_remove, t.writes, fn k, tree ->
        :gb_trees.delete_any(k, tree)
      end)

    %{t | writes: new_writes}
  end

  # Helper function to collect keys in range from gb_trees iterator
  defp gb_trees_range_keys(iterator, end_key, acc) do
    case :gb_trees.next(iterator) do
      {key, _value, next_iterator} when key < end_key ->
        gb_trees_range_keys(next_iterator, end_key, [key | acc])

      _ ->
        Enum.reverse(acc)
    end
  end

  defp clear_reads_in_range(t, s, e) do
    %{
      t
      | reads:
          Map.new(t.reads, fn
            {k, _} when k >= s and k < e -> {k, :clear}
            kv -> kv
          end)
    }
  end

  defp add_write_range(t, s, e) do
    %{
      t
      | range_writes: add_or_merge(t.range_writes, s, e)
    }
  end

  def add_or_merge([], s, e), do: [{s, e}]
  def add_or_merge([{hs, he} | rest], s, e) when e < hs, do: [{s, e}, {hs, he} | rest]
  def add_or_merge([{hs, he} | rest], s, e) when he < s, do: [{hs, he} | add_or_merge(rest, s, e)]
  def add_or_merge([{hs, he} | rest], s, e), do: add_or_merge(rest, min(hs, s), max(he, e))

  @spec get_write(t(), k :: binary()) :: binary() | :clear | nil
  defp get_write(t, k) do
    case :gb_trees.lookup(k, t.writes) do
      {:value, v} -> v
      :none -> nil
    end
  end

  @spec get_read(t(), k :: binary()) :: binary() | :clear | nil
  defp get_read(t, k), do: Map.get(t.reads, k)

  @spec put_clear(t(), k :: binary()) :: t()
  defp put_clear(t, k), do: %{t | writes: :gb_trees.enter(k, :clear, t.writes)}

  @spec put_write(t(), k :: binary(), v :: binary()) :: t()
  defp put_write(t, k, v), do: %{t | writes: :gb_trees.enter(k, v, t.writes)}

  defp fetch(t, k, fetch_fn, state) do
    {result, new_state} = fetch_fn.(k, state)

    case result do
      {:ok, v} ->
        {%{t | reads: Map.put(t.reads, k, v)}, result, new_state}

      {:error, :not_found} ->
        {%{t | reads: Map.put(t.reads, k, :clear)}, result, new_state}

      result ->
        {t, result, new_state}
    end
  end

  @spec record_mutation(t(), mutation()) :: t()
  defp record_mutation(t, op) do
    %{t | mutations: [op | t.mutations]}
  end

  defp next_key(k), do: k <> <<0>>

  # Helper function to reduce nesting in get_range
end
