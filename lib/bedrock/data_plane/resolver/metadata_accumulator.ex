defmodule Bedrock.DataPlane.Resolver.MetadataAccumulator do
  @moduledoc """
  Manages a version-ordered window of metadata mutations.

  The accumulator maintains metadata mutations in version order, allowing
  efficient retrieval of mutations since a given version and pruning of
  old entries. This enables differential updates to be returned to proxies.

  Internally, entries are stored in reverse order (newest first) for O(1) append.
  """

  @type mutation :: Bedrock.Internal.TransactionBuilder.Tx.mutation()

  @type entry :: {version :: Bedrock.version(), mutations :: [mutation()]}

  @type t :: %__MODULE__{
          reversed_entries: [entry()]
        }

  defstruct reversed_entries: []

  @doc """
  Creates a new empty metadata accumulator.

  ## Examples

      iex> entries(new())
      []
  """
  @spec new() :: t()
  def new, do: %__MODULE__{}

  @doc """
  Returns all entries in version order (oldest first).

  ## Examples

      iex> acc = new() |> append(v(1), [{:set, <<0xFF, "a">>, "1"}])
      iex> entries(acc)
      [{<<0, 0, 0, 0, 0, 0, 0, 1>>, [{:set, <<0xFF, "a">>, "1"}]}]
  """
  @spec entries(t()) :: [entry()]
  def entries(%__MODULE__{reversed_entries: reversed}), do: Enum.reverse(reversed)

  @doc """
  Appends mutations at a given version to the accumulator.

  Mutations are stored in version order. If mutations is empty, this is a no-op.

  ## Parameters
    - `accumulator` - The accumulator to append to
    - `version` - The commit version for these mutations
    - `mutations` - List of metadata mutations to append

  ## Examples

      iex> acc = new() |> append(v(1), [{:set, <<0xFF, "key">>, "value"}])
      iex> length(entries(acc))
      1
  """
  @spec append(t(), Bedrock.version(), [mutation()]) :: t()
  def append(accumulator, _version, []), do: accumulator

  def append(%__MODULE__{reversed_entries: reversed} = accumulator, version, mutations) do
    %{accumulator | reversed_entries: [{version, mutations} | reversed]}
  end

  @doc """
  Flattens per-transaction metadata mutations, keeping only transactions that
  survived the abort set (indices are batch positions), in transaction order.

  Shared by immediate accumulation (resolver, local abort set) and deferred
  confirmation (commit proxy, merged global abort set) so both sides agree on
  which mutations a batch committed.

  ## Examples

      iex> committed_mutations([[{:set, <<0xFF, "a">>, "1"}], [{:set, <<0xFF, "b">>, "2"}]], MapSet.new([1]))
      [{:set, <<0xFF, "a">>, "1"}]
  """
  @spec committed_mutations([[mutation()]], MapSet.t(non_neg_integer())) :: [mutation()]
  def committed_mutations(metadata_per_tx, aborted_set) do
    metadata_per_tx
    |> Enum.with_index()
    |> Enum.reject(fn {_mutations, idx} -> MapSet.member?(aborted_set, idx) end)
    |> Enum.flat_map(fn {mutations, _idx} -> mutations end)
  end

  @doc """
  Inserts mutations at a given version, maintaining version order even when
  the version is older than existing entries.

  Used for deferred (confirmed-later) metadata in sharded-resolver mode, where
  confirmations for different versions can arrive out of order. If mutations
  is empty, this is a no-op.

  ## Examples

      iex> acc = new()
      iex>   |> append(v(2), [{:set, <<0xFF, "b">>, "2"}])
      iex>   |> insert_sorted(v(1), [{:set, <<0xFF, "a">>, "1"}])
      iex> Enum.map(entries(acc), &elem(&1, 0))
      [<<0, 0, 0, 0, 0, 0, 0, 1>>, <<0, 0, 0, 0, 0, 0, 0, 2>>]
  """
  @spec insert_sorted(t(), Bedrock.version(), [mutation()]) :: t()
  def insert_sorted(accumulator, _version, []), do: accumulator

  def insert_sorted(%__MODULE__{reversed_entries: reversed} = accumulator, version, mutations) do
    {newer, older} = Enum.split_while(reversed, fn {v, _} -> v > version end)
    %{accumulator | reversed_entries: newer ++ [{version, mutations} | older]}
  end

  @doc """
  Returns all mutations since (but not including) the given version.

  Returns mutations in version order (oldest first). If `since_version` is nil,
  returns all mutations in the accumulator.

  ## Parameters
    - `accumulator` - The accumulator to query
    - `since_version` - Return mutations after this version (exclusive), or nil for all

  ## Examples

      iex> acc = new()
      iex>   |> append(v(1), [{:set, <<0xFF, "a">>, "1"}])
      iex>   |> append(v(2), [{:set, <<0xFF, "b">>, "2"}])
      iex> mutations_since(acc, v(1))
      [{<<0, 0, 0, 0, 0, 0, 0, 2>>, [{:set, <<0xFF, "b">>, "2"}]}]
  """
  @spec mutations_since(t(), Bedrock.version() | nil) :: [entry()]
  def mutations_since(%__MODULE__{reversed_entries: reversed}, nil), do: Enum.reverse(reversed)

  def mutations_since(%__MODULE__{reversed_entries: reversed}, since_version) do
    collect_since(reversed, since_version, [])
  end

  # Traverse from newest, collect entries newer than since_version
  defp collect_since([], _since_version, acc), do: acc

  defp collect_since([{version, _} | _rest], since_version, acc) when version <= since_version do
    acc
  end

  defp collect_since([entry | rest], since_version, acc) do
    collect_since(rest, since_version, [entry | acc])
  end

  @doc """
  Removes all entries with versions at or below the given version.

  This prunes entries every proxy has confirmed applying (they can never be
  requested again - `mutations_since/2` is exclusive), keeping memory bounded.

  ## Parameters
    - `accumulator` - The accumulator to prune
    - `through_version` - Remove entries with versions <= this version

  ## Examples

      iex> acc = new()
      iex>   |> append(v(1), [{:set, <<0xFF, "a">>, "1"}])
      iex>   |> append(v(2), [{:set, <<0xFF, "b">>, "2"}])
      iex>   |> prune_through(v(1))
      iex> length(entries(acc))
      1
  """
  @spec prune_through(t(), Bedrock.version()) :: t()
  def prune_through(%__MODULE__{reversed_entries: reversed} = accumulator, through_version) do
    # Keep entries where version > through_version (from newest end)
    pruned = Enum.take_while(reversed, fn {version, _} -> version > through_version end)
    %{accumulator | reversed_entries: pruned}
  end

  @doc """
  Returns the newest entry version at or below the given version, or nil if
  there is none.

  Used to record the exact coverage lost by a `prune_through/2` call: a proxy
  whose ack is at or above this version has confirmed every discarded entry,
  even when the prune version itself (a commit-stream version) is far ahead of
  its ack.
  """
  @spec newest_version_at_or_below(t(), Bedrock.version()) :: Bedrock.version() | nil
  def newest_version_at_or_below(%__MODULE__{reversed_entries: reversed}, version) do
    Enum.find_value(reversed, fn {v, _} -> if v <= version, do: v end)
  end
end
