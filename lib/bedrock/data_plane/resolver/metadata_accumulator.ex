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
  Removes all entries with versions strictly before the given version.

  This prunes old entries that are no longer needed, keeping memory bounded.

  ## Parameters
    - `accumulator` - The accumulator to prune
    - `before_version` - Remove entries with versions < this version

  ## Examples

      iex> acc = new()
      iex>   |> append(v(1), [{:set, <<0xFF, "a">>, "1"}])
      iex>   |> append(v(2), [{:set, <<0xFF, "b">>, "2"}])
      iex>   |> prune_before(v(2))
      iex> length(entries(acc))
      1
  """
  @spec prune_before(t(), Bedrock.version()) :: t()
  def prune_before(%__MODULE__{reversed_entries: reversed} = accumulator, before_version) do
    # Keep entries where version >= before_version (from newest end)
    pruned = Enum.take_while(reversed, fn {version, _} -> version >= before_version end)
    %{accumulator | reversed_entries: pruned}
  end
end
