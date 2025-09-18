defprotocol Bedrock.ToKeyRange do
  @moduledoc """
  Protocol for converting various types to key ranges.

  A key range is represented as a tuple `{start_key, end_key}` where:
  - `start_key` is inclusive
  - `end_key` is exclusive, or the atom `:end` for unbounded ranges

  This protocol provides a unified interface for range operations in Bedrock,
  allowing directories, keyspaces, and other types to be used wherever a
  key range is expected.
  """

  @type key_range :: {binary(), binary() | :end}

  @doc """
  Converts the given data to a key range tuple.

  Returns a tuple `{start_key, end_key}` where `end_key` may be a binary
  or the atom `:end` for unbounded ranges.
  """
  @spec to_key_range(term()) :: key_range()
  def to_key_range(data)
end

defprotocol Bedrock.ToKeyspace do
  @moduledoc """
  Protocol for converting various types to keyspaces.

  This protocol allows directories, prefixes, and other types to be
  automatically coerced into `Bedrock.Keyspace` structs, enabling
  more ergonomic APIs.
  """

  @doc """
  Converts the given data to a keyspace.

  Returns a `Bedrock.Keyspace` struct that can be used for data operations.
  """
  @spec to_keyspace(term()) :: Bedrock.Keyspace.t()
  def to_keyspace(data)
end

# Implementation for tuples (explicit ranges)
defimpl Bedrock.ToKeyRange, for: Tuple do
  def to_key_range({start_key, end_key}) when is_binary(start_key) and is_binary(end_key) and start_key < end_key,
    do: {start_key, end_key}

  def to_key_range(tuple) do
    raise ArgumentError, "ToKeyRange expects a 2-tuple with binary start key, got: #{inspect(tuple)}"
  end
end

# Implementation for binaries (prefix ranges)
defimpl Bedrock.ToKeyRange, for: BitString do
  alias Bedrock.KeyRange

  def to_key_range(prefix) when is_binary(prefix), do: KeyRange.from_prefix(prefix)
end

# ToKeyspace implementations

# Implementation for Keyspace (identity)
defimpl Bedrock.ToKeyspace, for: Bedrock.Keyspace do
  def to_keyspace(keyspace), do: keyspace
end

# Implementation for binaries (create keyspace from prefix)
defimpl Bedrock.ToKeyspace, for: BitString do
  def to_keyspace(prefix) when is_binary(prefix), do: Bedrock.Keyspace.new(prefix)
end
