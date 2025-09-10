defmodule Bedrock.Subspace do
  @moduledoc """
  Elixir implementation of FoundationDB Subspace functionality.

  A Subspace represents a well-defined region of keyspace in a FoundationDB database.
  It provides a convenient way to use FoundationDB tuples to define namespaces for
  different categories of data. The namespace is specified by a prefix tuple which is
  prepended to all tuples packed by the subspace. When unpacking a key with the subspace,
  the prefix tuple will be removed from the result.

  Based on the pattern from other FoundationDB language bindings (Go, Rust, C++).
  """
  alias Bedrock.Key

  # Define the subspace struct
  defstruct [:prefix]

  @type t :: %__MODULE__{prefix: binary()}

  @doc """
  Create a new subspace from raw bytes prefix.
  """
  @spec new(binary()) :: t()
  def new(prefix) when is_binary(prefix), do: %__MODULE__{prefix: prefix}

  @spec new(tuple()) :: t()
  def new(tuple) when is_tuple(tuple), do: %__MODULE__{prefix: Key.pack(tuple)}

  @doc """
  Create a subspace that contains all keys in the database.
  """
  @spec all() :: t()
  def all, do: %__MODULE__{prefix: <<>>}

  @doc """
  Create a new subspace with the given tuple and raw prefix.
  The tuple is packed and appended to the raw prefix.
  """
  @spec create(t(), tuple()) :: t()
  def create(%__MODULE__{prefix: prefix}, tuple) when is_tuple(tuple),
    do: %__MODULE__{prefix: :erlang.iolist_to_binary([prefix | Key.to_iolist(tuple)])}

  @doc """
  Add an item to the subspace, creating a new subspace.
  The item will be packed as a single-element tuple and appended.
  """
  @spec add(t(), term()) :: t()
  def add(%__MODULE__{prefix: prefix}, item),
    do: %__MODULE__{prefix: :erlang.iolist_to_binary([prefix | Key.to_iolist({item})])}

  @doc """
  Create a nested subspace by extending this subspace with the given tuple.
  """
  @spec subspace(t(), tuple()) :: t()
  def subspace(%__MODULE__{} = subspace, tuple) when is_tuple(tuple), do: create(subspace, tuple)

  @doc """
  Get the raw prefix bytes of the subspace.
  """
  @spec key(t()) :: binary()
  def key(%__MODULE__{prefix: prefix}), do: prefix

  @doc """
  Get the raw prefix bytes of the subspace (alias for key/1).
  """
  @spec bytes(t()) :: binary()
  def bytes(%__MODULE__{} = subspace), do: key(subspace)

  @doc """
  Pack a list or tuple within this subspace.
  The list/tuple will be packed and prefixed with the subspace's prefix.
  """
  @spec pack(t(), list() | tuple()) :: binary()
  def pack(%__MODULE__{prefix: prefix}, value) when is_list(value) or is_tuple(value),
    do: :erlang.iolist_to_binary([prefix | Key.to_iolist(value)])

  # @doc """
  # Pack a tuple with version stamp within this subspace.
  # """
  # @spec pack_vs(t(), tuple()) :: binary()
  # def pack_vs(%__MODULE__{prefix: prefix}, tuple) when is_tuple(tuple), do: prefix <> Tuple.pack_vs(tuple)

  @doc """
  Unpack a key that belongs to this subspace.
  Returns the list or tuple with the subspace prefix removed.
  Raises an error if the key doesn't belong to this subspace.
  """
  @spec unpack(t(), binary()) :: list() | tuple()
  def unpack(%__MODULE__{prefix: prefix}, key) when is_binary(key) do
    case key do
      <<^prefix::binary, remaining_key::binary>> -> Key.unpack(remaining_key)
      _ -> raise(ArgumentError, "Key does not belong to this subspace")
    end
  end

  @doc """
  Check if a key belongs to this subspace (starts with the prefix).
  """
  @spec contains?(t(), binary()) :: boolean()
  def contains?(%__MODULE__{prefix: prefix}, key) when is_binary(key), do: _contains?(key, prefix)

  # Private helper function
  defp _contains?(key, prefix) when is_binary(key) and is_binary(prefix) do
    case key do
      <<^prefix::binary, _rest::binary>> -> true
      _ -> false
    end
  end

  @doc """
  Get the range of keys that belong to this subspace.
  Returns a tuple of {start_key, end_key} suitable for range operations.
  """
  @spec range(t()) :: {binary(), binary()}
  def range(%__MODULE__{prefix: <<>>}), do: {<<>>, <<0xFF>>}
  def range(%__MODULE__{prefix: prefix}), do: {prefix <> <<0x00>>, prefix <> <<0xFF>>}

  @doc """
  Get the range of keys for a tuple within this subspace.
  Returns a tuple of {start_key, end_key} suitable for range operations.
  """
  @spec range(t(), tuple() | binary()) :: {binary(), binary()}
  def range(%__MODULE__{prefix: prefix}, key) when is_tuple(key), do: new_range(prefix, Key.to_iolist(key))
  def range(%__MODULE__{prefix: prefix}, key) when is_binary(key), do: new_range(prefix, key)

  defp new_range(prefix, key),
    do: {:erlang.iolist_to_binary([prefix, key, 0x00]), :erlang.iolist_to_binary([prefix, key, 0xFF])}

  @doc """
  Convert the subspace to a string representation for debugging.
  """
  @spec to_string(t()) :: String.t()
  def to_string(%__MODULE__{prefix: prefix}), do: "Subspace<#{inspect(prefix)}>"

  defimpl String.Chars do
    defdelegate to_string(subspace), to: Bedrock.Subspace
  end

  defimpl Inspect do
    def inspect(%Bedrock.Subspace{prefix: prefix}, _opts), do: "#Subspace<#{inspect(prefix)}>"
  end
end
