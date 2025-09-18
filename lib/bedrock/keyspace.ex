defmodule Bedrock.Keyspace do
  @moduledoc """
  Elixir implementation of FoundationDB Subspace functionality.

  A Keyspace represents a well-defined region of keyspace in a FoundationDB database.
  It provides a convenient way to use FoundationDB tuples to define namespaces for
  different categories of data. The namespace is specified by a prefix tuple which is
  prepended to all tuples packed by the keyspace. When unpacking a key with the keyspace,
  the prefix tuple will be removed from the result.

  Based on the pattern from other FoundationDB language bindings (Go, Rust, C++).
  """

  alias Bedrock.KeyRange
  alias Bedrock.ToKeyspace

  # Define the keyspace struct
  @type t :: %__MODULE__{
          prefix: binary(),
          key_encoding: module(),
          value_encoding: module()
        }
  defstruct [:prefix, :key_encoding, :value_encoding]

  @type keyspace_opts :: [key_encoding: module(), value_encoding: module()]

  @doc """
  Create a new keyspace from raw bytes prefix.
  """
  @spec new(prefix :: binary()) :: t()
  @spec new(prefix :: binary(), opts :: [key_encoding: module(), value_encoding: module()]) :: t()
  def new(prefix, opts \\ [])
  def new(prefix, []) when is_binary(prefix), do: %__MODULE__{prefix: prefix}
  def new(prefix, opts) when is_binary(prefix), do: apply_opts(%__MODULE__{prefix: prefix}, opts)

  defp apply_opts(keyspace, opts) do
    %{
      keyspace
      | key_encoding: Keyword.get(opts, :key_encoding, keyspace.key_encoding),
        value_encoding: Keyword.get(opts, :value_encoding, keyspace.value_encoding)
    }
  end

  @doc """
  Create a keyspace that contains all keys in the database.
  """
  @spec all() :: t()
  def all, do: %__MODULE__{prefix: <<>>}

  @doc """
  Add an item to the keyspace, creating a new keyspace.
  The item will be packed as a single-element tuple and appended.
  """
  @spec add(t(), term()) :: t()
  def add(%__MODULE__{prefix: prefix}, item),
    do: %__MODULE__{prefix: :erlang.iolist_to_binary([prefix | Bedrock.Encoding.Tuple.to_iolist({item})])}

  @doc """
  Create a nested keyspace by extending this keyspace with the given binary, or term if the keyspace supports key
  encoding.
  """
  def partition(keyspace, name, opts \\ [])

  @spec partition(t(), binary(), keyspace_opts()) :: t()
  def partition(%__MODULE__{prefix: prefix, key_encoding: nil} = keyspace, name, opts) when is_binary(name),
    do: apply_opts(%{keyspace | prefix: prefix <> name}, opts)

  def partition(%__MODULE__{key_encoding: nil}, name, _opts),
    do: raise(ArgumentError, "Keyspace does not support key encoding for: #{inspect(name)}")

  @spec partition(t(), term(), keyspace_opts()) :: t()
  def partition(%__MODULE__{prefix: prefix, key_encoding: key_encoding} = keyspace, name, opts),
    do: apply_opts(%{keyspace | prefix: prefix <> key_encoding.pack(name)}, opts)

  # Catch-all: try to coerce first argument to keyspace using ToKeyspace protocol
  def partition(term, name, opts), do: term |> ToKeyspace.to_keyspace() |> partition(name, opts)

  @doc """
  """
  @spec prefix(t()) :: binary()
  def prefix(%__MODULE__{prefix: prefix}), do: prefix

  @doc """
  """
  @spec pack(t(), binary()) :: binary()
  def pack(%__MODULE__{prefix: prefix, key_encoding: nil}, key) when is_binary(key), do: prefix <> key

  @spec pack(t(), any()) :: binary()
  def pack(%__MODULE__{prefix: prefix, key_encoding: encoding}, key), do: prefix <> encoding.pack(key)
  def pack(_, _), do: raise(ArgumentError, "Key must be a binary or schema must be provided")

  @doc """
  """
  @spec unpack(t(), key :: binary()) :: term()
  def unpack(%__MODULE__{prefix: prefix, key_encoding: encoding}, key) when not is_nil(encoding) do
    case key do
      <<^prefix::binary, suffix::binary>> when not is_nil(encoding) -> encoding.unpack(suffix)
      _ -> raise(ArgumentError, "Key does not belong to this keyspace")
    end
  end

  @spec unpack(t(), key :: binary()) :: binary()
  def unpack(%__MODULE__{prefix: prefix}, key) when is_binary(key) do
    case key do
      <<^prefix::binary, suffix::binary>> -> suffix
      _ -> raise(ArgumentError, "Key does not belong to this keyspace")
    end
  end

  @doc """
  Check if a key belongs to this keyspace (starts with the prefix).
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

  def get_range_from_repo(%__MODULE__{prefix: prefix} = keyspace, repo, opts \\ []) do
    prefix_len = byte_size(prefix)
    key_decoder = key_decoder_for(keyspace.key_encoding, prefix_len)
    stream = repo.get_range(KeyRange.from_prefix(prefix), opts)

    case keyspace.value_encoding do
      nil ->
        Stream.map(stream, fn {k, v} -> {key_decoder.(k), v} end)

      value_encoding ->
        Stream.map(stream, fn {k, v} -> {key_decoder.(k), v && value_encoding.unpack(v)} end)
    end
  end

  defp key_decoder_for(nil, prefix_len), do: &binary_part(&1, prefix_len, byte_size(&1) - prefix_len)

  defp key_decoder_for(encoding, prefix_len),
    do: &encoding.unpack(binary_part(&1, prefix_len, byte_size(&1) - prefix_len))

  @doc """
  Convert the keyspace to a string representation for debugging.
  """
  @spec to_string(t()) :: String.t()
  def to_string(%__MODULE__{prefix: prefix}), do: "Keyspace<#{inspect(prefix)}>"

  defimpl String.Chars do
    defdelegate to_string(keyspace), to: Bedrock.Keyspace
  end

  defimpl Inspect do
    def inspect(%{prefix: prefix, key_encoding: nil, value_encoding: nil}, _opts),
      do: "#Keyspace<#{prefix_str(prefix)}>"

    def inspect(%{prefix: prefix, key_encoding: key_encoding, value_encoding: nil}, _opts),
      do: "#Keyspace<#{prefix_str(prefix)}|keys:#{module_name(key_encoding)}>"

    def inspect(%{prefix: prefix, key_encoding: nil, value_encoding: value_encoding}, _opts),
      do: "#Keyspace<#{prefix_str(prefix)}|values:#{module_name(value_encoding)}>"

    def inspect(%{prefix: prefix, key_encoding: key_encoding, value_encoding: value_encoding}, _opts),
      do: "#Keyspace<#{prefix_str(prefix)}|keys:#{module_name(key_encoding)},values:#{module_name(value_encoding)}>"

    defp prefix_str(prefix), do: "0x" <> :binary.encode_hex(prefix, :lowercase)
    defp module_name(module), do: module |> Module.split() |> List.last()
  end

  defimpl Bedrock.ToKeyRange do
    def to_key_range(%{prefix: <<>>}), do: {<<>>, <<0xFF>>}
    def to_key_range(%{prefix: prefix}), do: KeyRange.from_prefix(prefix)
  end
end
