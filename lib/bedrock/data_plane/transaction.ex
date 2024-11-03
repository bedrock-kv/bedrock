defmodule Bedrock.DataPlane.Transaction do
  @type t ::
          {Bedrock.version() | binary(),
           %{
             Bedrock.key() => Bedrock.value() | nil,
             Bedrock.key_range() => nil
           }
           | binary()}

  @doc """
  Create a new transaction.
  """
  @spec new(Bedrock.version() | binary(), map() | binary()) :: t()
  def new(version, [{_key, _value} | _] = key_values), do: {version, Map.new(key_values)}
  def new(version, []), do: {version, %{}}
  def new(version, key_values), do: {version, key_values}

  @doc """
  Get the version from the transaction.
  """
  @spec version(t()) :: Bedrock.version()
  def version({<<version::big-unsigned-integer-size(64)>>, _}), do: version
  def version({version, _}) when is_integer(version), do: version

  @doc """
  Get the key-values from the transaction. If they have been previously encoded,
  they will be decoded before being returned.
  """
  @spec key_values(t()) :: %{Bedrock.key() => Bedrock.value() | nil, Bedrock.key_range() => nil}
  def key_values({_, %{} = key_values}), do: key_values

  def key_values({_, <<key_values::binary>>}),
    do: :erlang.binary_to_term(key_values)

  @doc """
  Ensure that a transaction is decoded from on-disk form. If the transaction
  has already been decoded then no work is performed.
  """
  @spec decode(t()) :: t()
  def decode({version, key_values} = transaction) when is_integer(version) and is_map(key_values),
    do: transaction

  def decode({version, key_values}),
    do: {decode_version(version), decode_key_values(key_values)}

  defp decode_version(<<version::big-unsigned-integer-size(64)>>), do: version
  defp decode_version(version) when is_integer(version), do: version

  defp decode_key_values(%{} = key_values), do: key_values
  defp decode_key_values(<<key_values::bytes>>), do: key_values |> :erlang.binary_to_term()

  @doc """
  Ensure that a transaction is encoded into on-disk form. If a transaction has
  already been encoded then no work is performed.
  """
  @spec encode(t()) :: t()
  def encode({<<_version::binary>>, <<_key_values::binary>>} = transaction), do: transaction

  def encode({version, key_values}),
    do: {encode_version(version), encode_key_values(key_values)}

  defp encode_version(version) when is_integer(version),
    do: <<version::big-unsigned-integer-size(64)>>

  defp encode_version(version), do: version

  defp encode_key_values(key_values) when is_map(key_values),
    do: key_values |> :erlang.term_to_binary()

  defp encode_key_values(key_values), do: key_values
end
