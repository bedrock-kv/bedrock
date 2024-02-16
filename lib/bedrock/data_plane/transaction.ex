defmodule Bedrock.DataPlane.Transaction do
  @type version :: binary()
  @type key_value :: Bedrock.key_value()

  @type t :: {version(), [key_value()] | binary() | nil}

  @doc """
  Create a new transaction.
  """
  @spec new(version(), [key_value()] | binary()) :: t()
  def new(version, key_values),
    do: {version, key_values}

  @doc """
  Get the version from the transaction.
  """
  @spec version(t()) :: version()
  def version({version, _}), do: version

  @doc """
  Get the key-values from the transaction. If they have been previously encoded,
  they will be decoded before being returned.
  """
  @spec key_values(t()) :: [key_value()]
  def key_values({_, key_values}) when is_list(key_values), do: key_values

  def key_values({_, key_values}) when is_binary(key_values),
    do: :erlang.binary_to_term(key_values)

  @doc """
  Ensure that a transaction is decoded from on-disk form. If the transaction
  has already been decoded then no work is performed.
  """
  @spec decode(t()) :: t()
  def decode({_version, key_values} = transaction) when is_list(key_values),
    do: transaction

  def decode({version, key_values}) when is_binary(key_values),
    do: {version, key_values |> :erlang.binary_to_term()}

  @doc """
  Ensure that a transaction is encoded into on-disk form. If a transaction has
  already been encoded then no work is performed.
  """
  @spec encode(t()) :: t()
  def encode({_version, key_values} = transaction) when is_binary(key_values),
    do: transaction

  def encode({version, key_values}) when is_list(key_values),
    do: {version, key_values |> :erlang.term_to_binary()}
end
