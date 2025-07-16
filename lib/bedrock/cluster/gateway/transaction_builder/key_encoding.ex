defmodule Bedrock.Cluster.Gateway.TransactionBuilder.KeyEncoding do
  @spec encode_key(key :: binary()) :: {:ok, binary()} | :key_error
  def encode_key(key) when is_binary(key), do: {:ok, key}
  def encode_key(_key), do: :key_error
end
