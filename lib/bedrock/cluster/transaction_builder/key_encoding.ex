defmodule Bedrock.Cluster.TransactionBuilder.KeyEncoding do
  def encode_key(key) when is_binary(key), do: {:ok, key}
  def encode_key(_key), do: :key_error
end
