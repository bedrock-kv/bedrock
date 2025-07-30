defmodule Bedrock.Cluster.Gateway.TransactionBuilder.Putting do
  @moduledoc false

  alias Bedrock.Cluster.Gateway.TransactionBuilder.State

  @spec do_put(State.t(), Bedrock.key(), Bedrock.value()) :: {:ok, State.t()} | :key_error
  def do_put(t, key, value) do
    with {:ok, encoded_key} <- t.key_codec.encode_key(key),
         {:ok, encoded_value} <- t.value_codec.encode_value(value) do
      {:ok, %{t | writes: Map.put(t.writes, encoded_key, encoded_value)}}
    end
  end
end
