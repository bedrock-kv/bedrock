defmodule Bedrock.Cluster.TransactionBuilder.Putting do
  alias Bedrock.Cluster.TransactionBuilder.State
  import Bedrock.Cluster.TransactionBuilder.KeyEncoding

  @spec do_put(State.t(), any(), any()) :: {:ok, State.t()} | :key_error
  def do_put(t, key, value) do
    with {:ok, encoded_key} <- encode_key(key) do
      %{t | writes: Map.put(t.writes, encoded_key, value)}
    end
  end
end
