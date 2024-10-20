defmodule Bedrock.DataPlane.CommitProxy do
  use Bedrock.Internal.GenServerApi

  @type ref :: GenServer.name()

  @spec commit(commit_proxy :: ref(), Bedrock.transaction()) ::
          {:ok, Bedrock.version()} | {:error, :timeout | :unavailable}
  def commit(commit_proxy, transaction),
    do: call(commit_proxy, {:commit, transaction}, :infinity)
end
