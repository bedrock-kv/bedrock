defmodule Bedrock.DataPlane.CommitProxy do
  use Bedrock.Internal.GenServerApi, for: __MODULE__.Server

  @type ref :: GenServer.server()

  @spec recover_from(
          ref(),
          lock_token :: binary(),
          transaction_system_layout :: term()
        ) :: :ok | {:error, :timeout} | {:error, :unavailable}
  def recover_from(commit_proxy, lock_token, transaction_system_layout),
    do: call(commit_proxy, {:recover_from, lock_token, transaction_system_layout}, :infinity)

  @spec commit(commit_proxy :: ref(), Bedrock.transaction()) ::
          {:ok, Bedrock.version()} | {:error, :timeout | :unavailable}
  def commit(commit_proxy, transaction),
    do: call(commit_proxy, {:commit, transaction}, :infinity)
end
