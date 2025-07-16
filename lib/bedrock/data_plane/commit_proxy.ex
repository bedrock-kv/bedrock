defmodule Bedrock.DataPlane.CommitProxy do
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout

  use Bedrock.Internal.GenServerApi, for: __MODULE__.Server

  @type ref :: pid() | atom() | {atom(), node()}

  @spec recover_from(
          commit_proxy_ref :: ref(),
          lock_token :: binary(),
          transaction_system_layout :: TransactionSystemLayout.t()
        ) :: :ok | {:error, :timeout} | {:error, :unavailable}
  def recover_from(commit_proxy, lock_token, transaction_system_layout),
    do: call(commit_proxy, {:recover_from, lock_token, transaction_system_layout}, :infinity)

  @spec commit(commit_proxy_ref :: ref(), transaction :: Bedrock.transaction()) ::
          {:ok, version :: Bedrock.version()} | {:error, :timeout | :unavailable}
  def commit(commit_proxy, transaction),
    do: call(commit_proxy, {:commit, transaction}, :infinity)
end
