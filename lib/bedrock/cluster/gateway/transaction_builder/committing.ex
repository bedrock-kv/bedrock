defmodule Bedrock.Cluster.Gateway.TransactionBuilder.Committing do
  @moduledoc false

  alias Bedrock.Cluster.Gateway.TransactionBuilder.State
  alias Bedrock.Cluster.Gateway.TransactionBuilder.Tx
  alias Bedrock.DataPlane.CommitProxy
  alias Bedrock.DataPlane.Transaction

  @spec commit(State.t()) :: {:ok, State.t()} | {:error, term()}
  @spec commit(State.t(), opts :: keyword()) :: {:ok, State.t()} | {:error, term()}
  def commit(t, opts \\ [])

  def commit(%{stack: []} = t, opts) do
    commit_fn = Keyword.get(opts, :commit_fn, &CommitProxy.commit/2)

    transaction = prepare_transaction_for_commit(t.read_version, t.tx)

    with {:ok, commit_proxy} <- select_commit_proxy(t.transaction_system_layout),
         {:ok, version} <- commit_fn.(commit_proxy, transaction) do
      {:ok, %{t | state: :committed, commit_version: version}}
    end
  end

  def commit(%{stack: [_tx | stack]} = t, _opts), do: {:ok, %{t | stack: stack}}

  @spec prepare_transaction_for_commit(
          read_version :: Bedrock.version() | nil,
          tx :: Tx.t()
        ) ::
          Transaction.encoded()
  defp prepare_transaction_for_commit(read_version, tx) do
    Tx.commit_binary(tx, read_version)
  end

  @spec select_commit_proxy(Bedrock.ControlPlane.Config.TransactionSystemLayout.t()) ::
          {:ok, CommitProxy.ref()} | {:error, :unavailable}
  defp select_commit_proxy(%{proxies: []}), do: {:error, :unavailable}
  defp select_commit_proxy(%{proxies: proxies}), do: {:ok, Enum.random(proxies)}
end
