defmodule Bedrock.Internal.TransactionBuilder.Finalization do
  @moduledoc false

  alias Bedrock.DataPlane.CommitProxy
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.Internal.TransactionBuilder.State
  alias Bedrock.Internal.TransactionBuilder.Tx

  @spec commit(State.t()) :: {:ok, State.t()} | {:error, term()}
  @spec commit(State.t(), opts :: keyword()) :: {:ok, State.t()} | {:error, term()}
  def commit(t, opts \\ [])

  def commit(%{stack: [], tx: %{mutations: []}} = t, _opts) do
    commit_version = t.read_version || 0
    {:ok, %{t | state: :committed, commit_version: commit_version}}
  end

  def commit(%{stack: []} = t, opts) do
    commit_fn = Keyword.get(opts, :commit_fn, &CommitProxy.commit/3)
    # Use epoch from transaction_system_layout, not hardcoded 0
    epoch = Keyword.get(opts, :epoch, t.transaction_system_layout.epoch)
    transaction = prepare_transaction_for_commit(t.read_version, t.tx)

    with {:ok, commit_proxy} <- select_commit_proxy(t.transaction_system_layout),
         {:ok, version, _sequence} <- commit_fn.(commit_proxy, epoch, transaction) do
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
    Tx.commit(tx, read_version)
  end

  @spec select_commit_proxy(Bedrock.ControlPlane.Config.TransactionSystemLayout.t()) ::
          {:ok, CommitProxy.ref()} | {:error, :unavailable}
  defp select_commit_proxy(%{proxies: []}), do: {:error, :unavailable}
  defp select_commit_proxy(%{proxies: proxies}), do: {:ok, Enum.random(proxies)}

  @spec rollback(State.t()) :: :stop | State.t()
  def rollback(%{stack: []}), do: :stop
  def rollback(%{stack: [tx | stack]} = t), do: %{t | tx: tx, stack: stack}
end
