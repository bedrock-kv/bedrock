defmodule Bedrock.Cluster.Gateway.TransactionBuilder.Committing do
  @moduledoc false

  alias Bedrock.Cluster.Gateway.TransactionBuilder.State
  alias Bedrock.DataPlane.CommitProxy

  @spec do_commit(State.t()) :: {:ok, State.t()} | {:error, term()}
  def do_commit(%{stack: []} = t) do
    with transaction <- prepare_transaction_for_commit(t.read_version, t.reads, t.writes),
         {:ok, commit_proxy} <- select_commit_proxy(t.transaction_system_layout),
         {:ok, version} <- CommitProxy.commit(commit_proxy, transaction) do
      {:ok, %{t | state: :committed, commit_version: version}}
    end
  end

  def do_commit(%{stack: [{reads, writes} | stack]} = t) do
    {:ok,
     %{
       t
       | reads: Map.merge(t.reads, reads),
         writes: Map.merge(t.writes, writes),
         stack: stack
     }}
  end

  @spec prepare_transaction_for_commit(
          Bedrock.version() | nil,
          reads :: %{Bedrock.key() => Bedrock.value()},
          writes :: %{Bedrock.key() => Bedrock.value()}
        ) ::
          {nil | {Bedrock.version(), [Bedrock.key()]}, %{Bedrock.key() => Bedrock.value()}}
  defp prepare_transaction_for_commit(nil, _, %{} = writes),
    do: {nil, writes}

  defp prepare_transaction_for_commit(read_version, %{} = reads, %{} = writes)
       when map_size(reads) > 0 do
    {{read_version, reads |> Map.keys()}, writes}
  end

  @spec select_commit_proxy(Bedrock.ControlPlane.Config.TransactionSystemLayout.t()) ::
          {:ok, CommitProxy.ref()} | {:error, :unavailable}
  defp select_commit_proxy(%{proxies: []}), do: {:error, :unavailable}
  defp select_commit_proxy(%{proxies: proxies}), do: {:ok, Enum.random(proxies)}
end
