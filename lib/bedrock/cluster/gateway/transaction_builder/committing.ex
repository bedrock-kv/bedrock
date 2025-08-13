defmodule Bedrock.Cluster.Gateway.TransactionBuilder.Committing do
  @moduledoc false

  alias Bedrock.Cluster.Gateway.TransactionBuilder.State
  alias Bedrock.DataPlane.CommitProxy

  @spec do_commit(State.t()) :: {:ok, State.t()} | {:error, term()}
  @spec do_commit(State.t(), opts :: keyword()) :: {:ok, State.t()} | {:error, term()}
  def do_commit(t, opts \\ [])

  def do_commit(%{stack: []} = t, opts) do
    commit_fn = Keyword.get(opts, :commit_fn, &CommitProxy.commit/2)

    with transaction <- prepare_transaction_for_commit(t.read_version, t.reads, t.writes),
         {:ok, commit_proxy} <- select_commit_proxy(t.transaction_system_layout),
         {:ok, version} <- commit_fn.(commit_proxy, transaction) do
      {:ok, %{t | state: :committed, commit_version: version}}
    end
  end

  def do_commit(%{stack: [{reads, writes} | stack]} = t, _opts) do
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

  defp prepare_transaction_for_commit(_read_version, %{} = reads, %{} = writes)
       when map_size(reads) == 0,
       do: {nil, writes}

  defp prepare_transaction_for_commit(read_version, %{} = reads, %{} = writes),
    do: {{read_version, reads |> Map.keys()}, writes}

  @spec select_commit_proxy(Bedrock.ControlPlane.Config.TransactionSystemLayout.t()) ::
          {:ok, CommitProxy.ref()} | {:error, :unavailable}
  defp select_commit_proxy(%{proxies: []}), do: {:error, :unavailable}
  defp select_commit_proxy(%{proxies: proxies}), do: {:ok, Enum.random(proxies)}
end
