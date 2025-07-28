defmodule Bedrock.ControlPlane.Director.Recovery.CommitProxyPhase do
  @moduledoc """
  Starts commit proxy components that batch transactions and coordinate commits.

  Commit proxies receive transactions from gateways, batch them for efficiency,
  and coordinate the commit process with sequencer, resolvers, and logs. Multiple
  proxies run concurrently to provide horizontal scalability.

  Starts the desired number of commit proxy processes distributed across
  available nodes. Each proxy is configured with the sequencer and resolver
  information needed for transaction processing.

  Can stall if insufficient nodes are available or if proxy startup fails.
  At least one commit proxy must be operational for the cluster to accept
  transactions.

  Transitions to :define_resolvers to continue starting transaction system
  components.
  """

  alias Bedrock.Cluster
  alias Bedrock.DataPlane.CommitProxy
  alias Bedrock.ControlPlane.Config.RecoveryAttempt
  alias Bedrock.ControlPlane.Director.Recovery.Shared
  alias Bedrock.ControlPlane.Director.Recovery.RecoveryPhase
  @behaviour RecoveryPhase

  @impl true
  def execute(%RecoveryAttempt{state: :define_commit_proxies} = recovery_attempt, context) do
    start_supervised_fn =
      Map.get(context, :start_supervised_fn, fn child_spec, node ->
        sup_otp_name = recovery_attempt.cluster.otp_name(:sup)
        starter_fn = Shared.starter_for(sup_otp_name)
        starter_fn.(child_spec, node)
      end)

    define_commit_proxies(
      context.cluster_config.parameters.desired_commit_proxies,
      recovery_attempt.cluster,
      recovery_attempt.epoch,
      self(),
      Node.list(),
      start_supervised_fn,
      context.lock_token
    )
    |> case do
      {:error, reason} ->
        %{recovery_attempt | state: {:stalled, reason}}

      {:ok, commit_proxies} ->
        %{recovery_attempt | proxies: commit_proxies, state: :define_resolvers}
    end
  end

  @spec define_commit_proxies(
          n_proxies :: pos_integer(),
          cluster :: module(),
          Bedrock.epoch(),
          director :: pid(),
          available_nodes :: [node()],
          start_supervised :: (Supervisor.child_spec(), node() -> {:ok, pid()} | {:error, term()}),
          lock_token :: binary()
        ) ::
          {:ok, [pid()]} | {:error, {:failed_to_start, :commit_proxy, node(), reason :: term()}}
  def define_commit_proxies(
        n_proxies,
        cluster,
        epoch,
        director,
        available_nodes,
        start_supervised,
        lock_token
      ) do
    child_spec = child_spec_for_commit_proxy(cluster, epoch, director, lock_token)

    available_nodes
    |> Enum.take(n_proxies)
    |> Task.async_stream(
      fn node ->
        start_supervised.(child_spec, node)
        |> case do
          {:ok, pid} -> {node, pid}
          {:error, reason} -> {node, {:error, reason}}
        end
      end,
      ordered: false
    )
    |> Enum.reduce_while([], fn
      {:ok, {_node, pid}}, pids when is_pid(pid) ->
        {:cont, [pid | pids]}

      {:ok, {node, {:error, reason}}}, _ ->
        {:halt, {:error, {:failed_to_start, :commit_proxy, node, reason}}}

      {:exit, {node, reason}}, _ ->
        {:halt, {:error, {:failed_to_start, :commit_proxy, node, reason}}}
    end)
    |> case do
      {:error, reason} -> {:error, reason}
      pids -> {:ok, pids}
    end
  end

  @spec child_spec_for_commit_proxy(
          cluster :: Cluster.t(),
          epoch :: Bedrock.epoch(),
          director :: pid(),
          lock_token :: Bedrock.lock_token()
        ) ::
          Supervisor.child_spec()
  def child_spec_for_commit_proxy(cluster, epoch, director, lock_token) do
    CommitProxy.child_spec(
      cluster: cluster,
      epoch: epoch,
      director: director,
      lock_token: lock_token
    )
  end
end
