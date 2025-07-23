defmodule Bedrock.ControlPlane.Director.Recovery.CommitProxyPhase do
  @moduledoc """
  Handles the :define_commit_proxies phase of recovery.

  This phase is responsible for starting commit proxy components
  which batch transactions and coordinate commits.

  See: [Recovery Guide](docs/knowledge_base/01-guides/recovery-guide.md#recovery-process)
  """

  alias Bedrock.Cluster
  alias Bedrock.DataPlane.CommitProxy
  alias Bedrock.ControlPlane.Config.RecoveryAttempt
  alias Bedrock.ControlPlane.Director.Recovery.Shared
  alias Bedrock.ControlPlane.Director.Recovery.RecoveryPhase
  @behaviour RecoveryPhase

  @doc """
  Execute the commit proxy definition phase of recovery.

  Starts the desired number of commit proxy components across
  available nodes.
  """
  @impl true
  def execute(%RecoveryAttempt{state: :define_commit_proxies} = recovery_attempt, context) do
    sup_otp_name = recovery_attempt.cluster.otp_name(:sup)
    starter_fn = Shared.starter_for(sup_otp_name)

    define_commit_proxies(
      context.cluster_config.parameters.desired_commit_proxies,
      recovery_attempt.cluster,
      recovery_attempt.epoch,
      self(),
      Node.list(),
      starter_fn,
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
