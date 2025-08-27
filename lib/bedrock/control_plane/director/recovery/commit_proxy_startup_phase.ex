defmodule Bedrock.ControlPlane.Director.Recovery.CommitProxyStartupPhase do
  @moduledoc """
  Solves the scalability challenge by starting commit proxy components that coordinate
  transaction processing across multiple distributed processes.

  Commit proxies act as the critical bridge between client gateways and the core
  transaction system, batching requests and distributing coordination workload.
  Multiple proxies provide horizontal scalability—as transaction volume increases,
  more proxies handle the load without bottlenecks.

  Uses round-robin distribution to start proxies across nodes with coordination capabilities
  from `context.node_capabilities.coordination`, ensuring fault tolerance by spreading
  proxies across different machines. Each proxy is configured with epoch and director
  information for recovery coordination.

  Stalls if no coordination-capable nodes are available since at least one proxy
  must be operational for transaction processing. Proxies remain locked until
  Transaction System Layout phase transitions them to operational mode.

  See the Proxy Startup section in `docs/knowlege_base/02-deep/recovery-narrative.md`
  for detailed explanation of the scalability problem and coordination approach.
  """

  use Bedrock.ControlPlane.Director.Recovery.RecoveryPhase

  alias Bedrock.Cluster
  alias Bedrock.ControlPlane.Director.Recovery.Shared
  alias Bedrock.DataPlane.CommitProxy

  @impl true
  def execute(recovery_attempt, context) do
    start_supervised_fn =
      Map.get(context, :start_supervised_fn, fn child_spec, node ->
        sup_otp_name = recovery_attempt.cluster.otp_name(:sup)
        starter_fn = Shared.starter_for(sup_otp_name)
        starter_fn.(child_spec, node)
      end)

    available_commit_proxy_nodes = Map.get(context.node_capabilities, :coordination, [])

    context.cluster_config.parameters.desired_commit_proxies
    |> define_commit_proxies(
      recovery_attempt.cluster,
      recovery_attempt.epoch,
      self(),
      available_commit_proxy_nodes,
      start_supervised_fn,
      context.lock_token,
      context.cluster_config
    )
    |> case do
      {:error, reason} ->
        {recovery_attempt, {:stalled, reason}}

      {:ok, commit_proxies} ->
        updated_recovery_attempt = %{recovery_attempt | proxies: commit_proxies}
        {updated_recovery_attempt, Bedrock.ControlPlane.Director.Recovery.ResolverStartupPhase}
    end
  end

  @spec define_commit_proxies(
          n_proxies :: pos_integer(),
          cluster :: module(),
          Bedrock.epoch(),
          director :: pid(),
          available_nodes :: [node()],
          start_supervised :: (Supervisor.child_spec(), node() -> {:ok, pid()} | {:error, term()}),
          lock_token :: binary(),
          cluster_config :: map()
        ) ::
          {:ok, [pid()]}
          | {:error, {:failed_to_start, :commit_proxy, node(), reason :: term()}}
          | {:error,
             {:insufficient_nodes, :no_coordination_capable_nodes, requested :: pos_integer(),
              available :: non_neg_integer()}}
  def define_commit_proxies(
        n_proxies,
        cluster,
        epoch,
        director,
        available_nodes,
        start_supervised,
        lock_token,
        cluster_config
      ) do
    if Enum.empty?(available_nodes) do
      {:error, {:insufficient_nodes, :no_coordination_capable_nodes, n_proxies, 0}}
    else
      available_nodes
      |> distribute_proxies_round_robin(n_proxies)
      |> Enum.with_index()
      |> start_proxies(cluster, epoch, director, lock_token, cluster_config, start_supervised)
    end
  end

  @spec distribute_proxies_round_robin([node()], pos_integer()) :: [node()]
  defp distribute_proxies_round_robin([], _n_proxies), do: []

  defp distribute_proxies_round_robin(available_nodes, n_proxies) do
    available_nodes
    |> Stream.cycle()
    |> Enum.take(n_proxies)
  end

  @spec child_spec(
          cluster :: Cluster.t(),
          epoch :: Bedrock.epoch(),
          director :: pid(),
          lock_token :: Bedrock.lock_token(),
          cluster_config :: map(),
          instance :: non_neg_integer()
        ) ::
          Supervisor.child_spec()
  def child_spec(cluster, epoch, director, lock_token, cluster_config, instance) do
    empty_transaction_timeout_ms =
      Map.get(cluster_config.parameters, :empty_transaction_timeout_ms, 1_000)

    CommitProxy.child_spec(
      cluster: cluster,
      epoch: epoch,
      director: director,
      lock_token: lock_token,
      instance: instance,
      empty_transaction_timeout_ms: empty_transaction_timeout_ms,
      max_latency_in_ms: 1,
      max_per_batch: 10
    )
  end

  @spec start_proxies(
          [{node(), non_neg_integer()}],
          cluster :: module(),
          Bedrock.epoch(),
          director :: pid(),
          lock_token :: binary(),
          cluster_config :: map(),
          (Supervisor.child_spec(), node() -> {:ok, pid()} | {:error, term()})
        ) ::
          {:ok, [pid()]} | {:error, {:failed_to_start, :commit_proxy, node(), term()}}
  defp start_proxies(nodes_with_instances, cluster, epoch, director, lock_token, cluster_config, start_supervised) do
    nodes_with_instances
    |> Task.async_stream(
      fn {node, instance} ->
        child_spec = child_spec(cluster, epoch, director, lock_token, cluster_config, instance)

        child_spec
        |> start_supervised.(node)
        |> case do
          {:ok, pid} -> {:ok, pid}
          {:error, reason} -> {:error, {:failed_to_start, :commit_proxy, node, reason}}
        end
      end,
      ordered: true
    )
    |> Enum.reduce_while({:ok, []}, fn
      {:ok, {:ok, pid}}, {:ok, acc} -> {:cont, {:ok, [pid | acc]}}
      {:ok, {:error, reason}}, _ -> {:halt, {:error, reason}}
      {:exit, reason}, _ -> {:halt, {:error, {:failed_to_start, :commit_proxy, :unknown_node, reason}}}
    end)
    |> case do
      {:ok, pids} -> {:ok, Enum.reverse(pids)}
      error -> error
    end
  end
end
