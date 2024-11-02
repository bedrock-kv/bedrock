defmodule Bedrock.ControlPlane.ClusterController.Recovery.DefiningProxiesAndResolvers do
  alias Bedrock.DataPlane.CommitProxy
  alias Bedrock.DataPlane.Resolver

  @spec define_commit_proxies(
          n_proxies :: pos_integer(),
          Bedrock.epoch(),
          controller :: pid(),
          nodes :: [node()],
          supervisor_otp_name :: atom()
        ) :: {:ok, [pid()]} | {:error, {:failed_to_start_proxy, node(), reason :: term()}}
  def define_commit_proxies(n_proxies, epoch, controller, nodes, supervisor_otp_name) do
    child_spec =
      child_spec_for_proxy(epoch, controller)

    nodes
    |> Enum.take(n_proxies)
    |> Task.async_stream(
      fn node ->
        DynamicSupervisor.start_child({supervisor_otp_name, node}, child_spec)
        |> case do
          {:ok, pid} -> {node, pid}
          {:ok, pid, _info} -> {node, pid}
          {:error, {:already_started, pid}} -> {node, pid}
          {:error, reason} -> {node, {:error, reason}}
        end
      end,
      ordered: false
    )
    |> Enum.reduce_while([], fn
      {:ok, {_node, pid}}, pids when is_pid(pid) ->
        {:cont, [pid | pids]}

      {:ok, {node, {:error, reason}}}, _ ->
        {:halt, {:error, {:failed_to_start_proxy, node, reason}}}

      {:exit, {node, reason}}, _ ->
        {:halt, {:error, {:failed_to_stary_proxy, node, reason}}}
    end)
    |> case do
      {:error, reason} -> {:error, reason}
      pids -> {:ok, pids}
    end
  end

  @spec child_spec_for_proxy(epoch :: Bedrock.epoch(), controller :: pid()) ::
          Supervisor.child_spec()
  def child_spec_for_proxy(epoch, controller) do
    CommitProxy.child_spec(
      epoch: epoch,
      controller: controller
    )
  end

  def define_resolvers(n_resolvers, logs) do
    {:ok, []}
  end
end
