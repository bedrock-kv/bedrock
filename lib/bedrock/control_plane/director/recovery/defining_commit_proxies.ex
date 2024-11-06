defmodule Bedrock.ControlPlane.Director.Recovery.DefiningCommitProxies do
  alias Bedrock.DataPlane.CommitProxy

  #
  #
  #
  @spec define_commit_proxies(
          n_proxies :: pos_integer(),
          cluster :: module(),
          Bedrock.epoch(),
          director :: pid(),
          available_nodes :: [node()],
          start_supervised :: (Supervisor.child_spec(), node() -> {:ok, pid()} | {:error, term()})
        ) ::
          {:ok, [pid()]} | {:error, {:failed_to_start, :commit_proxy, node(), reason :: term()}}
  def define_commit_proxies(
        n_proxies,
        cluster,
        epoch,
        director,
        available_nodes,
        start_supervised
      ) do
    child_spec = child_spec_for_commit_proxy(cluster, epoch, director)

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
          cluster :: module(),
          epoch :: Bedrock.epoch(),
          director :: pid()
        ) ::
          Supervisor.child_spec()
  def child_spec_for_commit_proxy(cluster, epoch, director) do
    CommitProxy.child_spec(
      cluster: cluster,
      epoch: epoch,
      director: director
    )
  end
end
