defmodule Bedrock.ControlPlane.Director.Recovery.DefiningProxiesAndResolvers do
  alias Bedrock.DataPlane.CommitProxy
  alias Bedrock.DataPlane.Resolver
  alias Bedrock.DataPlane.Log

  #
  #
  #
  @spec define_commit_proxies(
          n_proxies :: pos_integer(),
          cluster :: module(),
          Bedrock.epoch(),
          director :: pid(),
          available_nodes :: [node()],
          supervisor_otp_name :: atom()
        ) :: {:ok, [pid()]} | {:error, {:failed_to_start_proxy, node(), reason :: term()}}
  def define_commit_proxies(
        n_proxies,
        cluster,
        epoch,
        director,
        available_nodes,
        supervisor_otp_name
      ) do
    child_spec =
      child_spec_for_commit_proxy(cluster, epoch, director)

    available_nodes
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

  #
  #
  #
  @spec define_resolvers(
          n_resolvers :: pos_integer(),
          Bedrock.version_vector(),
          logs :: [pid],
          Bedrock.epoch(),
          available_nodes :: [node()],
          supervisor_otp_name :: atom()
        ) ::
          {:ok, [pid()]}
          | {:error, {:failed_to_start_proxy, node(), reason :: term()}}
          | {:error, {:failed_to_playback_logs, %{Log.ref() => reason :: term()}}}
  def define_resolvers(
        n_resolvers,
        version_vector,
        logs,
        epoch,
        available_nodes,
        supervisor_otp_name
      ) do
    child_specs =
      1..n_resolvers
      |> Enum.map(fn _ -> child_spec_for_transaction_resolver(epoch) end)

    with {:ok, resolvers} <- start_resolvers(child_specs, available_nodes, supervisor_otp_name),
         :ok <- playback_logs_into_resolvers(resolvers, logs, version_vector) do
      {:ok, resolvers}
    else
      {:error, reason} -> {:error, reason}
    end
  end

  @spec start_resolvers(
          child_specs :: [Supervisor.child_spec()],
          available_nodes :: [node()],
          supervisor_otp_name :: atom()
        ) :: {:ok, [pid()]} | {:error, {:failed_to_start_proxy, node(), reason :: term()}}
  def start_resolvers(child_specs, available_nodes, supervisor_otp_name) do
    available_nodes
    |> Stream.cycle()
    |> Enum.zip(child_specs)
    |> Task.async_stream(
      fn {node, child_spec} ->
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

  @spec playback_logs_into_resolvers(
          resolvers :: [pid()],
          logs :: [pid()],
          version_vector :: Bedrock.version_vector()
        ) ::
          :ok | {:error, {:failed_to_playback_logs, %{Log.ref() => reason :: term()}}}
  def playback_logs_into_resolvers(resolvers, logs, {first_version, last_version}) do
    resolvers
    |> pair_resolvers_with_logs(logs)
    |> Task.async_stream(
      fn {resolver, source_log} ->
        {resolver,
         Resolver.recover_from(
           resolver,
           source_log,
           first_version,
           last_version
         )}
      end,
      ordered: false,
      zip_input_on_exit: true
    )
    |> Enum.reduce_while(%{}, fn
      {:ok, {_, {:error, :newer_epoch_exists} = error}}, _ ->
        {:halt, error}

      {:ok, {_log_id, :ok}}, failures ->
        {:cont, failures}

      {:ok, {log_id, {:error, reason}}}, failures ->
        {:cont, Map.put(failures, log_id, reason)}

      {:exit, {log_id, reason}}, failures ->
        {:cont, Map.put(failures, log_id, reason)}
    end)
    |> case do
      failures when failures == %{} -> :ok
      failures -> {:error, {:failed_to_playback_logs, failures}}
    end
  end

  def pair_resolvers_with_logs(resolvers, []),
    do: resolvers |> Stream.zip([nil] |> Stream.cycle())

  def pair_resolvers_with_logs(resolvers, logs),
    do: resolvers |> Stream.zip(logs |> Stream.cycle())

  @spec child_spec_for_transaction_resolver(epoch :: Bedrock.epoch()) :: Supervisor.child_spec()
  def child_spec_for_transaction_resolver(epoch) do
    Resolver.child_spec(epoch: epoch)
  end
end
