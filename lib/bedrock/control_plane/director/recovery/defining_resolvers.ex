defmodule Bedrock.ControlPlane.Director.Recovery.DefiningResolvers do
  alias Bedrock.DataPlane.Resolver
  alias Bedrock.DataPlane.Log
  alias Bedrock.ControlPlane.Config.LogDescriptor
  alias Bedrock.ControlPlane.Config.ResolverDescriptor
  alias Bedrock.ControlPlane.Config.StorageTeamDescriptor

  @spec define_resolvers(
          resolvers :: [ResolverDescriptor.t()],
          storage_teams :: [StorageTeamDescriptor.t()],
          logs :: %{Log.id() => LogDescriptor.t()},
          running_logs :: %{Log.id() => pid()},
          epoch :: Bedrock.epoch(),
          available_nodes :: [node()],
          version_vector :: Bedrock.version_vector(),
          start_supervised :: (Supervisor.child_spec(), node() -> {:ok, pid()} | {:error, term()})
        ) ::
          {:ok, [{start_key :: Bedrock.version(), resolver :: pid()}]}
          | {:error, {:failed_to_start, :resolver, node(), reason :: term()}}
  def define_resolvers(
        resolvers,
        storage_teams,
        logs,
        running_logs,
        epoch,
        available_nodes,
        version_vector,
        start_supervised
      ) do
    resolver_boot_info =
      resolvers
      |> generate_resolver_ranges()
      |> prepare_resolver_range_tags(storage_teams)
      |> assign_logs_to_resolvers(logs, running_logs)
      |> Enum.map(fn {{start_key, _end_key} = key_range, logs_to_copy} ->
        {child_spec_for_resolver(epoch, key_range), start_key, logs_to_copy}
      end)

    start_resolvers(resolver_boot_info, available_nodes, version_vector, start_supervised)
  end

  defp generate_resolver_ranges(resolvers) do
    resolvers
    |> Enum.map(& &1.start_key)
    |> Enum.sort()
    |> Enum.concat([:end])
    |> Enum.chunk_every(2, 1, :discard)
  end

  defp prepare_resolver_range_tags(resolver_ranges, storage_teams) do
    storage_team_info =
      storage_teams
      |> Enum.map(&tuple_from_storage_team/1)

    resolver_ranges
    |> Enum.map(fn [min, max_ex] ->
      {{min, max_ex}, storage_team_tags_covering_range(storage_team_info, min, max_ex)}
    end)
  end

  def tuple_from_storage_team(storage_team),
    do: {storage_team.key_range, storage_team.tag, storage_team.storage_ids}

  def storage_team_tags_covering_range(storage_teams, min_key, max_key_exclusive) do
    :ets.match_spec_run(
      storage_teams,
      :ets.match_spec_compile([
        {
          {{:"$1", :"$2"}, :"$3", :_},
          [
            {:or, {:<, min_key, :"$2"}, {:==, :end, :"$2"}},
            {:and, {:or, {:<, :"$1", max_key_exclusive}, {:==, :end, max_key_exclusive}}}
          ],
          [:"$3"]
        }
      ])
    )
  end

  @spec assign_logs_to_resolvers(
          resolver_range_tags :: [{Bedrock.key_range(), [Bedrock.range_tag()]}],
          tags_by_log_id :: %{Log.id() => LogDescriptor.t()},
          running_logs :: %{Log.id() => pid()}
        ) ::
          [{Bedrock.key_range(), %{Log.id() => pid()}}]
  defp assign_logs_to_resolvers(resolver_range_tags, tags_by_log_id, running_logs) do
    resolver_range_tags
    |> Enum.map(fn {key_range, tags} ->
      minimal_logs =
        Enum.reduce(running_logs, [], fn {log_id, pid}, acc ->
          if log_id not in acc and Enum.any?(tags, &(&1 in tags_by_log_id[log_id])) do
            [{log_id, pid} | acc]
          else
            acc
          end
        end)

      {key_range, minimal_logs |> Map.new()}
    end)
  end

  @spec start_resolvers(
          resolver_boot_info :: [
            {Supervisor.child_spec(), start_key :: Bedrock.version(), %{Log.id() => pid()}}
          ],
          available_nodes :: [node()],
          Bedrock.version_vector(),
          start_supervised :: (Supervisor.child_spec(), node() -> {:ok, pid()} | {:error, term()})
        ) ::
          {:ok, [{start_key :: Bedrock.version(), resolver :: pid()}]}
          | {:error, {:failed_to_start, :resolver, node(), reason :: term()}}
  def start_resolvers(
        resolver_boot_info,
        available_nodes,
        {first_version, last_version},
        start_supervised
      ) do
    available_nodes
    |> Stream.cycle()
    |> Enum.zip(resolver_boot_info)
    |> Task.async_stream(
      fn {node, {child_spec, start_key, logs_to_copy}} ->
        with {:ok, resolver} <- start_supervised.(child_spec, node),
             :ok <-
               Resolver.recover_from(
                 resolver,
                 logs_to_copy,
                 first_version,
                 last_version
               ) do
          {node, {start_key, resolver}}
        else
          {:error, reason} -> {node, {:error, reason}}
        end
      end,
      ordered: false
    )
    |> Enum.reduce_while([], fn
      {:ok, {_node, {start_key, pid}}}, resolvers when is_pid(pid) ->
        {:cont, [{start_key, pid} | resolvers]}

      {:ok, {node, {:error, reason}}}, _ ->
        {:halt, {:error, {:failed_to_start, :resolver, node, reason}}}

      {:exit, {node, reason}}, _ ->
        {:halt, {:error, {:failed_to_stary, :resolver, node, reason}}}
    end)
    |> case do
      {:error, reason} -> {:error, reason}
      resolvers -> {:ok, resolvers |> Enum.sort_by(&elem(&1, 0))}
    end
  end

  @spec child_spec_for_resolver(epoch :: Bedrock.epoch(), Bedrock.key_range()) ::
          Supervisor.child_spec()
  def child_spec_for_resolver(epoch, key_range) do
    Resolver.child_spec(epoch: epoch, key_range: key_range)
  end
end
