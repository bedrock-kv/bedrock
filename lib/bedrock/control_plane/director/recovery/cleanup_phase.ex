defmodule Bedrock.ControlPlane.Director.Recovery.CleanupPhase do
  @moduledoc """
  Cleans up workers that are no longer part of the running transaction system.

  Identifies log and storage workers that were available during recovery but
  not selected for the final transaction system layout. These obsolete workers
  are terminated to free resources and avoid confusion.

  Worker cleanup only removes workers that are definitively not needed. Workers
  that might be useful for future recovery attempts or cluster expansion are
  preserved.

  This cleanup step ensures the cluster runs with only the workers that are
  actually part of the current configuration, making monitoring and debugging
  easier.

  Always succeeds since worker cleanup failures do not affect cluster operation.
  Transitions to :completed to mark recovery as finished.
  """

  alias Bedrock.Service.Foreman
  alias Bedrock.Service.Worker

  use Bedrock.ControlPlane.Director.Recovery.RecoveryPhase

  import Bedrock.ControlPlane.Director.Recovery.Telemetry

  @impl true
  def execute(recovery_attempt, context) do
    obsolete_services = find_obsolete_services(recovery_attempt, context)
    untracked_workers = find_untracked_workers(recovery_attempt, context)
    all_obsolete_services = Map.merge(obsolete_services, untracked_workers)

    if has_obsolete_services?(all_obsolete_services) do
      execute_cleanup(all_obsolete_services, recovery_attempt, context)
    else
      complete_recovery(recovery_attempt)
    end
  end

  @spec find_obsolete_services(RecoveryAttempt.t(), RecoveryPhase.context()) :: %{
          Worker.id() => {atom(), {atom(), node()}}
        }
  defp find_obsolete_services(recovery_attempt, context),
    do: Map.drop(context.available_services, Map.keys(recovery_attempt.transaction_services))

  @spec find_untracked_workers(RecoveryAttempt.t(), RecoveryPhase.context()) :: %{
          Worker.id() => %{kind: atom(), last_seen: {atom(), node()}, status: {:up, pid()}}
        }
  defp find_untracked_workers(recovery_attempt, context) do
    required_worker_ids = MapSet.new(Map.keys(recovery_attempt.transaction_services))
    tracked_worker_ids = MapSet.new(Map.keys(context.available_services))

    available_nodes = get_nodes_with_capability(context, :storage)

    foreman_all_fn = Map.get(context, :foreman_all_fn, &Foreman.all/2)
    worker_info_fn = Map.get(context, :worker_info_fn, &Worker.info/3)

    available_nodes
    |> Enum.flat_map(fn node ->
      foreman_ref = {recovery_attempt.cluster.otp_name(:foreman), node}

      case foreman_all_fn.(foreman_ref, timeout: 5_000) do
        {:ok, worker_refs} ->
          worker_refs
          |> Enum.map(fn worker_ref ->
            case worker_info_fn.({worker_ref, node}, [:id, :otp_name, :kind, :pid],
                   timeout_in_ms: 5_000
                 ) do
              {:ok, worker_info} ->
                worker_id = worker_info[:id]

                # Only include workers that are not required and not already tracked
                if not MapSet.member?(required_worker_ids, worker_id) and
                     not MapSet.member?(tracked_worker_ids, worker_id) do
                  service_info = %{
                    kind: worker_info[:kind],
                    last_seen: {worker_info[:otp_name], node},
                    status: {:up, worker_info[:pid]}
                  }

                  {worker_id, service_info}
                else
                  nil
                end

              _ ->
                nil
            end
          end)
          |> Enum.reject(&is_nil/1)

        _ ->
          []
      end
    end)
    |> Map.new()
  end

  @spec get_nodes_with_capability(RecoveryPhase.context(), Bedrock.Cluster.capability()) :: [
          node()
        ]
  defp get_nodes_with_capability(context, capability) do
    Map.get(context.node_capabilities, capability, [])
  end

  @spec has_obsolete_services?(map()) :: boolean()
  defp has_obsolete_services?(obsolete_services), do: map_size(obsolete_services) > 0

  @spec execute_cleanup(map(), map(), map()) :: {map(), :completed} | {map(), {:stalled, term()}}
  defp execute_cleanup(obsolete_services, recovery_attempt, context) do
    case cleanup_obsolete_workers(obsolete_services, recovery_attempt.cluster, context) do
      {:ok, cleanup_stats} ->
        trace_cleanup_completion(cleanup_stats)
        complete_recovery(recovery_attempt)

      {:error, reason} ->
        stall_recovery_with_cleanup_failure(recovery_attempt, reason)
    end
  end

  @spec trace_cleanup_completion(map()) :: :ok
  defp trace_cleanup_completion(cleanup_stats) do
    trace_recovery_cleanup_completed(
      cleanup_stats.total_workers,
      cleanup_stats.successful_cleanups,
      cleanup_stats.failed_cleanups
    )
  end

  @spec complete_recovery(map()) :: {map(), :completed}
  defp complete_recovery(recovery_attempt), do: {recovery_attempt, :completed}

  @spec stall_recovery_with_cleanup_failure(map(), term()) :: {map(), {:stalled, term()}}
  defp stall_recovery_with_cleanup_failure(recovery_attempt, reason),
    do: {recovery_attempt, {:stalled, {:worker_cleanup_failed, reason}}}

  @spec cleanup_obsolete_workers(map(), module(), map()) :: {:ok, map()} | {:error, term()}
  defp cleanup_obsolete_workers(obsolete_services, cluster, context) do
    workers_by_node = group_workers_by_node(obsolete_services)

    trace_cleanup_started(obsolete_services, workers_by_node)
    cleanup_results = execute_node_cleanups(workers_by_node, cluster, context)
    calculate_cleanup_statistics(obsolete_services, cleanup_results)
  rescue
    error ->
      {:error, {:cleanup_exception, error}}
  end

  @spec group_workers_by_node(map()) :: map()
  defp group_workers_by_node(obsolete_services) do
    obsolete_services
    |> Enum.group_by(fn {_worker_id, %{last_seen: {_worker_name, node}}} ->
      node
    end)
  end

  @spec trace_cleanup_started(map(), map()) :: :ok
  defp trace_cleanup_started(obsolete_services, workers_by_node) do
    total_workers = map_size(obsolete_services)
    affected_nodes = Map.keys(workers_by_node)
    trace_recovery_cleanup_started(total_workers, affected_nodes)
  end

  @spec execute_node_cleanups(map(), module(), map()) :: map()
  defp execute_node_cleanups(workers_by_node, cluster, context),
    do: workers_by_node |> Enum.map(&cleanup_workers_on_node(&1, cluster, context)) |> Map.new()

  @spec cleanup_workers_on_node({node(), [{String.t(), map()}]}, module(), map()) ::
          {node(), map()}
  defp cleanup_workers_on_node({node, workers}, cluster, context) do
    worker_ids = extract_worker_ids(workers)
    foreman_ref = build_foreman_ref(cluster, node)

    remove_workers_fn = Map.get(context, :remove_workers_fn, &Foreman.remove_workers/3)

    trace_recovery_node_cleanup_started(node, length(worker_ids))

    results = remove_workers_fn.(foreman_ref, worker_ids, timeout: 30_000)
    trace_recovery_node_cleanup_completed(node, Map.to_list(results))
    {node, results}
  end

  @spec extract_worker_ids([{String.t(), map()}]) :: [String.t()]
  defp extract_worker_ids(workers), do: Enum.map(workers, fn {worker_id, _} -> worker_id end)

  @spec build_foreman_ref(module(), node()) :: {atom(), node()}
  defp build_foreman_ref(cluster, node), do: {cluster.otp_name(:foreman), node}

  @spec calculate_cleanup_statistics(map(), map()) :: {:ok, map()}
  defp calculate_cleanup_statistics(obsolete_services, cleanup_results) do
    all_results = extract_all_cleanup_results(cleanup_results)

    cleanup_stats = %{
      total_workers: map_size(obsolete_services),
      successful_cleanups: count_successful_cleanups(all_results),
      failed_cleanups: count_failed_cleanups(all_results),
      cleanup_results: cleanup_results
    }

    {:ok, cleanup_stats}
  end

  @spec extract_all_cleanup_results(map()) :: [:ok | {:error, term()}]
  defp extract_all_cleanup_results(cleanup_results),
    do: cleanup_results |> Enum.flat_map(fn {_node, results} -> Map.values(results) end)

  @spec count_successful_cleanups([:ok | {:error, term()}]) :: non_neg_integer()
  defp count_successful_cleanups(all_results), do: Enum.count(all_results, &(&1 == :ok))

  @spec count_failed_cleanups([:ok | {:error, term()}]) :: non_neg_integer()
  defp count_failed_cleanups(all_results), do: Enum.count(all_results, &match?({:error, _}, &1))
end
