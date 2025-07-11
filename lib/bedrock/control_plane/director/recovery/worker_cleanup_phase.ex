defmodule Bedrock.ControlPlane.Director.Recovery.WorkerCleanupPhase do
  @moduledoc """
  Handles the :cleanup_obsolete_workers phase of recovery.

  This phase is responsible for cleaning up workers that are no longer
  part of the running transaction system after a successful recovery.
  """

  alias Bedrock.Service.Foreman
  alias Bedrock.ControlPlane.Director.Recovery.RecoveryPhase
  @behaviour RecoveryPhase

  import Bedrock.ControlPlane.Director.Recovery.Telemetry

  @impl true
  def execute(%{state: :cleanup_obsolete_workers} = recovery_attempt, context) do
    obsolete_services = find_obsolete_services(recovery_attempt)
    untracked_workers = find_untracked_workers(recovery_attempt, context)
    all_obsolete_services = Map.merge(obsolete_services, untracked_workers)

    if has_obsolete_services?(all_obsolete_services) do
      execute_cleanup(all_obsolete_services, recovery_attempt)
    else
      complete_recovery(recovery_attempt)
    end
  end

  defp find_obsolete_services(recovery_attempt),
    do:
      Map.drop(recovery_attempt.available_services, Map.keys(recovery_attempt.required_services))

  defp find_untracked_workers(recovery_attempt, context) do
    required_worker_ids = MapSet.new(Map.keys(recovery_attempt.required_services))
    tracked_worker_ids = MapSet.new(Map.keys(recovery_attempt.available_services))

    # Get all nodes with foreman capability
    available_nodes = get_nodes_with_capability(context, :foreman)

    # Query each foreman for all workers and find untracked ones
    available_nodes
    |> Enum.flat_map(fn node ->
      foreman_ref = {recovery_attempt.cluster.otp_name(:foreman), node}

      case Foreman.all(foreman_ref, timeout: 5_000) do
        {:ok, worker_refs} ->
          worker_refs
          |> Enum.map(fn worker_ref ->
            case Bedrock.Service.Worker.info({worker_ref, node}, [:id, :otp_name, :kind, :pid],
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

  defp get_nodes_with_capability(context, capability) do
    alias Bedrock.ControlPlane.Director.NodeTracking
    NodeTracking.nodes_with_capability(context.node_tracking, capability)
  end

  defp has_obsolete_services?(obsolete_services), do: map_size(obsolete_services) > 0

  defp execute_cleanup(obsolete_services, recovery_attempt) do
    case cleanup_obsolete_workers(obsolete_services, recovery_attempt.cluster) do
      {:ok, cleanup_stats} ->
        trace_cleanup_completion(cleanup_stats)
        complete_recovery(recovery_attempt)

      {:error, reason} ->
        stall_recovery_with_cleanup_failure(recovery_attempt, reason)
    end
  end

  defp trace_cleanup_completion(cleanup_stats) do
    trace_recovery_cleanup_completed(
      cleanup_stats.total_workers,
      cleanup_stats.successful_cleanups,
      cleanup_stats.failed_cleanups
    )
  end

  defp complete_recovery(recovery_attempt), do: Map.put(recovery_attempt, :state, :completed)

  defp stall_recovery_with_cleanup_failure(recovery_attempt, reason),
    do: Map.put(recovery_attempt, :state, {:stalled, {:worker_cleanup_failed, reason}})

  @spec cleanup_obsolete_workers(map(), module()) :: {:ok, map()} | {:error, term()}
  defp cleanup_obsolete_workers(obsolete_services, cluster) do
    workers_by_node = group_workers_by_node(obsolete_services)

    trace_cleanup_started(obsolete_services, workers_by_node)
    cleanup_results = execute_node_cleanups(workers_by_node, cluster)
    calculate_cleanup_statistics(obsolete_services, cleanup_results)
  rescue
    error ->
      {:error, {:cleanup_exception, error}}
  end

  defp group_workers_by_node(obsolete_services) do
    obsolete_services
    |> Enum.group_by(fn {_worker_id, %{last_seen: {_worker_name, node}}} ->
      node
    end)
  end

  defp trace_cleanup_started(obsolete_services, workers_by_node) do
    total_workers = map_size(obsolete_services)
    affected_nodes = Map.keys(workers_by_node)
    trace_recovery_cleanup_started(total_workers, affected_nodes)
  end

  defp execute_node_cleanups(workers_by_node, cluster),
    do: workers_by_node |> Enum.map(&cleanup_workers_on_node(&1, cluster)) |> Map.new()

  defp cleanup_workers_on_node({node, workers}, cluster) do
    worker_ids = extract_worker_ids(workers)
    foreman_ref = build_foreman_ref(cluster, node)

    trace_recovery_node_cleanup_started(node, length(worker_ids))

    results = Foreman.remove_workers(foreman_ref, worker_ids, timeout: 30_000)
    trace_recovery_node_cleanup_completed(node, results)
    {node, results}
  end

  defp extract_worker_ids(workers), do: Enum.map(workers, fn {worker_id, _} -> worker_id end)

  defp build_foreman_ref(cluster, node), do: {cluster.otp_name(:foreman), node}

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

  defp extract_all_cleanup_results(cleanup_results),
    do: cleanup_results |> Enum.flat_map(fn {_node, results} -> Map.values(results) end)

  defp count_successful_cleanups(all_results), do: Enum.count(all_results, &(&1 == :ok))

  defp count_failed_cleanups(all_results), do: Enum.count(all_results, &match?({:error, _}, &1))
end
