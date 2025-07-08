defmodule Bedrock.ControlPlane.Director.Recovery.LogRecruitmentPhase do
  @moduledoc """
  Handles the :recruit_logs_to_fill_vacancies phase of recovery.

  This phase is responsible for filling log vacancies by assigning existing
  log workers or creating new ones as needed.
  """

  alias Bedrock.DataPlane.Log

  import Bedrock.ControlPlane.Director.Recovery.Telemetry

  @doc """
  Execute the log recruitment phase of recovery.

  Fills log vacancies with available workers or creates new workers
  on available nodes.
  """
  @spec execute(map()) :: map()
  def execute(%{state: :recruit_logs_to_fill_vacancies} = recovery_attempt) do
    assigned_log_ids =
      recovery_attempt.last_transaction_system_layout.logs |> Map.keys() |> MapSet.new()

    all_log_ids =
      recovery_attempt.available_services
      |> Enum.filter(fn {_id, %{kind: kind}} -> kind == :log end)
      |> Enum.map(&elem(&1, 0))
      |> MapSet.new()

    # Get nodes with log capability from node tracking
    available_log_nodes = get_nodes_with_capability(recovery_attempt, :log)

    fill_log_vacancies(recovery_attempt.logs, assigned_log_ids, all_log_ids, available_log_nodes)
    |> case do
      {:error, reason} ->
        recovery_attempt |> Map.put(:state, {:stalled, reason})

      {:ok, logs, new_worker_ids} ->
        # Create the new workers if any are needed
        case create_new_log_workers(new_worker_ids, available_log_nodes, recovery_attempt) do
          {:ok, updated_services} ->
            trace_recovery_all_log_vacancies_filled()

            recovery_attempt
            |> Map.put(:logs, logs)
            |> Map.update!(:available_services, &Map.merge(&1, updated_services))
            |> Map.put(:state, :recruit_storage_to_fill_vacancies)

          {:error, reason} ->
            recovery_attempt |> Map.put(:state, {:stalled, reason})
        end
    end
  end

  @spec fill_log_vacancies(
          logs :: %{Log.id() => any()},
          assigned_log_ids :: MapSet.t(Log.id()),
          all_log_ids :: MapSet.t(Log.id()),
          available_nodes :: [node()]
        ) ::
          {:ok, %{Log.id() => any()}, [Log.id()]}
          | {:error, term()}
  def fill_log_vacancies(logs, assigned_log_ids, all_log_ids, available_nodes) do
    vacancies = all_vacancies(logs)
    n_vacancies = MapSet.size(vacancies)

    candidates_ids = all_log_ids |> MapSet.difference(assigned_log_ids)
    n_candidates = MapSet.size(candidates_ids)

    if n_vacancies <= n_candidates do
      # We have enough existing workers
      {:ok,
       replace_vacancies_with_log_ids(
         logs,
         Enum.zip(vacancies, candidates_ids) |> Map.new()
       ), []}
    else
      # We need to create new workers
      needed_workers = n_vacancies - n_candidates

      if length(available_nodes) < needed_workers do
        {:error, {:insufficient_nodes, needed_workers, length(available_nodes)}}
      else
        # Create new worker IDs
        new_worker_ids =
          1..needed_workers
          |> Enum.map(&"log_#{System.unique_integer([:positive])}_#{&1}")

        # Use existing candidates plus new worker IDs
        all_worker_ids = Enum.concat(candidates_ids, new_worker_ids)

        {:ok,
         replace_vacancies_with_log_ids(
           logs,
           Enum.zip(vacancies, all_worker_ids) |> Map.new()
         ), new_worker_ids}
      end
    end
  end

  @spec all_vacancies(%{Log.id() => any()}) :: MapSet.t()
  def all_vacancies(logs) do
    Enum.reduce(logs, [], fn
      {{:vacancy, _} = vacancy, _}, list -> [vacancy | list]
      _, list -> list
    end)
    |> MapSet.new()
  end

  @spec replace_vacancies_with_log_ids(
          logs :: %{Log.id() => any()},
          log_id_for_vacancy :: %{any() => Log.id()}
        ) :: %{Log.id() => any()}
  def replace_vacancies_with_log_ids(logs, log_id_for_vacancy) do
    logs
    |> Enum.map(fn {log_id, descriptor} ->
      case Map.get(log_id_for_vacancy, log_id) do
        nil -> {log_id, descriptor}
        candidate_id -> {candidate_id, descriptor}
      end
    end)
    |> Map.new()
  end

  @spec get_nodes_with_capability(map(), atom()) :: [node()]
  defp get_nodes_with_capability(_recovery_attempt, _capability) do
    # This function needs access to the Director state's node_tracking
    # For now, we'll use Node.list() as a fallback
    # In a real implementation, this should query the node tracking table
    Node.list() ++ [Node.self()]
  end

  @spec create_new_log_workers([String.t()], [node()], map()) ::
          {:ok, %{String.t() => map()}} | {:error, term()}
  defp create_new_log_workers([], _available_nodes, _recovery_attempt), do: {:ok, %{}}

  defp create_new_log_workers(new_worker_ids, available_nodes, recovery_attempt) do
    # Distribute workers across available nodes
    node_assignments =
      new_worker_ids
      |> Enum.with_index()
      |> Enum.map(fn {worker_id, index} ->
        node = Enum.at(available_nodes, rem(index, length(available_nodes)))
        {worker_id, node}
      end)

    # Create workers on their assigned nodes by calling Foreman directly
    results =
      Enum.map(node_assignments, fn {worker_id, node} ->
        # Contact the foreman on the target node directly
        foreman_ref = {recovery_attempt.cluster.otp_name(:foreman), node}

        case Bedrock.Service.Foreman.new_worker(foreman_ref, worker_id, :log, timeout: 10_000) do
          {:ok, worker_ref} ->
            case Bedrock.Service.Worker.info({worker_ref, node}, [:id, :otp_name, :kind, :pid]) do
              {:ok, worker_info} ->
                service_info = %{
                  kind: :log,
                  last_seen: {worker_info[:otp_name], node},
                  status: {:up, worker_info[:pid]}
                }

                {worker_id, service_info}

              {:error, reason} ->
                {:error, {worker_id, node, {:worker_info_failed, reason}}}
            end

          {:error, reason} ->
            {:error, {worker_id, node, {:worker_creation_failed, reason}}}
        end
      end)

    # Check if all workers were created successfully
    case Enum.find(results, &match?({:error, _}, &1)) do
      nil ->
        # All successful
        {:ok, Map.new(results)}

      {:error, {worker_id, node, reason}} ->
        {:error, {:worker_creation_failed, worker_id, node, reason}}
    end
  end
end
