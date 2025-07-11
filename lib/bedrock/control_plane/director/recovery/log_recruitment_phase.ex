defmodule Bedrock.ControlPlane.Director.Recovery.LogRecruitmentPhase do
  @moduledoc """
  Handles the :recruit_logs_to_fill_vacancies phase of recovery.

  This phase is responsible for filling log vacancies by assigning existing
  log workers or creating new ones as needed.
  """

  @behaviour Bedrock.ControlPlane.Director.Recovery.RecoveryPhase

  alias Bedrock.ControlPlane.Director.Recovery.RecoveryPhase
  alias Bedrock.DataPlane.Log
  alias Bedrock.Service.Foreman
  alias Bedrock.Service.Worker

  import Bedrock.ControlPlane.Director.Recovery.Telemetry

  @doc """
  Execute the log recruitment phase of recovery.

  Fills log vacancies with available workers or creates new workers
  on available nodes.
  """

  @impl true
  def execute(%{state: :recruit_logs_to_fill_vacancies} = recovery_attempt, context) do
    assigned_log_ids =
      recovery_attempt.last_transaction_system_layout.logs |> Map.keys() |> MapSet.new()

    all_log_ids =
      recovery_attempt.available_services
      |> Enum.filter(fn {_id, %{kind: kind}} -> kind == :log end)
      |> Enum.map(&elem(&1, 0))
      |> MapSet.new()

    # Get nodes with log capability from node tracking
    alias Bedrock.ControlPlane.Director.NodeTracking
    available_log_nodes = NodeTracking.nodes_with_capability(context.node_tracking, :log)

    fill_log_vacancies(recovery_attempt.logs, assigned_log_ids, all_log_ids, available_log_nodes)
    |> case do
      {:error, reason} ->
        recovery_attempt |> Map.put(:state, {:stalled, reason})

      {:ok, logs, new_worker_ids} ->
        # Create the new workers if any are needed
        case create_new_log_workers(
               new_worker_ids,
               available_log_nodes,
               recovery_attempt,
               context
             ) do
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

  @spec create_new_log_workers([String.t()], [node()], map(), RecoveryPhase.context()) ::
          {:ok, %{String.t() => map()}} | {:error, term()}
  defp create_new_log_workers([], _available_nodes, _recovery_attempt, _context), do: {:ok, %{}}

  defp create_new_log_workers(new_worker_ids, available_nodes, recovery_attempt, _context) do
    new_worker_ids
    |> assign_workers_to_nodes(available_nodes)
    |> Enum.map(&create_worker_on_node(&1, recovery_attempt))
    |> separate_successes_from_failures()
    |> handle_worker_creation_outcome()
  end

  @spec assign_workers_to_nodes([String.t()], [node()]) :: [{String.t(), node()}]
  defp assign_workers_to_nodes(worker_ids, available_nodes) do
    worker_ids
    |> Enum.with_index()
    |> Enum.map(fn {worker_id, index} ->
      node = Enum.at(available_nodes, rem(index, length(available_nodes)))
      {worker_id, node}
    end)
  end

  @spec create_worker_on_node({String.t(), node()}, map()) ::
          {String.t(), map()} | {:error, {String.t(), node(), term()}}
  defp create_worker_on_node({worker_id, node}, recovery_attempt) do
    foreman_ref = {recovery_attempt.cluster.otp_name(:foreman), node}

    with {:ok, worker_ref} <- Foreman.new_worker(foreman_ref, worker_id, :log, timeout: 10_000),
         {:ok, worker_info} <- Worker.info({worker_ref, node}, [:id, :otp_name, :kind, :pid]) do
      {worker_id,
       %{
         kind: :log,
         last_seen: {worker_info[:otp_name], node},
         status: {:up, worker_info[:pid]}
       }}
    else
      {:error, reason} -> {:error, {worker_id, node, {:worker_creation_failed, reason}}}
    end
  end

  @spec separate_successes_from_failures([{String.t(), map()} | {:error, term()}]) ::
          {[{String.t(), map()}], [term()]}
  defp separate_successes_from_failures(results) do
    Enum.reduce(results, {[], []}, fn
      {:error, error}, {successes, failures} -> {successes, [error | failures]}
      success, {successes, failures} -> {[success | successes], failures}
    end)
  end

  @spec handle_worker_creation_outcome({[{String.t(), map()}], [term()]}) ::
          {:ok, %{String.t() => map()}} | {:error, term()}
  defp handle_worker_creation_outcome({successful_workers, failed_workers}) do
    cond do
      successful_workers != [] and failed_workers != [] ->
        log_partial_failures(failed_workers)
        {:ok, Map.new(successful_workers)}

      successful_workers != [] ->
        {:ok, Map.new(successful_workers)}

      true ->
        {:error, {:all_workers_failed, failed_workers}}
    end
  end

  @spec log_partial_failures([term()]) :: :ok
  defp log_partial_failures(failed_workers) do
    require Logger

    Logger.warning(
      "Some workers failed to be tracked during creation: #{inspect(failed_workers)}"
    )
  end
end
