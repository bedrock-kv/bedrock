defmodule Bedrock.ControlPlane.Director.Recovery.StorageRecruitmentPhase do
  @moduledoc """
  Handles the :recruit_storage_to_fill_vacancies phase of recovery.

  This phase is responsible for filling storage team vacancies with
  available storage workers.

  See: [Recovery Guide](docs/knowledge_base/01-guides/recovery-guide.md#recovery-process)
  """

  @behaviour Bedrock.ControlPlane.Director.Recovery.RecoveryPhase

  alias Bedrock.DataPlane.Storage
  alias Bedrock.ControlPlane.Config.StorageTeamDescriptor
  alias Bedrock.ControlPlane.Director.Recovery.RecoveryPhase
  alias Bedrock.Service.Foreman
  alias Bedrock.Service.Worker

  import Bedrock.ControlPlane.Director.Recovery.Telemetry

  @doc """
  Execute the storage recruitment phase of recovery.

  Fills storage team vacancies with available storage workers.
  """

  @impl true
  def execute(%{state: :recruit_storage_to_fill_vacancies} = recovery_attempt, context) do
    assigned_storage_ids =
      recovery_attempt.storage_teams
      |> Enum.reduce(MapSet.new(), &Enum.into(&1.storage_ids, &2))

    all_storage_ids =
      recovery_attempt.storage_recovery_info_by_id |> Map.keys() |> MapSet.new()

    # Get nodes with storage capability from node tracking
    alias Bedrock.ControlPlane.Director.NodeTracking
    available_storage_nodes = NodeTracking.nodes_with_capability(context.node_tracking, :storage)

    fill_storage_team_vacancies(
      recovery_attempt.storage_teams,
      assigned_storage_ids,
      all_storage_ids,
      available_storage_nodes
    )
    |> case do
      {:error, reason} ->
        recovery_attempt |> Map.put(:state, {:stalled, reason})

      {:ok, storage_teams, new_worker_ids} ->
        # Create the new workers if any are needed
        case create_new_storage_workers(
               new_worker_ids,
               available_storage_nodes,
               recovery_attempt,
               context
             ) do
          {:ok, updated_services} ->
            trace_recovery_all_storage_team_vacancies_filled()

            recovery_attempt
            |> Map.put(:storage_teams, storage_teams)
            |> Map.update!(:storage_recovery_info_by_id, &Map.merge(&1, updated_services))
            |> Map.put(:state, :replay_old_logs)

          {:error, reason} ->
            recovery_attempt |> Map.put(:state, {:stalled, reason})
        end
    end
  end

  @doc """
  Fills vacancies in storage teams by assigning IDs of storage workers that are
  not currently part of the transaction system. If there are not enough storage
  workers to fill all vacancies, an error is returned.
  """
  @spec fill_storage_team_vacancies(
          storage_teams :: [StorageTeamDescriptor.t()],
          assigned_storage_ids :: MapSet.t(Storage.id()),
          all_storage_ids :: MapSet.t(Storage.id()),
          available_nodes :: [node()]
        ) ::
          {:ok, [StorageTeamDescriptor.t()], [Storage.id()]}
          | {:error, term()}
  def fill_storage_team_vacancies(storage_teams, assigned_storage_ids, all_storage_ids, available_nodes) do
    vacancies = assigned_storage_ids |> MapSet.filter(&vacancy?/1)
    n_vacancies = MapSet.size(vacancies)

    candidate_ids = all_storage_ids |> MapSet.difference(assigned_storage_ids)
    n_candidates = MapSet.size(candidate_ids)

    if n_vacancies <= n_candidates do
      # We have enough existing workers
      {:ok,
       replace_vacancies_with_storage_ids(
         storage_teams,
         vacancies |> Enum.zip(candidate_ids) |> Map.new()
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
          |> Enum.map(fn _ -> Worker.random_id() end)

        # Use existing candidates plus new worker IDs
        all_worker_ids = Enum.concat(candidate_ids, new_worker_ids)

        {:ok,
         replace_vacancies_with_storage_ids(
           storage_teams,
           vacancies |> Enum.zip(all_worker_ids) |> Map.new()
         ), new_worker_ids}
      end
    end
  end

  @spec vacancy?(Storage.id() | StorageTeamDescriptor.vacancy()) :: boolean()
  def vacancy?({:vacancy, _}), do: true
  def vacancy?(_), do: false

  @spec replace_vacancies_with_storage_ids(
          storage_teams :: [StorageTeamDescriptor.t()],
          storage_id_for_vacancy :: %{StorageTeamDescriptor.vacancy() => Storage.id()}
        ) :: [StorageTeamDescriptor.t()]
  def replace_vacancies_with_storage_ids(storage_teams, storage_id_for_vacancy) do
    storage_teams
    |> Enum.map(fn descriptor ->
      descriptor
      |> Map.update!(:storage_ids, fn storage_ids ->
        storage_ids
        |> Enum.map(&Map.get(storage_id_for_vacancy, &1, &1))
      end)
    end)
  end

  @spec create_new_storage_workers([String.t()], [node()], map(), RecoveryPhase.context()) ::
          {:ok, %{String.t() => map()}} | {:error, term()}
  defp create_new_storage_workers([], _available_nodes, _recovery_attempt, _context), do: {:ok, %{}}

  defp create_new_storage_workers(new_worker_ids, available_nodes, recovery_attempt, _context) do
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

    with {:ok, worker_ref} <- Foreman.new_worker(foreman_ref, worker_id, :storage, timeout: 10_000),
         {:ok, worker_info} <- Worker.info({worker_ref, node}, [:id, :otp_name, :kind, :pid]) do
      {worker_id,
       %{
         kind: :storage,
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
      "Some storage workers failed to be tracked during creation: #{inspect(failed_workers)}"
    )
  end
end
