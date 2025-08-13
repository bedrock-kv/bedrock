defmodule Bedrock.ControlPlane.Director.Recovery.StorageRecruitmentPhase do
  @moduledoc """
  Transforms storage team vacancy placeholders into concrete service assignments using
  ultra-conservative data preservation approach.

  Solves the challenge of filling storage positions without accidentally destroying valuable
  persistent data. Unlike logs which can be replayed, storage services contain committed data
  that's expensive to recreate and potentially irreplaceable if lost.

  **Assignment Strategy**: Count all storage vacancies across teams, identify candidate services
  (available storage not part of old system or current assignments), assign directly if sufficient,
  otherwise create needed workers with round-robin distribution across nodes for fault tolerance.

  **Data Preservation Hierarchy**: Preserve old system services (never touch during recruitment),
  reuse available unaffiliated services, create new workers only as last resort.

  Stalls if insufficient nodes exist for worker creation or if recruited services fail
  to lock. However, immediately halts with error if any service is locked by a newer
  epoch (this director has been superseded). Transitions to log replay with complete
  storage service assignments.

  See the Storage Recruitment section in `docs/knowlege_base/02-deep/recovery-narrative.md`
  for detailed explanation of the conservative assignment strategy and data preservation approach.
  """

  use Bedrock.ControlPlane.Director.Recovery.RecoveryPhase

  alias Bedrock.ControlPlane.Config.StorageTeamDescriptor
  alias Bedrock.ControlPlane.Director.Recovery.RecoveryPhase
  alias Bedrock.DataPlane.Storage
  alias Bedrock.Service.Foreman
  alias Bedrock.Service.Worker

  import Bedrock.ControlPlane.Director.Recovery.Telemetry

  @impl true
  def execute(recovery_attempt, context) do
    assigned_storage_ids =
      recovery_attempt.storage_teams
      |> Enum.reduce(MapSet.new(), &Enum.into(&1.storage_ids, &2))

    old_system_storage_ids = get_old_storage_ids(context)

    available_storage_ids =
      context.available_services
      |> Enum.filter(fn {_id, {kind, _}} -> kind == :storage end)
      |> Enum.map(&elem(&1, 0))
      |> MapSet.new()
      |> MapSet.difference(old_system_storage_ids)
      |> MapSet.difference(assigned_storage_ids |> MapSet.filter(&(not vacancy?(&1))))

    available_storage_nodes = Map.get(context.node_capabilities, :storage, [])

    case fill_storage_team_vacancies(
           recovery_attempt.storage_teams,
           assigned_storage_ids,
           available_storage_ids,
           available_storage_nodes
         ) do
      {:error, reason} ->
        {recovery_attempt, {:stalled, reason}}

      {:ok, storage_teams, new_worker_ids} ->
        handle_successful_vacancy_filling(
          storage_teams,
          new_worker_ids,
          available_storage_nodes,
          recovery_attempt,
          context
        )
    end
  end

  defp get_old_storage_ids(%{old_transaction_system_layout: %{storage_teams: storage_teams}}) do
    storage_teams
    |> Enum.flat_map(& &1.storage_ids)
    |> MapSet.new()
  end

  defp get_old_storage_ids(_), do: MapSet.new()

  @spec handle_successful_vacancy_filling([map()], [String.t()], [node()], map(), map()) ::
          {map(), module()} | {map(), {:stalled, term()}} | {map(), {:error, :newer_epoch_exists}}
  defp handle_successful_vacancy_filling(
         storage_teams,
         new_worker_ids,
         available_storage_nodes,
         recovery_attempt,
         context
       ) do
    case create_new_storage_workers(
           new_worker_ids,
           available_storage_nodes,
           recovery_attempt,
           context
         ) do
      {:ok, updated_services} ->
        handle_successful_worker_creation(
          storage_teams,
          updated_services,
          recovery_attempt,
          context
        )

      {:error, reason} ->
        {recovery_attempt, {:stalled, reason}}
    end
  end

  @spec handle_successful_worker_creation([map()], map(), map(), map()) ::
          {map(), module()} | {map(), {:stalled, term()}} | {map(), {:error, :newer_epoch_exists}}
  defp handle_successful_worker_creation(
         storage_teams,
         updated_services,
         recovery_attempt,
         context
       ) do
    trace_recovery_all_storage_team_vacancies_filled()

    case extract_and_lock_existing_storage_services(
           storage_teams,
           context.available_services,
           recovery_attempt,
           context
         ) do
      {:ok, locked_existing_services} ->
        finalize_storage_recruitment(
          storage_teams,
          locked_existing_services,
          updated_services,
          recovery_attempt
        )

      {:error, :newer_epoch_exists} = error ->
        {recovery_attempt, error}

      {:error, reason} ->
        {recovery_attempt, {:stalled, reason}}
    end
  end

  @spec finalize_storage_recruitment([map()], map(), map(), map()) :: {map(), module()}
  defp finalize_storage_recruitment(
         storage_teams,
         locked_existing_services,
         updated_services,
         recovery_attempt
       ) do
    all_storage_services = Map.merge(locked_existing_services, updated_services)
    all_storage_pids = extract_service_pids(all_storage_services)

    updated_recovery_attempt =
      recovery_attempt
      |> Map.put(:storage_teams, storage_teams)
      |> Map.update!(:storage_recovery_info_by_id, &Map.merge(&1, updated_services))
      |> Map.update(:transaction_services, %{}, &Map.merge(&1, all_storage_services))
      |> Map.update(:service_pids, %{}, &Map.merge(&1, all_storage_pids))

    {updated_recovery_attempt, Bedrock.ControlPlane.Director.Recovery.LogReplayPhase}
  end

  @doc """
  Fills vacancies in storage teams by assigning IDs of available storage workers.
  If there are not enough available storage workers to fill all vacancies,
  new workers will be created.
  """
  @spec fill_storage_team_vacancies(
          storage_teams :: [StorageTeamDescriptor.t()],
          assigned_storage_ids :: MapSet.t(Storage.id()),
          available_storage_ids :: MapSet.t(Storage.id()),
          available_nodes :: [node()]
        ) ::
          {:ok, [StorageTeamDescriptor.t()], [Storage.id()]}
          | {:error, term()}
  def fill_storage_team_vacancies(
        storage_teams,
        assigned_storage_ids,
        available_storage_ids,
        available_nodes
      ) do
    vacancies = assigned_storage_ids |> MapSet.filter(&vacancy?/1)
    n_vacancies = MapSet.size(vacancies)

    candidate_ids = available_storage_ids
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
  defp create_new_storage_workers([], _available_nodes, _recovery_attempt, _context),
    do: {:ok, %{}}

  defp create_new_storage_workers(new_worker_ids, available_nodes, recovery_attempt, context) do
    new_worker_ids
    |> assign_workers_to_nodes(available_nodes)
    |> Enum.map(&create_worker_on_node(&1, recovery_attempt, context))
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

  @spec create_worker_on_node({String.t(), node()}, map(), map()) ::
          {String.t(), map()} | {:error, {String.t(), node(), term()}}
  defp create_worker_on_node({worker_id, node}, recovery_attempt, context) do
    foreman_ref = {recovery_attempt.cluster.otp_name(:foreman), node}

    create_worker_fn = Map.get(context, :create_worker_fn, &Foreman.new_worker/4)
    worker_info_fn = Map.get(context, :worker_info_fn, &Worker.info/3)

    with {:ok, worker_ref} <-
           create_worker_fn.(foreman_ref, worker_id, :storage, timeout: 10_000),
         {:ok, worker_info} <-
           worker_info_fn.({worker_ref, node}, [:id, :otp_name, :kind, :pid], []) do
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

  @spec extract_service_pids(%{String.t() => map()}) :: %{String.t() => pid()}
  defp extract_service_pids(services) do
    services
    |> Enum.filter(fn {_id, service} ->
      case service do
        %{status: {:up, _}} -> true
        _ -> false
      end
    end)
    |> Enum.map(fn {id, %{status: {:up, pid}}} -> {id, pid} end)
    |> Map.new()
  end

  @spec extract_and_lock_existing_storage_services(
          [StorageTeamDescriptor.t()],
          %{String.t() => map()},
          map(),
          map()
        ) ::
          {:ok, %{String.t() => map()}} | {:error, term()}
  defp extract_and_lock_existing_storage_services(
         storage_teams,
         available_services,
         recovery_attempt,
         context
       ) do
    existing_storage_ids =
      storage_teams
      |> Enum.flat_map(fn %{storage_ids: storage_ids} -> storage_ids end)
      |> Enum.reject(&match?({:vacancy, _}, &1))

    # Lock each existing storage service that was recruited
    existing_storage_ids
    |> Enum.reduce_while({:ok, %{}}, fn storage_id, {:ok, locked_services} ->
      process_storage_service_locking(
        storage_id,
        available_services,
        recovery_attempt,
        context,
        locked_services
      )
    end)
  end

  @spec process_storage_service_locking(String.t(), map(), map(), map(), map()) ::
          {:cont, {:ok, map()}} | {:halt, {:error, term()}}
  defp process_storage_service_locking(
         storage_id,
         available_services,
         recovery_attempt,
         context,
         locked_services
       ) do
    case Map.get(available_services, storage_id) do
      {_kind, last_seen} = service ->
        handle_storage_service_locking(
          service,
          last_seen,
          storage_id,
          recovery_attempt,
          context,
          locked_services
        )

      _ ->
        # Service not available - this shouldn't happen if recruitment logic is correct
        {:halt, {:error, {:recruited_service_unavailable, storage_id}}}
    end
  end

  @spec handle_storage_service_locking(tuple(), tuple(), String.t(), map(), map(), map()) ::
          {:cont, {:ok, map()}} | {:halt, {:error, term()}}
  defp handle_storage_service_locking(
         service,
         last_seen,
         storage_id,
         recovery_attempt,
         context,
         locked_services
       ) do
    case lock_recruited_service(service, recovery_attempt.epoch, context) do
      {:ok, pid, info} ->
        locked_service = %{
          status: {:up, pid},
          kind: info.kind,
          last_seen: last_seen
        }

        {:cont, {:ok, Map.put(locked_services, storage_id, locked_service)}}

      {:error, :newer_epoch_exists} = error ->
        {:halt, error}

      {:error, reason} ->
        {:halt, {:error, {:failed_to_lock_recruited_service, storage_id, reason}}}
    end
  end

  @spec lock_recruited_service({atom(), {atom(), node()}}, pos_integer(), map()) ::
          {:ok, pid(), map()} | {:error, term()}
  defp lock_recruited_service(service, epoch, context) do
    lock_service_for_recovery(service, epoch, context)
  end

  @spec lock_service_for_recovery(
          {atom(), {atom(), node()}},
          pos_integer(),
          map()
        ) ::
          {:ok, pid(), map()} | {:error, term()}
  def lock_service_for_recovery(service, epoch, context \\ %{}) do
    lock_fn = Map.get(context, :lock_service_fn, &lock_service_impl/2)
    lock_fn.(service, epoch)
  end

  @spec lock_service_impl({atom(), {atom(), node()}}, pos_integer()) ::
          {:ok, pid(), map()} | {:error, term()}
  defp lock_service_impl({:storage, name}, epoch),
    do: Storage.lock_for_recovery(name, epoch)
end
