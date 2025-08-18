defmodule Bedrock.ControlPlane.Director.Recovery.LogRecruitmentPhase do
  @moduledoc """
  Transforms abstract vacancy placeholders into concrete service assignments.

  Solves the practical challenge of assigning real services to fill log vacancies while
  balancing efficiency with safety—reusing existing services when possible but avoiding
  services from the old system that contain recovery data.

  **Three-Phase Assignment Strategy**:
  1. Prefer existing log services that weren't part of the old transaction system
  2. Create new log workers using round-robin distribution across nodes when needed
  3. Lock all recruited services (existing and new) to establish exclusive control

  **Constraints**: Old system services are excluded to preserve committed transaction data
  in case this recovery fails and another one starts. All recruited services must be
  successfully locked before proceeding to ensure readiness for transaction processing.

  Stalls if insufficient nodes exist for worker creation or if recruited services fail
  to lock. However, immediately halts with error if any service is locked by a newer
  epoch (this director has been superseded). Transitions to storage recruitment with
  complete log service assignments.

  See the Log Recruitment section in `docs/knowlege_base/02-deep/recovery-narrative.md`
  for detailed explanation of the assignment strategy and fault tolerance approach.
  """

  use Bedrock.ControlPlane.Director.Recovery.RecoveryPhase

  import Bedrock.ControlPlane.Director.Recovery.Telemetry

  alias Bedrock.DataPlane.Log
  alias Bedrock.Service.Foreman
  alias Bedrock.Service.Worker

  require Logger

  @impl true
  def execute(recovery_attempt, context) do
    old_system_log_ids = get_old_system_log_ids(context)
    available_log_ids = get_available_log_ids(context)
    available_log_nodes = get_available_log_nodes(context)

    with {:ok, logs, new_worker_ids} <-
           fill_log_vacancies(
             recovery_attempt.logs,
             old_system_log_ids,
             available_log_ids,
             available_log_nodes
           ),
         {:ok, updated_services} <-
           create_new_log_workers(new_worker_ids, available_log_nodes, recovery_attempt, context),
         {:ok, existing_log_services} <-
           extract_and_lock_existing_log_services(
             logs,
             context.available_services,
             recovery_attempt,
             context
           ) do
      trace_recovery_all_log_vacancies_filled()

      all_log_services = Map.merge(existing_log_services, updated_services)
      all_log_pids = extract_service_pids(all_log_services)

      trace_recovery_log_recruitment_completed(
        Map.keys(logs),
        all_log_pids,
        context.available_services,
        updated_services
      )

      updated_recovery_attempt =
        recovery_attempt
        |> Map.put(:logs, logs)
        |> Map.update(:transaction_services, %{}, &Map.merge(&1, all_log_services))
        |> Map.update(:service_pids, %{}, &Map.merge(&1, all_log_pids))

      {updated_recovery_attempt, Bedrock.ControlPlane.Director.Recovery.StorageRecruitmentPhase}
    else
      {:error, :newer_epoch_exists} = error ->
        {recovery_attempt, error}

      {:error, reason} ->
        {recovery_attempt, {:stalled, reason}}
    end
  end

  defp get_old_system_log_ids(%{old_transaction_system_layout: %{logs: old_logs}}),
    do: old_logs |> Map.keys() |> MapSet.new()

  defp get_old_system_log_ids(_), do: MapSet.new()

  defp get_available_log_ids(%{available_services: available_services}) do
    available_services
    |> Enum.filter(fn {_id, {kind, _}} -> kind == :log end)
    |> MapSet.new(&elem(&1, 0))
  end

  defp get_available_log_nodes(%{node_capabilities: node_capabilities}), do: Map.get(node_capabilities, :log, [])

  @spec fill_log_vacancies(
          logs :: %{Log.id() => any()},
          old_system_log_ids :: MapSet.t(Log.id()),
          available_log_ids :: MapSet.t(Log.id()),
          available_nodes :: [node()]
        ) ::
          {:ok, %{Log.id() => any()}, [Log.id()]}
          | {:error, {:insufficient_nodes, needed_workers :: pos_integer(), available_nodes :: non_neg_integer()}}
  def fill_log_vacancies(logs, old_system_log_ids, available_log_ids, available_nodes) do
    vacancies = all_vacancies(logs)
    n_vacancies = MapSet.size(vacancies)

    candidates_ids = MapSet.difference(available_log_ids, old_system_log_ids)
    n_candidates = MapSet.size(candidates_ids)

    if n_vacancies <= n_candidates do
      {:ok,
       replace_vacancies_with_log_ids(
         logs,
         vacancies |> Enum.zip(candidates_ids) |> Map.new()
       ), []}
    else
      needed_workers = n_vacancies - n_candidates

      if length(available_nodes) < needed_workers do
        {:error, {:insufficient_nodes, needed_workers, length(available_nodes)}}
      else
        new_worker_ids =
          Enum.map(1..needed_workers, fn _ -> Worker.random_id() end)

        all_worker_ids = Enum.concat(candidates_ids, new_worker_ids)

        {:ok,
         replace_vacancies_with_log_ids(
           logs,
           vacancies |> Enum.zip(all_worker_ids) |> Map.new()
         ), new_worker_ids}
      end
    end
  end

  @spec all_vacancies(%{Log.id() => [term()]}) :: MapSet.t()
  def all_vacancies(logs) do
    logs
    |> Enum.reduce([], fn
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
    Map.new(logs, fn {log_id, descriptor} ->
      case Map.get(log_id_for_vacancy, log_id) do
        nil -> {log_id, descriptor}
        candidate_id -> {candidate_id, descriptor}
      end
    end)
  end

  @spec create_new_log_workers([String.t()], [node()], map(), RecoveryPhase.context()) ::
          {:ok, %{String.t() => map()}} | {:error, term()}
  defp create_new_log_workers([], _available_nodes, _recovery_attempt, _context), do: {:ok, %{}}

  defp create_new_log_workers(new_worker_ids, available_nodes, recovery_attempt, context) do
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

    with {:ok, worker_ref} <- create_worker_fn.(foreman_ref, worker_id, :log, timeout: 10_000),
         {:ok, worker_info} <-
           worker_info_fn.({worker_ref, node}, [:id, :otp_name, :kind, :pid], []) do
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
    Logger.warning("Some workers failed to be tracked during creation: #{inspect(failed_workers)}")
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
    |> Map.new(fn {id, %{status: {:up, pid}}} -> {id, pid} end)
  end

  @spec extract_and_lock_existing_log_services(
          %{Log.id() => any()},
          %{String.t() => map()},
          map(),
          map()
        ) ::
          {:ok, %{String.t() => map()}} | {:error, term()}
  defp extract_and_lock_existing_log_services(logs, available_services, recovery_attempt, context) do
    existing_log_ids =
      logs
      |> Map.keys()
      |> Enum.reject(&match?({:vacancy, _}, &1))

    Enum.reduce_while(existing_log_ids, {:ok, %{}}, fn log_id, {:ok, locked_services} ->
      process_log_service_locking(
        log_id,
        available_services,
        recovery_attempt,
        context,
        locked_services
      )
    end)
  end

  @spec process_log_service_locking(String.t(), map(), map(), map(), map()) ::
          {:cont, {:ok, map()}} | {:halt, {:error, term()}}
  defp process_log_service_locking(log_id, available_services, recovery_attempt, context, locked_services) do
    case Map.get(available_services, log_id) do
      {_kind, last_seen} = service ->
        handle_log_service_locking(
          service,
          last_seen,
          log_id,
          recovery_attempt,
          context,
          locked_services
        )

      _ ->
        {:halt, {:error, {:recruited_service_unavailable, log_id}}}
    end
  end

  @spec handle_log_service_locking(tuple(), tuple(), String.t(), map(), map(), map()) ::
          {:cont, {:ok, map()}} | {:halt, {:error, term()}}
  defp handle_log_service_locking(service, last_seen, log_id, recovery_attempt, context, locked_services) do
    case lock_recruited_service(service, recovery_attempt.epoch, context) do
      {:ok, pid, info} ->
        locked_service = %{
          status: {:up, pid},
          kind: info.kind,
          last_seen: last_seen
        }

        {:cont, {:ok, Map.put(locked_services, log_id, locked_service)}}

      {:error, :newer_epoch_exists} = error ->
        {:halt, error}

      {:error, reason} ->
        {:halt, {:error, {:failed_to_lock_recruited_service, log_id, reason}}}
    end
  end

  @spec lock_recruited_service({atom(), {atom(), node()}}, pos_integer(), map()) ::
          {:ok, pid(), map()} | {:error, term()}
  defp lock_recruited_service(service, epoch, context) do
    lock_service_for_recovery(service, epoch, context)
  end

  @spec lock_service_for_recovery(
          {atom(), {atom(), node()}},
          Bedrock.epoch(),
          map()
        ) ::
          {:ok, pid(), map()} | {:error, term()}
  def lock_service_for_recovery(service, epoch, context \\ %{}) do
    lock_fn = Map.get(context, :lock_service_fn, &lock_service_impl/2)
    lock_fn.(service, epoch)
  end

  @spec lock_service_impl({atom(), {atom(), node()}}, Bedrock.epoch()) ::
          {:ok, pid(), map()} | {:error, term()}
  defp lock_service_impl({:log, name}, epoch), do: Log.lock_for_recovery(name, epoch)
end
