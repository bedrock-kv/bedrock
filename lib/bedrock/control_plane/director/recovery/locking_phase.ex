defmodule Bedrock.ControlPlane.Director.Recovery.LockingPhase do
  @moduledoc """
  Establishes exclusive director control by selectively locking services from the old system layout.

  Service locking serves three purposes: prevent split-brain scenarios where multiple directors
  attempt concurrent control, halt all transaction processing from the old system, and validate
  service reachability while collecting recovery state information (transaction versions,
  durability status).

  Only services referenced in the old transaction system layout are locked - these contain
  data that must be preserved during recovery. Individual service failures (unreachable, timeout)
  are ignored since recovery gathers as many services as possible from a potentially failed system.
  However, if any service is already locked with a newer epoch, this director has been superseded
  and should stop all recovery attempts.

  The recovery path is determined by whether the old layout contained logs: no logs means
  first-time initialization, logs present means recovery from existing data.

  """

  use Bedrock.ControlPlane.Director.Recovery.RecoveryPhase

  alias Bedrock.ControlPlane.Config.CoreState
  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.Materializer
  alias Bedrock.Service.RecoveryAuthority
  alias Bedrock.Service.Worker

  @impl true
  def execute(recovery_attempt, context) do
    old_system_services =
      extract_old_system_services(
        context.prior_core_state,
        context.available_services
      )

    old_system_services
    |> lock_old_system_services(Map.fetch!(context, :recovery_authority), context)
    |> case do
      {:error, :newer_epoch_exists} = error ->
        {recovery_attempt, error}

      {:ok, locked_service_ids, log_recovery_info_by_id, materializer_recovery_info_by_id, transaction_services,
       service_pids} ->
        updated_recovery_attempt =
          recovery_attempt
          |> Map.update!(:log_recovery_info_by_id, &Map.merge(log_recovery_info_by_id, &1))
          |> Map.update!(
            :materializer_recovery_info_by_id,
            &Map.merge(materializer_recovery_info_by_id, &1)
          )
          |> Map.put(:locked_service_ids, locked_service_ids)
          |> Map.update!(:transaction_services, &Map.merge(transaction_services, &1))
          |> Map.update!(:service_pids, &Map.merge(service_pids, &1))

        {updated_recovery_attempt, Bedrock.ControlPlane.Director.Recovery.LogRecoveryPlanningPhase}
    end
  end

  @spec lock_old_system_services_timeout() :: Bedrock.timeout_in_ms()
  def lock_old_system_services_timeout, do: 2_000

  @spec lock_old_system_services(
          %{Worker.id() => %{kind: atom(), last_seen: {atom(), node()}}},
          RecoveryAuthority.input(),
          map()
        ) ::
          {:ok, locked_ids :: MapSet.t(Worker.id()), new_log_recovery_info_by_id :: %{Log.id() => Log.recovery_info()},
           new_materializer_recovery_info_by_id :: %{Materializer.id() => Materializer.recovery_info()},
           transaction_services :: %{
             Worker.id() => %{
               status: {:up, pid()},
               kind: :log | :materializer,
               last_seen: {atom(), node()}
             }
           }, service_pids :: %{Worker.id() => pid()}}
          | {:error, :newer_epoch_exists}
  def lock_old_system_services(old_system_services, authority, context \\ %{}) do
    timeout_in_ms = lock_old_system_services_timeout()

    old_system_services
    |> Task.async_stream(
      fn {id, service} ->
        {id, service, lock_service_for_recovery(service, authority, context)}
      end,
      timeout: timeout_in_ms,
      on_timeout: :kill_task,
      ordered: false,
      zip_input_on_exit: true
    )
    |> Enum.reduce_while({MapSet.new(), %{}, %{}, %{}}, fn
      {:ok, {_, _, {:error, :newer_epoch_exists} = error}}, _ ->
        {:halt, error}

      {:ok, {id, service, {:ok, pid, info}}}, {locked_ids, info_by_id, transaction_services, service_pids} ->
        {:cont,
         {MapSet.put(locked_ids, id), Map.put(info_by_id, id, info),
          Map.put(transaction_services, id, %{
            status: {:up, pid},
            kind: info.kind,
            last_seen:
              case service do
                {_kind, location} -> location
                %{last_seen: location} -> location
              end
          }), Map.put(service_pids, id, pid)}}

      {:ok, {_id, _, {:error, _}}}, acc ->
        {:cont, acc}
    end)
    |> case do
      {:error, _reason} = error ->
        error

      {locked_ids, info_by_id, transaction_services, service_pids} ->
        grouped_recovery_info = Enum.group_by(info_by_id, &Map.get(elem(&1, 1), :kind))
        new_log_recovery_info_by_id = grouped_recovery_info |> Map.get(:log, []) |> Map.new()

        new_materializer_recovery_info_by_id =
          grouped_recovery_info |> Map.get(:materializer, []) |> Map.new()

        {:ok, locked_ids, new_log_recovery_info_by_id, new_materializer_recovery_info_by_id, transaction_services,
         service_pids}
    end
  end

  @spec lock_service_for_recovery(
          {atom(), {atom(), node()}},
          RecoveryAuthority.input(),
          map()
        ) ::
          {:ok, pid(), map()} | {:error, term()}
  def lock_service_for_recovery(service, authority, context \\ %{}) do
    lock_fn = Map.get(context, :lock_service_fn, &lock_service_impl/2)
    lock_fn.(service, authority)
  end

  @spec lock_service_impl({atom(), {atom(), node()}}, Bedrock.epoch()) ::
          {:ok, pid(), map()} | {:error, term()}
  defp lock_service_impl({:log, name}, authority), do: Log.lock_for_recovery(name, authority)

  defp lock_service_impl({:materializer, name}, authority), do: Materializer.lock_for_recovery(name, authority)

  defp lock_service_impl(_, _), do: {:error, :unavailable}

  @spec extract_old_system_services(map(), %{
          Worker.id() => {atom(), {atom(), node()}}
        }) ::
          %{Worker.id() => {atom(), {atom(), node()}}}
  # The services recovery must lock: the old layout's logs, and every
  # advertised materializer. Materializers carry the durable key-value
  # state — including the shard layout the bootstrap phase reads — and
  # locking them both fences old epochs and collects their recovery info
  # (durable version, shard assignment) for reuse.
  defp extract_old_system_services(prior_core_state, available_services) do
    old_log_ids = CoreState.log_ids(prior_core_state)

    available_services
    |> Enum.filter(fn
      {service_id, {:log, _location}} -> MapSet.member?(old_log_ids, service_id)
      {_service_id, {:materializer, _location}} -> true
      {service_id, _other} -> MapSet.member?(old_log_ids, service_id)
    end)
    |> Map.new()
  end
end
