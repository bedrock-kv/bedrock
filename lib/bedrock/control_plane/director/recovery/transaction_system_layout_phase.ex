defmodule Bedrock.ControlPlane.Director.Recovery.TransactionSystemLayoutPhase do
  @moduledoc """
  Constructs the Transaction System Layout, the critical blueprint defining how
  the distributed system operates.

  After validation confirms all components are ready (see `ValidationPhase`),
  this phase builds the complete system topology. The layout contains the
  sequencer, commit proxies, resolvers, logs, storage teams, and service
  descriptors that represent the recovered cluster's architecture.

  Construction begins by generating a unique layout ID and mapping discovered
  services to their descriptors. Once the topology is complete, all services are
  unlocked in parallel to prepare them for transaction processing. The unlocking
  step is essential since the subsequent system transaction requires responsive
  services.

  Failures during construction or unlocking indicate topology problems or
  service communication issues that require manual intervention. The director
  will stall recovery rather than proceed with an incomplete or unresponsive
  system.

  Transitions to `PersistencePhase` to commit the layout through a system
  transaction.
  """

  alias Bedrock.ControlPlane.Config.RecoveryAttempt
  alias Bedrock.ControlPlane.Config.ServiceDescriptor
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.DataPlane.CommitProxy
  alias Bedrock.DataPlane.Storage

  use Bedrock.ControlPlane.Director.Recovery.RecoveryPhase

  import Bedrock.ControlPlane.Director.Recovery.Telemetry

  @impl true
  def execute(recovery_attempt, context) do
    with {:ok, transaction_system_layout} <-
           build_transaction_system_layout(recovery_attempt, context),
         :ok <-
           unlock_services(
             recovery_attempt,
             transaction_system_layout,
             context.lock_token,
             context
           ) do
      updated_recovery_attempt =
        %{recovery_attempt | transaction_system_layout: transaction_system_layout}

      {updated_recovery_attempt, Bedrock.ControlPlane.Director.Recovery.PersistencePhase}
    else
      {:error, reason} ->
        {recovery_attempt, {:stalled, {:recovery_system_failed, reason}}}
    end
  end

  defp build_transaction_system_layout(recovery_attempt, context) do
    {:ok,
     %{
       id: TransactionSystemLayout.random_id(),
       epoch: recovery_attempt.epoch,
       director: self(),
       sequencer: recovery_attempt.sequencer,
       rate_keeper: nil,
       proxies: recovery_attempt.proxies,
       resolvers: recovery_attempt.resolvers,
       logs: recovery_attempt.logs,
       storage_teams: recovery_attempt.storage_teams,
       services:
         build_services_for_layout(
           recovery_attempt,
           context
         )
     }}
  end

  @spec build_services_for_layout(RecoveryAttempt.t(), RecoveryPhase.context()) ::
          %{String.t() => ServiceDescriptor.t()}
  defp build_services_for_layout(recovery_attempt, context) do
    recovery_attempt
    |> extract_service_ids()
    |> Enum.map(fn service_id ->
      {service_id, build_service_descriptor(service_id, recovery_attempt, context)}
    end)
    |> Enum.reject(fn {_id, descriptor} -> is_nil(descriptor) end)
    |> Map.new()
  end

  defp extract_service_ids(recovery_attempt) do
    [extract_log_service_ids(recovery_attempt), extract_storage_service_ids(recovery_attempt)]
    |> Enum.reduce(MapSet.new(), &MapSet.union/2)
  end

  defp extract_log_service_ids(recovery_attempt) do
    recovery_attempt.logs
    |> Map.keys()
    |> MapSet.new()
  end

  defp extract_storage_service_ids(recovery_attempt) do
    recovery_attempt.storage_teams
    |> Enum.flat_map(& &1.storage_ids)
    |> MapSet.new()
  end

  @spec build_service_descriptor(
          String.t(),
          RecoveryAttempt.t(),
          RecoveryPhase.context()
        ) :: ServiceDescriptor.t() | nil
  defp build_service_descriptor(service_id, recovery_attempt, context) do
    case Map.get(context.available_services, service_id) do
      {kind, last_seen} = _service ->
        status = determine_service_status(service_id, recovery_attempt.service_pids)

        %{
          kind: kind,
          last_seen: last_seen,
          status: status
        }

      _ ->
        # Service not found in available services
        nil
    end
  end

  @spec determine_service_status(String.t(), %{String.t() => pid()}) :: ServiceDescriptor.status()
  defp determine_service_status(service_id, service_pids) do
    case Map.get(service_pids, service_id) do
      pid when is_pid(pid) -> {:up, pid}
      _ -> :down
    end
  end

  # Unlock commit proxies and storage servers before exercising the transaction system
  @spec unlock_services(
          RecoveryAttempt.t(),
          TransactionSystemLayout.t(),
          Bedrock.lock_token(),
          map()
        ) ::
          :ok | {:error, {:unlock_failed, :timeout | :unavailable}}
  defp unlock_services(
         recovery_attempt,
         transaction_system_layout,
         lock_token,
         context
       )
       when is_binary(lock_token) do
    with :ok <-
           unlock_commit_proxies(
             recovery_attempt.proxies,
             transaction_system_layout,
             lock_token,
             context
           ),
         :ok <-
           unlock_storage_servers(recovery_attempt, transaction_system_layout, context) do
      :ok
    else
      {:error, reason} -> {:error, {:unlock_failed, reason}}
    end
  end

  @spec unlock_commit_proxies([pid()], TransactionSystemLayout.t(), Bedrock.lock_token(), map()) ::
          :ok | {:error, :timeout | :unavailable}
  defp unlock_commit_proxies(proxies, transaction_system_layout, lock_token, context)
       when is_list(proxies) do
    unlock_fn = Map.get(context, :unlock_commit_proxy_fn, &CommitProxy.recover_from/3)

    proxies
    |> Task.async_stream(
      &unlock_fn.(&1, lock_token, transaction_system_layout),
      ordered: false
    )
    |> Enum.reduce_while(:ok, fn
      {:ok, :ok}, :ok -> {:cont, :ok}
      {:ok, {:error, reason}}, _ -> {:halt, {:error, {:commit_proxy_unlock_failed, reason}}}
      {:exit, reason}, _ -> {:halt, {:error, {:commit_proxy_unlock_crashed, reason}}}
    end)
  end

  @spec unlock_storage_servers(RecoveryAttempt.t(), TransactionSystemLayout.t(), map()) ::
          :ok | {:error, :timeout | :unavailable}
  defp unlock_storage_servers(recovery_attempt, transaction_system_layout, context) do
    durable_version = recovery_attempt.durable_version
    unlock_fn = Map.get(context, :unlock_storage_fn, &Storage.unlock_after_recovery/3)

    transaction_system_layout.storage_teams
    |> Enum.flat_map(fn %{storage_ids: storage_ids} -> storage_ids end)
    |> Enum.uniq()
    |> Enum.map(fn storage_id ->
      recovery_attempt.transaction_services
      |> Map.fetch!(storage_id)
      |> then(fn %{status: {:up, pid}} -> {storage_id, pid} end)
    end)
    |> Task.async_stream(
      fn {storage_id, storage_pid} ->
        {storage_id, unlock_fn.(storage_pid, durable_version, transaction_system_layout)}
      end,
      ordered: false,
      timeout: 5000
    )
    |> Enum.reduce_while(:ok, fn
      {:ok, {storage_id, :ok}}, :ok ->
        trace_recovery_storage_unlocking(storage_id)
        {:cont, :ok}

      {:ok, {_storage_id, {:error, reason}}}, _ ->
        {:halt, {:error, {:storage_unlock_failed, reason}}}

      {:exit, reason}, _ ->
        {:halt, {:error, {:storage_unlock_crashed, reason}}}
    end)
  end
end
