defmodule Bedrock.ControlPlane.ClusterController.Recovery do
  @moduledoc false

  alias Bedrock.ControlPlane.ClusterController.State
  alias Bedrock.ControlPlane.Config
  alias Bedrock.ControlPlane.Config.RecoveryAttempt

  import Bedrock.Internal.Time

  import Bedrock.ControlPlane.ClusterController.Recovery.LockingAvailableServices,
    only: [lock_available_services: 4]

  import Bedrock.ControlPlane.ClusterController.Recovery.DeterminingSuitableLogs,
    only: [determine_suitable_logs: 3]

  import Bedrock.ControlPlane.ClusterController.Recovery.DeterminingDurableVersion,
    only: [determine_durable_version: 3]

  import Bedrock.Internal.Time, only: [now: 0]

  import Bedrock.ControlPlane.Config.Changes,
    only: [
      put_epoch: 2,
      put_recovery_attempt: 2,
      update_recovery_attempt: 2,
      update_transaction_system_layout: 2
    ]

  import Bedrock.ControlPlane.Config.TransactionSystemLayout.Tools,
    only: [
      put_controller: 2
    ]

  import Bedrock.ControlPlane.ClusterController.State.Changes,
    only: [
      update_config: 2
    ]

  import Bedrock.ControlPlane.ClusterController.Telemetry

  @spec claim_config(State.t()) :: State.t()
  def claim_config(t) do
    t
    |> update_config(fn config ->
      config
      |> put_epoch(t.epoch)
      |> update_transaction_system_layout(fn tsl ->
        tsl
        |> put_controller(self())
      end)
    end)
  end

  @spec start_new_recovery_attempt(State.t()) :: State.t()
  def start_new_recovery_attempt(t) when is_nil(t.config.recovery_attempt) do
    t
    |> update_config(fn config ->
      config
      |> put_recovery_attempt(
        RecoveryAttempt.new(
          t.epoch,
          now(),
          config.parameters.desired_logs,
          config.parameters.replication_factor,
          config.transaction_system_layout
        )
        |> RecoveryAttempt.put_available_services(t.config.transaction_system_layout.services)
      )
      |> update_transaction_system_layout(
        &%{
          &1
          | sequencer: nil,
            rate_keeper: nil,
            data_distributor: nil,
            proxies: [],
            transaction_resolvers: []
        }
      )
    end)
  end

  def start_new_recovery_attempt(t), do: t

  @spec try_to_fix_stalled_recovery_if_needed(State.t()) :: State.t()
  def try_to_fix_stalled_recovery_if_needed(t) do
    if RecoveryAttempt.stalled?(t.config.recovery_attempt) do
      t |> recover()
    else
      t
    end
  end

  @spec recover(State.t()) :: State.t()
  def recover(t) do
    :ok = trace_recovery_attempt_started(t)

    t
    |> update_config(fn config ->
      config
      |> update_recovery_attempt(fn recovery_attempt ->
        recovery_attempt
        |> RecoveryAttempt.put_available_services(t.config.transaction_system_layout.services)
        |> run_recovery_attempt()
        |> case do
          {:ok, new_recovery_attempt} ->
            IO.inspect("recovery attempt completed")
            new_recovery_attempt

          {{:stalled, reason}, new_recovery_attempt} ->
            IO.inspect("recovery stalled: #{reason}")
            new_recovery_attempt
        end
      end)
    end)
  end

  @spec run_recovery_attempt(RecoveryAttempt.t()) ::
          {:ok, RecoveryAttempt.t()}
          | {{:stalled, RecoveryAttempt.reason_for_stall()}, RecoveryAttempt.t()}
          | {:error, term()}
  def run_recovery_attempt(t) do
    case recovery(t) do
      %{state: :running} ->
        {:ok, t}

      %{state: {:stalled, _reason} = stalled} = t ->
        {stalled, t}

      %{state: new_state} = new_t when t.state != new_state ->
        new_t |> run_recovery_attempt()
    end
  end

  def recovery(%{state: :start} = t) do
    t
    |> RecoveryAttempt.put_started_at(now())
    |> RecoveryAttempt.put_state(:lock_available_services)
  end

  #
  #
  def recovery(%{state: {:stalled, _}} = t),
    do: t |> RecoveryAttempt.reset(now())

  #
  #
  def recovery(%{state: :lock_available_services} = t) do
    lock_available_services(t.available_services, t.locked_service_ids, t.epoch, 200)
    |> case do
      {:error, :newer_epoch_exists = reason} ->
        t |> RecoveryAttempt.put_state({:stalled, reason})

      {:ok, [], _, _} ->
        t |> RecoveryAttempt.put_state({:stalled, :waiting_for_services})

      {:ok, locked_services, log_recovery_info_by_id, storage_recovery_info_by_id} ->
        t
        |> RecoveryAttempt.update_log_recovery_info_by_id(&Enum.into(log_recovery_info_by_id, &1))
        |> RecoveryAttempt.update_storage_recovery_info_by_id(
          &Enum.into(storage_recovery_info_by_id, &1)
        )
        |> RecoveryAttempt.update_available_services(fn available_services ->
          available_services
          |> Map.new(&{&1.id, &1})
          |> Map.merge(locked_services |> Map.new(&{&1.id, &1}))
          |> Map.values()
          |> Enum.sort_by(& &1.id)
        end)
        |> RecoveryAttempt.update_locked_service_ids(fn locked_service_ids ->
          locked_service_ids
          |> Enum.concat(locked_services |> Enum.map(& &1.id))
          |> Enum.uniq()
          |> Enum.sort()
        end)
        |> RecoveryAttempt.put_state(:determine_suitable_logs)
    end
  end

  #
  #
  def recovery(%{state: :determine_suitable_logs} = t) do
    determine_suitable_logs(
      t.last_transaction_system_layout.logs,
      t.log_recovery_info_by_id,
      t.desired_logs |> determine_quorum()
    )
    |> case do
      {:error, :unable_to_meet_log_quorum = reason} ->
        t |> RecoveryAttempt.put_state({:stalled, reason})

      {:ok, log_ids, version_vector} ->
        t
        |> RecoveryAttempt.put_suitable_log_ids(log_ids)
        |> RecoveryAttempt.put_version_vector(version_vector)
        |> RecoveryAttempt.put_state(:determine_durable_version)
    end
  end

  #
  #
  def recovery(%{state: :determine_durable_version} = t) do
    determine_durable_version(
      t.last_transaction_system_layout.storage_teams,
      t.storage_recovery_info_by_id,
      t.desired_replication_factor |> determine_quorum()
    )
    |> case do
      {:error, {:insufficient_storage, _failed_tags} = reason} ->
        t |> RecoveryAttempt.put_state({:stalled, reason})

      {:ok, durable_version, degraded_teams} ->
        t
        |> RecoveryAttempt.put_durable_version(durable_version)
        |> RecoveryAttempt.put_degraded_teams(degraded_teams)
        |> RecoveryAttempt.put_state(:recruiting)
    end
  end

  def recovery(%{state: :recruiting} = t) do
    %{t | state: :replaying_logs}
  end

  def recovery(%{state: :replaying_logs} = t) do
    %{t | state: :repairing_data_distribution}
  end

  def recovery(%{state: :repairing_data_distribution} = t) do
    %{t | state: :defining_proxies_and_resolvers}
  end

  def recovery(%{state: :defining_proxies_and_resolvers} = t) do
    %{t | state: :final_checks}
  end

  def recovery(%{state: :final_checks} = t) do
    %{t | state: :running}
  end

  def recovery(t), do: raise("Invalid state: #{inspect(t)}")

  defp determine_quorum(n) when is_integer(n), do: 1 + div(n, 2)
end
