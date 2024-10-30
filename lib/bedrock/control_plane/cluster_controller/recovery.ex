defmodule Bedrock.ControlPlane.ClusterController.Recovery do
  @moduledoc false

  alias Bedrock.ControlPlane.ClusterController.State
  alias Bedrock.ControlPlane.Config.RecoveryAttempt

  alias Bedrock.ControlPlane.Config.TransactionSystemLayout

  import Bedrock.Internal.Time

  import __MODULE__.LockingAvailableServices, only: [lock_available_services: 4]
  import __MODULE__.DeterminingOldLogsToCopy, only: [determine_old_logs_to_copy: 3]
  import __MODULE__.DeterminingDurableVersion, only: [determine_durable_version: 3]
  import __MODULE__.FillingLogVacancies, only: [fill_log_vacancies: 3]
  import __MODULE__.FillingStorageTeamVacancies, only: [fill_storage_team_vacancies: 2]

  import Bedrock.Internal.Time, only: [now: 0]

  import Bedrock.ControlPlane.Config.Changes,
    only: [
      put_epoch: 2,
      put_recovery_attempt: 2,
      update_recovery_attempt: 2,
      update_transaction_system_layout: 2
    ]

  import Bedrock.ControlPlane.ClusterController.State.Changes,
    only: [put_state: 2, update_config: 2]

  import Bedrock.ControlPlane.ClusterController.Telemetry

  import Bedrock.ControlPlane.Config.StorageTeamDescriptor, only: [storage_team_descriptor: 3]
  import Bedrock.ControlPlane.Config.LogDescriptor, only: [log_descriptor: 2]

  @spec try_to_recover(State.t()) :: State.t()
  def try_to_recover(t) do
    case t.state do
      :starting ->
        t
        |> put_state(:recovery)
        |> update_config(fn config ->
          config
          |> put_epoch(t.epoch)
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
          |> update_transaction_system_layout(fn transaction_system_layout ->
            transaction_system_layout
            |> TransactionSystemLayout.Changes.put_controller(self())
            |> TransactionSystemLayout.Changes.put_sequencer(nil)
            |> TransactionSystemLayout.Changes.put_rate_keeper(nil)
            |> TransactionSystemLayout.Changes.put_data_distributor(nil)
            |> TransactionSystemLayout.Changes.put_proxies([])
            |> TransactionSystemLayout.Changes.put_transaction_resolvers([])
          end)
        end)
        |> recover()

      :recovery ->
        t
        |> update_config(fn config ->
          config
          |> update_recovery_attempt(fn recovery_attempt ->
            recovery_attempt
            |> RecoveryAttempt.reset(now())
            |> RecoveryAttempt.put_available_services(t.config.transaction_system_layout.services)
          end)
        end)
        |> recover()

      :stopped ->
        t

      :running ->
        t
    end
  end

  @spec recover(State.t()) :: State.t()
  def recover(t) do
    :ok = trace_recovery_attempt_started(t)

    t.config.recovery_attempt
    |> run_recovery_attempt()
    |> case do
      {:ok, completed_recovery_attempt} ->
        t |> apply_completed_recovery_attempt(completed_recovery_attempt)

      {{:stalled, reason}, stalled_recovery_attempt} ->
        IO.inspect(reason, label: "Recovery attempt stalled")
        t |> update_stalled_recovery_attempt(stalled_recovery_attempt)
    end
  end

  def apply_completed_recovery_attempt(t, completed_recovery_attempt) do
    t
    |> put_state(:running)
    |> update_config(fn config ->
      config
      |> put_recovery_attempt(nil)
      |> update_transaction_system_layout(fn transaction_system_layout ->
        transaction_system_layout
        |> TransactionSystemLayout.put_logs(completed_recovery_attempt.logs)
        |> TransactionSystemLayout.put_storage_teams(completed_recovery_attempt.storage_teams)
      end)
    end)
  end

  def update_stalled_recovery_attempt(t, stalled_recovery_attempt) do
    t
    |> update_config(fn config ->
      config
      |> put_recovery_attempt(stalled_recovery_attempt)
    end)
  end

  @spec key_range(Bedrock.key(), Bedrock.key()) :: Bedrock.key_range()
  def key_range(min_key, max_key_exclusive) when min_key < max_key_exclusive,
    do: {min_key, max_key_exclusive}

  @spec run_recovery_attempt(RecoveryAttempt.t()) ::
          {:ok, RecoveryAttempt.t()}
          | {{:stalled, RecoveryAttempt.reason_for_stall()}, RecoveryAttempt.t()}
          | {:error, term()}
  def run_recovery_attempt(t) do
    case recovery(t) do
      %{state: :completed} = t ->
        {:ok, t}

      %{state: {:stalled, _reason} = stalled} = t ->
        {stalled, t}

      %{state: new_state} = new_t when t.state != new_state ->
        new_t |> run_recovery_attempt()
    end
  end

  #
  #
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

      {:ok, locked_services, log_recovery_info_by_id, storage_recovery_info_by_id} ->
        t
        |> RecoveryAttempt.update_log_recovery_info_by_id(&Map.merge(log_recovery_info_by_id, &1))
        |> RecoveryAttempt.update_storage_recovery_info_by_id(
          &Map.merge(storage_recovery_info_by_id, &1)
        )
        |> RecoveryAttempt.update_available_services(fn available_services ->
          available_services
          |> Map.new(&{&1.id, &1})
          |> Map.merge(locked_services |> Map.new(&{&1.id, &1}))
          |> Map.values()
        end)
        |> RecoveryAttempt.update_locked_service_ids(fn locked_service_ids ->
          locked_services |> Enum.map(& &1.id) |> Enum.into(locked_service_ids)
        end)
        |> case do
          %{last_transaction_system_layout: %{logs: [], storage_teams: []}} = t ->
            t |> RecoveryAttempt.put_state(:first_time_initialization)

          t ->
            t |> RecoveryAttempt.put_state(:determine_old_logs_to_copy)
        end
    end
  end

  # Initialize a new system with empty logs and storage teams by creating
  # placeholders based on desired logs and replication factor, then proceed
  # to fill log vacancies.
  def recovery(%{state: :first_time_initialization} = t) do
    log_vacancies = 1..t.desired_logs |> Enum.map(&{:vacancy, &1})
    storage_team_vacancies = 1..t.desired_replication_factor |> Enum.map(&{:vacancy, &1})

    t
    |> RecoveryAttempt.put_durable_version(0)
    |> RecoveryAttempt.put_version_vector({:undefined, 0})
    |> RecoveryAttempt.put_logs(
      log_vacancies
      |> Enum.map(&log_descriptor(&1, [0, 1]))
    )
    |> RecoveryAttempt.put_storage_teams([
      storage_team_descriptor(
        0,
        key_range(<<0xFF>>, <<0xFF, 0xFF>>),
        storage_team_vacancies
      ),
      storage_team_descriptor(
        1,
        key_range(<<>>, <<0xFF>>),
        storage_team_vacancies
      )
    ])
    |> RecoveryAttempt.put_state(:recruit_logs_to_fill_vacancies)
  end

  #
  #
  def recovery(%{state: :determine_old_logs_to_copy} = t) do
    determine_old_logs_to_copy(
      t.last_transaction_system_layout.logs,
      t.log_recovery_info_by_id,
      t.desired_logs |> determine_quorum()
    )
    |> case do
      {:error, :unable_to_meet_log_quorum = reason} ->
        t |> RecoveryAttempt.put_state({:stalled, reason})

      {:ok, log_ids, version_vector} ->
        t
        |> RecoveryAttempt.put_old_log_ids_to_copy(log_ids)
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
      {:error, {:insufficient_replication, _failed_tags} = reason} ->
        t |> RecoveryAttempt.put_state({:stalled, reason})

      {:ok, durable_version, degraded_teams} ->
        t
        |> RecoveryAttempt.put_durable_version(durable_version)
        |> RecoveryAttempt.put_degraded_teams(degraded_teams)
        |> RecoveryAttempt.put_state(:recruit_logs_to_fill_vacancies)
    end
  end

  #
  #
  def recovery(%{state: :recruit_logs_to_fill_vacancies} = t) do
    fill_log_vacancies(
      t.logs,
      t.last_transaction_system_layout.logs |> MapSet.new(& &1.log_id),
      t.log_recovery_info_by_id |> Map.keys() |> MapSet.new()
    )
    |> case do
      {:error, {:need_log_workers, _} = reason} ->
        t |> RecoveryAttempt.put_state({:stalled, reason})

      {:error, :no_vacancies_to_fill} ->
        t |> RecoveryAttempt.put_state(:determine_old_logs_to_copy)

      {:ok, logs} ->
        t
        |> RecoveryAttempt.put_logs(logs)
        |> RecoveryAttempt.put_state(:recruit_storage_to_fill_vacancies)
    end
  end

  #
  #
  def recovery(%{state: :recruit_storage_to_fill_vacancies} = t) do
    fill_storage_team_vacancies(
      t.storage_teams,
      t.storage_recovery_info_by_id |> Map.keys() |> MapSet.new()
    )
    |> case do
      {:error, {:need_storage_workers, _} = reason} ->
        t |> RecoveryAttempt.put_state({:stalled, reason})

      {:error, :no_vacancies_to_fill} ->
        t |> RecoveryAttempt.put_state(:replay_old_logs)

      {:ok, storage_teams} ->
        t
        |> RecoveryAttempt.put_storage_teams(storage_teams)
        |> RecoveryAttempt.put_state(:replay_old_logs)
    end
  end

  #
  #
  def recovery(%{state: :replay_old_logs} = t) do
    t |> RecoveryAttempt.put_state(:repair_data_distribution)
  end

  #
  #
  def recovery(%{state: :repair_data_distribution} = t) do
    t |> RecoveryAttempt.put_state(:defining_proxies_and_resolvers)
  end

  #
  #
  def recovery(%{state: :defining_proxies_and_resolvers} = t) do
    t |> RecoveryAttempt.put_state(:final_checks)
  end

  #
  #
  def recovery(%{state: :final_checks} = t) do
    t |> RecoveryAttempt.put_state(:completed)
  end

  def recovery(%{state: :completed} = t) do
    t
  end

  #
  #
  def recovery(t), do: raise("Invalid state: #{inspect(t)}")

  defp determine_quorum(n) when is_integer(n), do: 1 + div(n, 2)
end
