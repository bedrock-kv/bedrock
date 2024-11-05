defmodule Bedrock.ControlPlane.Director.Recovery do
  @moduledoc false

  alias Bedrock.ControlPlane.Director.State
  alias Bedrock.ControlPlane.Config.RecoveryAttempt
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.ControlPlane.Config.ServiceDescriptor
  alias Bedrock.DataPlane.Storage

  import Bedrock.ControlPlane.Config.RecoveryAttempt, only: [recovery_attempt: 5]

  import Bedrock, only: [key_range: 2]
  import Bedrock.Internal.Time, only: [now: 0]

  import __MODULE__.LockingAvailableServices, only: [lock_available_services: 3]
  import __MODULE__.DeterminingOldLogsToCopy, only: [determine_old_logs_to_copy: 3]
  import __MODULE__.DeterminingDurableVersion, only: [determine_durable_version: 3]

  import __MODULE__.CreatingVacancies,
    only: [create_vacancies_for_logs: 2, create_vacancies_for_storage_teams: 2]

  import __MODULE__.FillingVacancies,
    only: [fill_log_vacancies: 3, fill_storage_team_vacancies: 2]

  import __MODULE__.ReplayingOldLogs, only: [replay_old_logs_into_new_logs: 4]

  import __MODULE__.DefiningProxiesAndResolvers,
    only: [define_commit_proxies: 6, define_resolvers: 6]

  import __MODULE__.StartingSequencer, only: [start_sequencer: 4]

  import Bedrock.ControlPlane.Config.Changes,
    only: [
      put_epoch: 2,
      put_recovery_attempt: 2,
      update_recovery_attempt!: 2,
      update_transaction_system_layout: 2
    ]

  import Bedrock.ControlPlane.Director.State.Changes,
    only: [put_state: 2, update_config: 2]

  import Bedrock.ControlPlane.Director.Telemetry

  import Bedrock.ControlPlane.Config.StorageTeamDescriptor, only: [storage_team_descriptor: 3]
  import Bedrock.ControlPlane.Config.LogDescriptor, only: [log_descriptor: 2]

  @spec try_to_recover(State.t()) :: State.t()
  def try_to_recover(%{state: :starting} = t) do
    t
    |> setup_for_initial_recovery()
    |> do_recovery()
  end

  def try_to_recover(%{state: :recovery} = t) do
    t
    |> setup_for_subsequent_recovery()
    |> do_recovery()
  end

  def try_to_recover(t), do: t

  def setup_for_initial_recovery(t) do
    t
    |> put_state(:recovery)
    |> update_config(fn config ->
      config
      |> put_epoch(t.epoch)
      |> put_recovery_attempt(
        recovery_attempt(
          t.cluster,
          t.epoch,
          now(),
          config.transaction_system_layout,
          Map.take(config.parameters, [
            :desired_logs,
            :desired_replication_factor,
            :desired_commit_proxies,
            :desired_resolvers
          ])
        )
        |> RecoveryAttempt.put_available_services(t.config.transaction_system_layout.services)
      )
      |> update_transaction_system_layout(fn transaction_system_layout ->
        transaction_system_layout
        |> TransactionSystemLayout.Changes.put_director(self())
        |> TransactionSystemLayout.Changes.put_sequencer(nil)
        |> TransactionSystemLayout.Changes.put_rate_keeper(nil)
        |> TransactionSystemLayout.Changes.put_data_distributor(nil)
        |> TransactionSystemLayout.Changes.put_proxies([])
        |> TransactionSystemLayout.Changes.put_resolvers([])
      end)
    end)
  end

  def setup_for_subsequent_recovery(t) do
    t
    |> update_config(fn config ->
      config
      |> update_recovery_attempt!(fn recovery_attempt ->
        recovery_attempt
        |> RecoveryAttempt.reset(now())
        |> RecoveryAttempt.put_available_services(t.config.transaction_system_layout.services)
      end)
    end)
  end

  @spec do_recovery(State.t()) :: State.t()
  def do_recovery(t) do
    trace_recovery_attempt_started(
      t.cluster,
      t.epoch,
      t.config.recovery_attempt.attempt,
      t.config.recovery_attempt.started_at
    )

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
        |> TransactionSystemLayout.Changes.put_sequencer(completed_recovery_attempt.sequencer)
        |> TransactionSystemLayout.Changes.put_resolvers(completed_recovery_attempt.resolvers)
        |> TransactionSystemLayout.Changes.put_proxies(completed_recovery_attempt.proxies)
        |> TransactionSystemLayout.Changes.put_logs(completed_recovery_attempt.logs)
        |> TransactionSystemLayout.Changes.put_storage_teams(
          completed_recovery_attempt.storage_teams
        )
      end)
    end)
    |> unlock_storage_after_recovery(completed_recovery_attempt.durable_version)
  end

  def unlock_storage_after_recovery(t, durable_version) do
    t.config.transaction_system_layout.services
    |> Enum.filter(&(&1.kind == :storage))
    |> Enum.each(fn %{status: {:up, worker}} = storage_descriptor ->
      trace_recovery_storage_unlocking(storage_descriptor.id)

      Storage.unlock_after_recovery(worker, durable_version, t.config.transaction_system_layout,
        timeout_in_ms: 1_000
      )
    end)

    t
  end

  def update_stalled_recovery_attempt(t, stalled_recovery_attempt) do
    t
    |> update_config(fn config ->
      config
      |> put_recovery_attempt(stalled_recovery_attempt)
    end)
  end

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
    lock_available_services(t.available_services, t.epoch, 200)
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
    log_vacancies =
      1..t.parameters.desired_logs |> Enum.map(&{:vacancy, &1})

    storage_team_vacancies =
      1..t.parameters.desired_replication_factor |> Enum.map(&{:vacancy, &1})

    t
    |> RecoveryAttempt.put_durable_version(0)
    |> RecoveryAttempt.put_old_log_ids_to_copy([])
    |> RecoveryAttempt.put_version_vector({:start, 0})
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
      t.parameters.desired_logs |> determine_quorum()
    )
    |> case do
      {:error, :unable_to_meet_log_quorum = reason} ->
        t |> RecoveryAttempt.put_state({:stalled, reason})

      {:ok, log_ids, version_vector} ->
        t
        |> RecoveryAttempt.put_old_log_ids_to_copy(log_ids)
        |> RecoveryAttempt.put_version_vector(version_vector)
        |> RecoveryAttempt.put_state(:create_vacancies)
    end
  end

  #
  #
  def recovery(%{state: :create_vacancies} = t) do
    t
    |> RecoveryAttempt.put_logs(
      create_vacancies_for_logs(
        t.last_transaction_system_layout.logs,
        t.parameters.desired_logs
      )
    )
    |> RecoveryAttempt.put_storage_teams(
      create_vacancies_for_storage_teams(
        t.last_transaction_system_layout.storage_teams,
        t.parameters.desired_replication_factor
      )
    )
    |> RecoveryAttempt.put_state(:determine_durable_version)
  end

  #
  #
  def recovery(%{state: :determine_durable_version} = t) do
    determine_durable_version(
      t.last_transaction_system_layout.storage_teams,
      t.storage_recovery_info_by_id,
      t.parameters.desired_replication_factor |> determine_quorum()
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
        t |> RecoveryAttempt.put_state(:recruit_storage_to_fill_vacancies)

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
    replay_old_logs_into_new_logs(
      t.old_log_ids_to_copy,
      t.logs |> Enum.map(& &1.log_id),
      t.version_vector,
      &ServiceDescriptor.find_pid_by_id(t.available_services, &1)
    )
    |> case do
      :ok -> t |> RecoveryAttempt.put_state(:repair_data_distribution)
      {:error, reason} -> t |> RecoveryAttempt.put_state({:stalled, reason})
    end
  end

  #
  #
  def recovery(%{state: :repair_data_distribution} = t) do
    t |> RecoveryAttempt.put_state(:defining_proxies_and_resolvers)
  end

  #
  #
  def recovery(%{state: :defining_proxies_and_resolvers} = t) do
    log_pids =
      t.logs
      |> Enum.map(& &1.log_id)
      |> Enum.map(&ServiceDescriptor.find_pid_by_id(t.available_services, &1))

    with {:ok, resolvers} <-
           define_resolvers(
             t.parameters.desired_resolvers,
             t.version_vector,
             log_pids,
             t.epoch,
             Node.list(),
             t.cluster.otp_name(:sup)
           ),
         {:ok, proxies} <-
           define_commit_proxies(
             t.parameters.desired_commit_proxies,
             t.cluster,
             t.epoch,
             self(),
             Node.list(),
             t.cluster.otp_name(:sup)
           ),
         {:ok, sequencer} <-
           start_sequencer(
             self(),
             t.epoch,
             t.version_vector,
             start_supervised_with(t.cluster.otp_name(:sup))
           ) do
      t
      |> RecoveryAttempt.put_sequencer(sequencer)
      |> RecoveryAttempt.put_resolvers(resolvers)
      |> RecoveryAttempt.put_proxies(proxies)
      |> RecoveryAttempt.put_state(:final_checks)
    else
      {:error, reason} -> t |> RecoveryAttempt.put_state({:stalled, reason})
    end
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

  defp start_supervised_with(supervisor_otp_name) do
    fn child_spec, node ->
      {supervisor_otp_name, node}
      |> DynamicSupervisor.start_child(child_spec)
      |> case do
        {:ok, pid} -> {:ok, pid}
        {:ok, pid, _} -> {:ok, pid}
        {:error, reason} -> {:error, {:failed_to_start_sequencer, reason}}
      end
    end
  end
end
