defmodule Bedrock.ControlPlane.Director.Recovery do
  @moduledoc false

  alias Bedrock.ControlPlane.Director.State
  alias Bedrock.ControlPlane.Config.RecoveryAttempt
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.DataPlane.Storage
  alias Bedrock.Internal.Time.Interval

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
  import __MODULE__.DefiningCommitProxies, only: [define_commit_proxies: 6]
  import __MODULE__.DefiningResolvers, only: [define_resolvers: 6]
  import __MODULE__.DefiningSequencer, only: [define_sequencer: 4]

  import Bedrock.ControlPlane.Director.Recovery.Telemetry

  import Bedrock.ControlPlane.Config.StorageTeamDescriptor, only: [storage_team_descriptor: 3]

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
    |> Map.put(:state, :recovery)
    |> Map.update!(:config, fn config ->
      config
      |> Map.put(:recovery_attempt, %{
        cluster: t.cluster,
        attempt: 1,
        epoch: t.epoch,
        state: :start,
        started_at: now(),
        parameters:
          Map.take(config.parameters, [
            :desired_logs,
            :desired_replication_factor,
            :desired_commit_proxies,
            :desired_resolvers
          ]),
        last_transaction_system_layout: config.transaction_system_layout,
        available_services: t.config.transaction_system_layout.services,
        #
        locked_service_ids: MapSet.new(),
        log_recovery_info_by_id: %{},
        storage_recovery_info_by_id: %{},
        old_log_ids_to_copy: [],
        version_vector: {0, 0},
        durable_version: 0,
        degraded_teams: [],
        logs: %{},
        storage_teams: [],
        resolvers: [],
        proxies: [],
        sequencer: nil
      })
      |> Map.update!(:transaction_system_layout, fn transaction_system_layout ->
        transaction_system_layout
        |> Map.put(:director, self())
        |> Map.put(:sequencer, nil)
        |> Map.put(:rate_keeper, nil)
        |> Map.put(:data_distributor, nil)
        |> Map.put(:proxies, [])
        |> Map.put(:resolvers, [])
      end)
    end)
  end

  def setup_for_subsequent_recovery(t) do
    t
    |> Map.update!(:config, fn config ->
      config
      |> Map.update!(:recovery_attempt, fn recovery_attempt ->
        recovery_attempt
        |> Map.update(:attempt, 0, &(&1 + 1))
        |> Map.put(:state, :start)
        |> Map.put(:available_services, t.config.transaction_system_layout.services)
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
      {:ok, completed} ->
        trace_recovery_completed(Interval.between(completed.started_at, now()))

        t
        |> Map.put(:state, :running)
        |> Map.update!(:config, fn config ->
          config
          |> Map.delete(:recovery_attempt)
          |> Map.update!(:transaction_system_layout, fn transaction_system_layout ->
            transaction_system_layout
            |> Map.put(:id, TransactionSystemLayout.random_id())
            |> Map.put(:sequencer, completed.sequencer)
            |> Map.put(:resolvers, completed.resolvers)
            |> Map.put(:proxies, completed.proxies)
            |> Map.put(:logs, completed.logs)
            |> Map.put(:storage_teams, completed.storage_teams)
          end)
        end)
        |> unlock_storage_after_recovery(completed.durable_version)

      {{:stalled, reason}, stalled} ->
        trace_recovery_stalled(Interval.between(stalled.started_at, now()), reason)

        t
        |> Map.update!(:config, fn config ->
          config
          |> Map.put(:recovery_attempt, stalled)
        end)
    end
  end

  def unlock_storage_after_recovery(t, durable_version) do
    t.config.transaction_system_layout.services
    |> Enum.each(fn
      {id, %{kind: :storage, status: {:up, worker}}} ->
        trace_recovery_storage_unlocking(id)

        Storage.unlock_after_recovery(worker, durable_version, t.config.transaction_system_layout,
          timeout_in_ms: 1_000
        )

      _ ->
        :ok
    end)

    t
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

  @spec recovery(RecoveryAttempt.t()) :: RecoveryAttempt.t()
  def recovery(recovery_attempt)

  #
  #
  def recovery(%{state: :start} = t) do
    t
    |> Map.put(:started_at, now())
    |> Map.put(:state, :lock_available_services)
  end

  #
  #
  def recovery(%{state: {:stalled, _}} = t),
    do: t |> Map.put(:state, :start)

  #
  #
  def recovery(%{state: :lock_available_services} = t) do
    lock_available_services(t.available_services, t.epoch, 200)
    |> case do
      {:error, :newer_epoch_exists = reason} ->
        t |> Map.put(:state, {:stalled, reason})

      {:ok, locked_service_ids, updated_services, log_recovery_info_by_id,
       storage_recovery_info_by_id} ->
        t
        |> Map.update!(:log_recovery_info_by_id, &Map.merge(log_recovery_info_by_id, &1))
        |> Map.update!(:storage_recovery_info_by_id, &Map.merge(storage_recovery_info_by_id, &1))
        |> Map.update!(:available_services, &Map.merge(&1, updated_services))
        |> Map.put(:locked_service_ids, locked_service_ids)
        |> case do
          %{last_transaction_system_layout: %{logs: %{}, storage_teams: []}} = t ->
            t |> Map.put(:state, :first_time_initialization)

          t ->
            t |> Map.put(:state, :determine_old_logs_to_copy)
        end
    end
  end

  # Initialize a new system with empty logs and storage teams by creating
  # placeholders based on desired logs and replication factor, then proceed
  # to fill log vacancies.
  def recovery(%{state: :first_time_initialization} = t) do
    trace_recovery_first_time_initialization()

    log_vacancies =
      1..t.parameters.desired_logs |> Enum.map(&{:vacancy, &1})

    storage_team_vacancies =
      1..t.parameters.desired_replication_factor |> Enum.map(&{:vacancy, &1})

    t
    |> Map.put(:durable_version, 0)
    |> Map.put(:old_log_ids_to_copy, [])
    |> Map.put(:version_vector, {0, 0})
    |> Map.put(:logs, log_vacancies |> Map.new(&{&1, [0, 1]}))
    |> Map.put(:storage_teams, [
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
    |> Map.put(:state, :recruit_logs_to_fill_vacancies)
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
        t |> Map.put(:state, {:stalled, reason})

      {:ok, log_ids, version_vector} ->
        trace_recovery_suitable_logs_chosen(log_ids, version_vector)

        t
        |> Map.put(:old_log_ids_to_copy, log_ids)
        |> Map.put(:version_vector, version_vector)
        |> Map.put(:state, :create_vacancies)
    end
  end

  #
  #
  def recovery(%{state: :create_vacancies} = t) do
    with {:ok, logs, n_log_vacancies} <-
           create_vacancies_for_logs(
             t.last_transaction_system_layout.logs,
             t.parameters.desired_logs
           ),
         {:ok, storage_teams, n_storage_team_vacancies} <-
           create_vacancies_for_storage_teams(
             t.last_transaction_system_layout.storage_teams,
             t.parameters.desired_replication_factor
           ) do
      trace_recovery_creating_vacancies(n_log_vacancies, n_storage_team_vacancies)

      t
      |> Map.put(:logs, logs)
      |> Map.put(:storage_teams, storage_teams)
      |> Map.put(:state, :determine_durable_version)
    end
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
        t |> Map.put(:state, {:stalled, reason})

      {:ok, durable_version, healthy_teams, degraded_teams} ->
        trace_recovery_durable_version_chosen(durable_version)
        trace_recovery_team_health(healthy_teams, degraded_teams)

        t
        |> Map.put(:durable_version, durable_version)
        |> Map.put(:degraded_teams, degraded_teams)
        |> Map.put(:state, :recruit_logs_to_fill_vacancies)
    end
  end

  #
  #
  def recovery(%{state: :recruit_logs_to_fill_vacancies} = t) do
    fill_log_vacancies(
      t.logs,
      t.last_transaction_system_layout.logs |> Map.keys() |> MapSet.new(),
      t.log_recovery_info_by_id |> Map.keys() |> MapSet.new()
    )
    |> case do
      {:error, {:need_log_workers, _} = reason} ->
        t |> Map.put(:state, {:stalled, reason})

      {:ok, logs} ->
        trace_recovery_all_log_vacancies_filled()

        t
        |> Map.put(:logs, logs)
        |> Map.put(:state, :recruit_storage_to_fill_vacancies)
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
        t |> Map.put(:state, {:stalled, reason})

      {:ok, storage_teams} ->
        trace_recovery_all_storage_team_vacancies_filled()

        t
        |> Map.put(:storage_teams, storage_teams)
        |> Map.put(:state, :replay_old_logs)
    end
  end

  #
  #
  def recovery(%{state: :replay_old_logs} = t) do
    new_log_ids = t.logs |> Map.keys()
    trace_recovery_replaying_old_logs(t.old_log_ids_to_copy, new_log_ids, t.version_vector)

    replay_old_logs_into_new_logs(
      t.old_log_ids_to_copy,
      new_log_ids,
      t.version_vector,
      fn log_id ->
        case Map.get(t.available_services, log_id) do
          %{status: {:up, pid}} -> pid
          _ -> nil
        end
      end
    )
    |> case do
      {:error, reason} -> t |> Map.put(:state, {:stalled, reason})
      :ok -> t |> Map.put(:state, :repair_data_distribution)
    end
  end

  #
  #
  def recovery(%{state: :repair_data_distribution} = t) do
    t |> Map.put(:state, :define_sequencer)
  end

  #
  #
  def recovery(%{state: :define_sequencer} = t) do
    sup_otp_name = t.cluster.otp_name(:sup)
    starter_fn = starter_for(sup_otp_name)

    define_sequencer(
      self(),
      t.epoch,
      t.version_vector,
      starter_fn
    )
    |> case do
      {:error, reason} ->
        t |> Map.put(:state, {:stalled, reason})

      {:ok, sequencer} ->
        t
        |> Map.put(:sequencer, sequencer)
        |> Map.put(:state, :define_commit_proxies)
    end
  end

  def recovery(%{state: :define_commit_proxies} = t) do
    sup_otp_name = t.cluster.otp_name(:sup)
    starter_fn = starter_for(sup_otp_name)

    define_commit_proxies(
      t.parameters.desired_commit_proxies,
      t.cluster,
      t.epoch,
      self(),
      Node.list(),
      starter_fn
    )
    |> case do
      {:error, reason} ->
        t |> Map.put(:state, {:stalled, reason})

      {:ok, commit_proxies} ->
        t
        |> Map.put(:proxies, commit_proxies)
        |> Map.put(:state, :define_resolvers)
    end
  end

  def recovery(%{state: :define_resolvers} = t) do
    sup_otp_name = t.cluster.otp_name(:sup)
    starter_fn = starter_for(sup_otp_name)

    log_pids =
      t.available_services
      |> Map.take(t.logs |> Map.keys())
      |> Enum.map(fn
        {_id, %{status: {:up, pid}}} -> pid
        _ -> nil
      end)
      |> Enum.reject(&is_nil/1)

    define_resolvers(
      t.parameters.desired_resolvers,
      t.version_vector,
      log_pids,
      t.epoch,
      Node.list(),
      starter_fn
    )
    |> case do
      {:error, reason} ->
        t |> Map.put(:state, {:stalled, reason})

      {:ok, resolvers} ->
        t
        |> Map.put(:resolvers, resolvers)
        |> Map.put(:state, :final_checks)
    end
  end

  #
  #
  def recovery(%{state: :final_checks} = t) do
    t |> Map.put(:state, :completed)
  end

  #
  #
  def recovery(t), do: raise("Invalid state: #{inspect(t)}")

  defp determine_quorum(n) when is_integer(n), do: 1 + div(n, 2)

  defp starter_for(supervisor_otp_name) do
    fn child_spec, node ->
      {supervisor_otp_name, node}
      |> DynamicSupervisor.start_child(child_spec)
      |> case do
        {:ok, pid} -> {:ok, pid}
        {:ok, pid, _} -> {:ok, pid}
        {:error, reason} -> {:error, reason}
      end
    end
  end
end
