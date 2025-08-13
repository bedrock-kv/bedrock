defmodule Bedrock.ControlPlane.Director.Recovery.Telemetry do
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.Internal.Time.Interval
  alias Bedrock.Telemetry

  @doc """
  Emits a telemetry event indicating that the cluster director has started
  the recovery process.
  """
  @spec trace_recovery_attempt_started(
          cluster :: module(),
          epoch :: Bedrock.epoch(),
          attempt :: pos_integer(),
          at :: DateTime.t()
        ) :: :ok
  def trace_recovery_attempt_started(cluster, epoch, attempt, at) do
    Telemetry.execute([:bedrock, :recovery, :started], %{}, %{
      cluster: cluster,
      epoch: epoch,
      attempt: attempt,
      at: at
    })
  end

  @spec trace_recovery_stalled(elapsed :: Interval.t(), reason :: term()) :: :ok
  def trace_recovery_stalled(elapsed, reason) do
    Telemetry.execute([:bedrock, :recovery, :stalled], %{}, %{elapsed: elapsed, reason: reason})
  end

  @spec trace_recovery_completed(elapsed :: Interval.t()) :: :ok
  def trace_recovery_completed(elapsed),
    do: Telemetry.execute([:bedrock, :recovery, :completed], %{}, %{elapsed: elapsed})

  @spec trace_recovery_failed(elapsed :: Interval.t(), reason :: term()) :: :ok
  def trace_recovery_failed(elapsed, reason) do
    Telemetry.execute([:bedrock, :recovery, :failed], %{}, %{elapsed: elapsed, reason: reason})
  end

  @spec trace_recovery_services_locked(
          n_services :: non_neg_integer(),
          n_reporting :: non_neg_integer()
        ) :: :ok
  def trace_recovery_services_locked(n_services, n_reporting) do
    Telemetry.execute([:bedrock, :recovery, :services_locked], %{}, %{
      n_services: n_services,
      n_reporting: n_reporting
    })
  end

  @spec trace_recovery_first_time_initialization() :: :ok
  def trace_recovery_first_time_initialization,
    do: Telemetry.execute([:bedrock, :recovery, :first_time_initialization], %{}, %{})

  @spec trace_recovery_creating_vacancies(non_neg_integer(), non_neg_integer()) :: :ok
  def trace_recovery_creating_vacancies(n_log_vacancies, n_storage_team_vacancies) do
    Telemetry.execute(
      [:bedrock, :recovery, :creating_vacancies],
      %{
        n_log_vacancies: n_log_vacancies,
        n_storage_team_vacancies: n_storage_team_vacancies
      },
      %{}
    )
  end

  @spec trace_recovery_durable_version_chosen(durable_version :: Bedrock.version()) :: :ok
  def trace_recovery_durable_version_chosen(durable_version) do
    Telemetry.execute([:bedrock, :recovery, :durable_version_chosen], %{}, %{
      durable_version: durable_version
    })
  end

  @spec trace_recovery_team_health(
          healthy_teams :: [Bedrock.range_tag()],
          degraded_teams :: [Bedrock.range_tag()]
        ) :: :ok
  def trace_recovery_team_health(healthy_teams, degraded_teams) do
    Telemetry.execute([:bedrock, :recovery, :team_health], %{}, %{
      healthy_teams: healthy_teams,
      degraded_teams: degraded_teams
    })
  end

  @spec trace_recovery_suitable_logs_chosen(
          suitable_logs :: [term()],
          log_version_vector :: Bedrock.version_vector()
        ) :: :ok
  def trace_recovery_suitable_logs_chosen(suitable_logs, log_version_vector) do
    Telemetry.execute([:bedrock, :recovery, :suitable_logs_chosen], %{}, %{
      suitable_logs: suitable_logs,
      log_version_vector: log_version_vector
    })
  end

  @spec trace_recovery_all_log_vacancies_filled() :: :ok
  def trace_recovery_all_log_vacancies_filled,
    do: Telemetry.execute([:bedrock, :recovery, :all_log_vacancies_filled], %{}, %{})

  @spec trace_recovery_all_storage_team_vacancies_filled() :: :ok
  def trace_recovery_all_storage_team_vacancies_filled,
    do: Telemetry.execute([:bedrock, :recovery, :all_storage_team_vacancies_filled], %{}, %{})

  @spec trace_recovery_replaying_old_logs([String.t()], [String.t()], Bedrock.version_vector()) ::
          :ok
  def trace_recovery_replaying_old_logs(old_log_ids, new_log_ids, version_vector) do
    Telemetry.execute([:bedrock, :recovery, :replaying_old_logs], %{}, %{
      old_log_ids: old_log_ids,
      new_log_ids: new_log_ids,
      version_vector: version_vector
    })
  end

  @spec trace_recovery_storage_unlocking(term()) :: :ok
  def trace_recovery_storage_unlocking(storage_worker_id) do
    Telemetry.execute([:bedrock, :recovery, :storage_unlocking], %{}, %{
      storage_worker_id: storage_worker_id
    })
  end

  @spec trace_recovery_persisting_system_state() :: :ok
  def trace_recovery_persisting_system_state do
    Telemetry.execute([:bedrock, :recovery, :persisting_system_state], %{}, %{})
  end

  @spec trace_recovery_system_state_persisted() :: :ok
  def trace_recovery_system_state_persisted do
    Telemetry.execute([:bedrock, :recovery, :system_state_persisted], %{}, %{})
  end

  @spec trace_recovery_system_transaction_failed(term()) :: :ok
  def trace_recovery_system_transaction_failed(reason) do
    Telemetry.execute([:bedrock, :recovery, :system_transaction_failed], %{}, %{
      reason: reason
    })
  end

  @spec trace_recovery_monitoring_components() :: :ok
  def trace_recovery_monitoring_components do
    Telemetry.execute([:bedrock, :recovery, :monitoring_components], %{}, %{})
  end

  @spec trace_recovery_old_logs_replayed() :: :ok
  def trace_recovery_old_logs_replayed do
    Telemetry.execute([:bedrock, :recovery, :old_logs_replayed], %{}, %{})
  end

  @spec trace_recovery_log_recruitment_completed(
          [any()],
          %{any() => any()},
          %{any() => any()},
          %{any() => any()}
        ) :: :ok
  def trace_recovery_log_recruitment_completed(
        log_ids,
        service_pids,
        available_services,
        updated_services
      ) do
    Telemetry.execute([:bedrock, :recovery, :log_recruitment_completed], %{}, %{
      log_ids: log_ids,
      service_pids: service_pids,
      available_services: available_services,
      updated_services: updated_services
    })
  end

  @spec trace_recovery_log_validation_started([any()], %{any() => any()}) :: :ok
  def trace_recovery_log_validation_started(log_ids, available_services) do
    Telemetry.execute([:bedrock, :recovery, :log_validation_started], %{}, %{
      log_ids: log_ids,
      available_services: available_services
    })
  end

  @spec trace_recovery_log_service_status(any(), :found | :missing, any()) :: :ok
  def trace_recovery_log_service_status(log_id, status, service) do
    Telemetry.execute([:bedrock, :recovery, :log_service_status], %{}, %{
      log_id: log_id,
      status: status,
      service: service
    })
  end

  @spec trace_recovery_attempt_persisted(any()) :: :ok
  def trace_recovery_attempt_persisted(txn_id) do
    Telemetry.execute([:bedrock, :recovery, :attempt_persisted], %{}, %{
      txn_id: txn_id
    })
  end

  @spec trace_recovery_attempt_persist_failed(term()) :: :ok
  def trace_recovery_attempt_persist_failed(reason) do
    Telemetry.execute([:bedrock, :recovery, :attempt_persist_failed], %{}, %{
      reason: reason
    })
  end

  @spec trace_recovery_layout_persisted(any()) :: :ok
  def trace_recovery_layout_persisted(txn_id) do
    Telemetry.execute([:bedrock, :recovery, :layout_persisted], %{}, %{
      txn_id: txn_id
    })
  end

  @spec trace_recovery_layout_persist_failed(term()) :: :ok
  def trace_recovery_layout_persist_failed(reason) do
    Telemetry.execute([:bedrock, :recovery, :layout_persist_failed], %{}, %{
      reason: reason
    })
  end

  @spec trace_recovery_unexpected_state(atom(), any()) :: :ok
  def trace_recovery_unexpected_state(unexpected_state, full_state) do
    Telemetry.execute([:bedrock, :recovery, :unexpected_state], %{}, %{
      unexpected_state: unexpected_state,
      full_state: full_state
    })
  end

  @spec trace_recovery_tsl_validation_success() :: :ok
  def trace_recovery_tsl_validation_success do
    Telemetry.execute([:bedrock, :recovery, :tsl_validation_success], %{}, %{})
  end

  @spec trace_recovery_tsl_validation_failed(
          TransactionSystemLayout.t(),
          validation_error :: term()
        ) :: :ok
  def trace_recovery_tsl_validation_failed(transaction_system_layout, validation_error) do
    Telemetry.execute([:bedrock, :recovery, :tsl_validation_failed], %{}, %{
      transaction_system_layout: transaction_system_layout,
      validation_error: validation_error
    })
  end
end
