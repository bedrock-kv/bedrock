defmodule Bedrock.ControlPlane.Director.Recovery.Telemetry do
  alias Bedrock.Telemetry
  alias Bedrock.Internal.Time.Interval

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

  @spec trace_recovery_stalled(elapsed :: Interval.t(), reason :: any()) :: :ok
  def trace_recovery_stalled(elapsed, reason) do
    Telemetry.execute([:bedrock, :recovery, :stalled], %{}, %{elapsed: elapsed, reason: reason})
  end

  @spec trace_recovery_completed(elapsed :: Interval.t()) :: :ok
  def trace_recovery_completed(elapsed),
    do: Telemetry.execute([:bedrock, :recovery, :completed], %{}, %{elapsed: elapsed})

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

  def trace_recovery_first_time_initialization,
    do: Telemetry.execute([:bedrock, :recovery, :first_time_initialization], %{}, %{})

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
          suitable_logs :: [Bedrock.service_id()],
          log_version_vector :: Bedrock.version_vector()
        ) :: :ok
  def trace_recovery_suitable_logs_chosen(suitable_logs, log_version_vector) do
    Telemetry.execute([:bedrock, :recovery, :suitable_logs_chosen], %{}, %{
      suitable_logs: suitable_logs,
      log_version_vector: log_version_vector
    })
  end

  def trace_recovery_all_log_vacancies_filled,
    do: Telemetry.execute([:bedrock, :recovery, :all_log_vacancies_filled], %{}, %{})

  def trace_recovery_all_storage_team_vacancies_filled,
    do: Telemetry.execute([:bedrock, :recovery, :all_storage_team_vacancies_filled], %{}, %{})

  def trace_recovery_replaying_old_logs(old_log_ids, new_log_ids, version_vector) do
    Telemetry.execute([:bedrock, :recovery, :replaying_old_logs], %{}, %{
      old_log_ids: old_log_ids,
      new_log_ids: new_log_ids,
      version_vector: version_vector
    })
  end

  def trace_recovery_storage_unlocking(storage_worker_id) do
    Telemetry.execute([:bedrock, :recovery, :storage_unlocking], %{}, %{
      storage_worker_id: storage_worker_id
    })
  end

  def trace_recovery_persisting_system_state do
    Telemetry.execute([:bedrock, :recovery, :persisting_system_state], %{}, %{})
  end

  def trace_recovery_system_state_persisted do
    Telemetry.execute([:bedrock, :recovery, :system_state_persisted], %{}, %{})
  end

  def trace_recovery_system_transaction_failed(reason) do
    Telemetry.execute([:bedrock, :recovery, :system_transaction_failed], %{}, %{
      reason: reason
    })
  end

  def trace_recovery_monitoring_components do
    Telemetry.execute([:bedrock, :recovery, :monitoring_components], %{}, %{})
  end

  def trace_recovery_old_logs_replayed do
    Telemetry.execute([:bedrock, :recovery, :old_logs_replayed], %{}, %{})
  end

  def trace_recovery_worker_cleanup(worker_id, node) do
    Telemetry.execute([:bedrock, :recovery, :worker_cleanup], %{}, %{
      worker_id: worker_id,
      node: node
    })
  end

  def trace_recovery_cleanup_started(total_obsolete_workers, nodes) do
    Telemetry.execute([:bedrock, :recovery, :cleanup_started], %{}, %{
      total_obsolete_workers: total_obsolete_workers,
      affected_nodes: nodes
    })
  end

  def trace_recovery_cleanup_completed(
        total_obsolete_workers,
        successful_cleanups,
        failed_cleanups
      ) do
    Telemetry.execute([:bedrock, :recovery, :cleanup_completed], %{}, %{
      total_obsolete_workers: total_obsolete_workers,
      successful_cleanups: successful_cleanups,
      failed_cleanups: failed_cleanups
    })
  end

  def trace_recovery_node_cleanup_started(node, worker_count) do
    Telemetry.execute([:bedrock, :recovery, :node_cleanup_started], %{}, %{
      node: node,
      worker_count: worker_count
    })
  end

  def trace_recovery_node_cleanup_completed(node, results) do
    successful = Enum.count(results, fn {_, result} -> result == :ok end)
    failed = Enum.count(results, fn {_, result} -> match?({:error, _}, result) end)

    Telemetry.execute([:bedrock, :recovery, :node_cleanup_completed], %{}, %{
      node: node,
      successful_cleanups: successful,
      failed_cleanups: failed,
      results: results
    })
  end
end
