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
    Telemetry.execute([:bedrock, :recovery, :creating_vacancies], %{}, %{
      n_log_vacancies: n_log_vacancies,
      n_storage_team_vacancies: n_storage_team_vacancies
    })
  end

  @spec trace_recovery_durable_version_chosen(
          durable_version :: Bedrock.version(),
          degraded_teams :: [Bedrock.range_tag()]
        ) :: :ok
  def trace_recovery_durable_version_chosen(durable_version, degraded_teams) do
    Telemetry.execute([:bedrock, :recovery, :durable_version_chosen], %{}, %{
      durable_version: durable_version,
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

  def trace_recovery_storage_unlocking(storage_worker_id) do
    Telemetry.execute([:bedrock, :recovery, :storage_unlocking], %{}, %{
      storage_worker_id: storage_worker_id
    })
  end
end
