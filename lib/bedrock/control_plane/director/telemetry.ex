defmodule Bedrock.ControlPlane.Director.Telemetry do
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
