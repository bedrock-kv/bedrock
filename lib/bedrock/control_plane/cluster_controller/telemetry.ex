defmodule Bedrock.ControlPlane.ClusterController.Telemetry do
  alias Bedrock.ControlPlane.ClusterController.State
  alias Bedrock.Telemetry

  @doc """
  Emits a telemetry event indicating that the cluster controller has started
  the recovery process.
  """
  @spec trace_recovery_attempt_started(t :: State.t()) :: :ok
  def trace_recovery_attempt_started(t) do
    Telemetry.execute([:bedrock, :cluster, :recovery, :started], %{}, %{
      cluster: t.cluster,
      epoch: t.epoch,
      attempt: t.config.recovery_attempt.attempt,
      at: t.config.recovery_attempt.started_at
    })
  end

  @spec trace_recovery_services_locked(
          State.t(),
          n_services :: non_neg_integer(),
          n_reporting :: non_neg_integer()
        ) :: :ok
  def trace_recovery_services_locked(t, n_services, n_reporting) do
    Telemetry.execute([:bedrock, :cluster, :recovery, :services_locked], %{}, %{
      cluster: t.cluster,
      epoch: t.epoch,
      n_services: n_services,
      n_reporting: n_reporting
    })
  end

  @spec trace_recovery_durable_version_chosen(
          t :: State.t(),
          durable_version :: Bedrock.version(),
          degraded_teams :: [Bedrock.range_tag()]
        ) :: :ok
  def trace_recovery_durable_version_chosen(t, durable_version, degraded_teams) do
    Telemetry.execute([:bedrock, :cluster, :recovery, :durable_version_chosen], %{}, %{
      cluster: t.cluster,
      epoch: t.epoch,
      durable_version: durable_version,
      degraded_teams: degraded_teams
    })
  end

  @spec trace_recovery_suitable_logs_chosen(
          t :: State.t(),
          suitable_logs :: [Bedrock.service_id()],
          log_version_vector :: Bedrock.version_vector()
        ) :: :ok
  def trace_recovery_suitable_logs_chosen(t, suitable_logs, log_version_vector) do
    Telemetry.execute([:bedrock, :cluster, :recovery, :suitable_logs_chosen], %{}, %{
      cluster: t.cluster,
      epoch: t.epoch,
      suitable_logs: suitable_logs,
      log_version_vector: log_version_vector
    })
  end
end
