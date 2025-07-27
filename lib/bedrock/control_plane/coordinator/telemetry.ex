defmodule Bedrock.ControlPlane.Coordinator.Telemetry do
  alias Bedrock.Telemetry

  @spec trace_started(cluster :: module(), otp_name :: atom()) :: :ok
  def trace_started(cluster, otp_name) do
    Telemetry.execute([:bedrock, :control_plane, :coordinator, :started], %{}, %{
      cluster: cluster,
      otp_name: otp_name
    })
  end

  @spec trace_election_completed(new_leader :: node()) :: :ok
  def trace_election_completed(new_leader) do
    Telemetry.execute([:bedrock, :control_plane, :coordinator, :election_completed], %{}, %{
      new_leader: new_leader
    })
  end

  @spec trace_director_changed(director :: pid() | :unavailable) :: :ok
  def trace_director_changed(director) do
    Telemetry.execute([:bedrock, :control_plane, :coordinator, :director_changed], %{}, %{
      director: director
    })
  end

  @spec trace_director_launch(epoch :: non_neg_integer(), config :: map() | nil) :: :ok
  def trace_director_launch(epoch, config) do
    config_summary =
      if config do
        %{
          epoch: config.epoch,
          has_transaction_system_layout: not is_nil(config.transaction_system_layout),
          logs_count: map_size(config.transaction_system_layout.logs || %{}),
          storage_teams_count: length(config.transaction_system_layout.storage_teams || [])
        }
      else
        nil
      end

    Telemetry.execute([:bedrock, :control_plane, :coordinator, :director_launch], %{}, %{
      epoch: epoch,
      config_summary: config_summary
    })
  end

  @spec trace_consensus_reached(transaction_id :: binary()) :: :ok
  def trace_consensus_reached(transaction_id) do
    Telemetry.execute([:bedrock, :control_plane, :coordinator, :consensus_reached], %{}, %{
      transaction_id: transaction_id
    })
  end

  @spec trace_director_failure_detected(director :: pid() | :unavailable, reason :: term()) :: :ok
  def trace_director_failure_detected(director, reason) do
    Telemetry.execute(
      [:bedrock, :control_plane, :coordinator, :director_failure_detected],
      %{},
      %{
        director: director,
        reason: reason
      }
    )
  end

  @spec trace_director_restart_attempt(
          attempt :: non_neg_integer(),
          backoff_delay :: non_neg_integer(),
          reason :: term()
        ) :: :ok
  def trace_director_restart_attempt(attempt, backoff_delay, reason) do
    Telemetry.execute([:bedrock, :control_plane, :coordinator, :director_restart_attempt], %{}, %{
      attempt: attempt,
      backoff_delay: backoff_delay,
      reason: reason
    })
  end
end
