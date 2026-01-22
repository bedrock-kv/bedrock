defmodule Bedrock.ControlPlane.Coordinator.Telemetry do
  @moduledoc false
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
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

  @spec trace_director_launch(
          epoch :: non_neg_integer(),
          old_transaction_system_layout :: TransactionSystemLayout.t() | nil
        ) :: :ok
  def trace_director_launch(epoch, old_transaction_system_layout) do
    config_summary =
      if old_transaction_system_layout do
        %{
          epoch: old_transaction_system_layout.epoch,
          has_transaction_system_layout: not is_nil(old_transaction_system_layout),
          logs_count: map_size(old_transaction_system_layout[:logs] || %{})
        }
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

  @spec trace_epoch_ended(previous_epoch :: Bedrock.epoch() | nil) :: :ok
  def trace_epoch_ended(previous_epoch) do
    Telemetry.execute([:bedrock, :control_plane, :coordinator, :epoch_ended], %{}, %{
      previous_epoch: previous_epoch
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

  @spec trace_director_shutdown(director :: pid(), reason :: atom()) :: :ok
  def trace_director_shutdown(director, reason) do
    Telemetry.execute(
      [:bedrock, :control_plane, :coordinator, :director_shutdown],
      %{},
      %{
        director: director,
        reason: reason
      }
    )
  end

  @spec trace_leader_waiting_for_consensus() :: :ok
  def trace_leader_waiting_for_consensus do
    Telemetry.execute(
      [:bedrock, :control_plane, :coordinator, :leader_waiting_consensus],
      %{},
      %{}
    )
  end

  @spec trace_leader_ready_starting_director(service_count :: non_neg_integer()) :: :ok
  def trace_leader_ready_starting_director(service_count) do
    Telemetry.execute(
      [:bedrock, :control_plane, :coordinator, :leader_ready],
      %{service_count: service_count},
      %{}
    )
  end

  @spec trace_recovery_capability_change_detected() :: :ok
  def trace_recovery_capability_change_detected do
    Telemetry.execute(
      [:bedrock, :control_plane, :coordinator, :recovery_capability_change],
      %{},
      %{}
    )
  end

  @spec trace_recovery_retry_attempt(reason :: :capability_change | :leadership_change | :director_failure) :: :ok
  def trace_recovery_retry_attempt(reason) do
    Telemetry.execute(
      [:bedrock, :control_plane, :coordinator, :recovery_retry_attempt],
      %{},
      %{reason: reason}
    )
  end

  @spec trace_recovery_failed(reason :: term()) :: :ok
  def trace_recovery_failed(reason) do
    Telemetry.execute(
      [:bedrock, :control_plane, :coordinator, :recovery_failed],
      %{},
      %{reason: reason}
    )
  end
end
