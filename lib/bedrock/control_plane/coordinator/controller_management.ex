defmodule Bedrock.ControlPlane.Coordinator.DirectorManagement do
  alias Bedrock.ControlPlane.Director
  alias Bedrock.ControlPlane.Coordinator.State

  import Bedrock.ControlPlane.Coordinator.State.Changes,
    only: [put_director: 2, put_epoch: 2, put_director_monitor: 2, put_director_retry_state: 2]

  import Bedrock.ControlPlane.Coordinator.Durability,
    only: [durably_write_config: 3]

  import Bedrock.ControlPlane.Coordinator.Telemetry,
    only: [
      trace_director_changed: 1,
      trace_director_failure_detected: 2,
      trace_director_restart_attempt: 3
    ]

  require Logger

  @spec timeout_in_ms(
          :old_director_stop
          | :director_restart_delay
          | :director_restart_backoff_max
        ) :: pos_integer()
  def timeout_in_ms(:old_director_stop), do: 100
  def timeout_in_ms(:director_restart_delay), do: 1_000
  def timeout_in_ms(:director_restart_backoff_max), do: 30_000

  @spec start_director_if_necessary(State.t()) :: State.t()
  def start_director_if_necessary(t)
      when t.leader_node == t.my_node do
    # Reset retry state when starting fresh director (e.g., after leadership change)
    t = reset_director_retry_state(t)

    new_epoch = 1 + t.config.epoch

    case start_director_with_monitoring(t, new_epoch) do
      {:ok, new_director, monitor_ref} ->
        trace_director_changed(new_director)

        t
        |> put_epoch(new_epoch)
        |> put_director(new_director)
        |> put_director_monitor(monitor_ref)
        |> write_updated_config_with_new_director(new_director, new_epoch)

      {:error, reason} ->
        Logger.error("Bedrock: failed to start director: #{inspect(reason)}")
        # Schedule retry with backoff
        schedule_director_restart(t, reason)
    end
  end

  def start_director_if_necessary(t), do: t

  @spec start_director_with_monitoring(State.t(), non_neg_integer()) ::
          {:ok, pid(), reference()} | {:error, term()}
  defp start_director_with_monitoring(t, epoch) do
    t.supervisor_otp_name
    |> DynamicSupervisor.start_child(
      {Director,
       [
         cluster: t.cluster,
         config: t.config,
         epoch: epoch,
         coordinator: self(),
         relieving: {t.config.epoch, t.director}
       ]}
    )
    |> case do
      {:ok, director_pid} ->
        monitor_ref = Process.monitor(director_pid)
        {:ok, director_pid, monitor_ref}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec stop_any_director_on_this_node!(State.t()) :: State.t()
  def stop_any_director_on_this_node!(t) when t.director == :unavailable, do: t

  def stop_any_director_on_this_node!(t) do
    if node() == node(t.director) do
      # Demonitor before terminating to avoid false failure detection
      if t.director_monitor do
        Process.demonitor(t.director_monitor, [:flush])
      end

      DynamicSupervisor.terminate_child(t.supervisor_otp_name, t.director)
      trace_director_changed(:unavailable)

      t
      |> put_director(:unavailable)
      |> put_director_monitor(nil)
    else
      t
    end
  end

  @spec handle_director_failure(State.t(), reference(), term()) :: State.t()
  def handle_director_failure(t, monitor_ref, reason) do
    # Only handle if this is the current director's monitor
    if t.director_monitor == monitor_ref and t.leader_node == t.my_node do
      trace_director_failure_detected(t.director, reason)
      Logger.warning("Director #{inspect(t.director)} failed with reason: #{inspect(reason)}")

      t
      |> put_director(:unavailable)
      |> put_director_monitor(nil)
      |> schedule_director_restart(reason)
    else
      t
    end
  end

  @spec schedule_director_restart(State.t(), term()) :: State.t()
  def schedule_director_restart(t, failure_reason) do
    retry_state = t.director_retry_state || %{consecutive_failures: 0, last_failure_time: nil}

    # Simple exponential backoff - no circuit breaker complexity
    backoff_delay = calculate_backoff_delay(retry_state.consecutive_failures)

    # Schedule restart
    Process.send_after(self(), :restart_director, backoff_delay)

    trace_director_restart_attempt(
      retry_state.consecutive_failures + 1,
      backoff_delay,
      failure_reason
    )

    t
    |> put_director_retry_state(%{
      consecutive_failures: retry_state.consecutive_failures + 1,
      last_failure_time: System.monotonic_time(:millisecond),
      last_failure_reason: failure_reason
    })
  end

  @spec handle_director_restart_timeout(State.t()) :: State.t()
  def handle_director_restart_timeout(t) do
    if t.leader_node == t.my_node and t.director == :unavailable do
      retry_state = t.director_retry_state || %{consecutive_failures: 0}

      Logger.info(
        "Attempting to restart director (attempt #{retry_state.consecutive_failures + 1})"
      )

      new_epoch = 1 + t.config.epoch

      case start_director_with_monitoring(t, new_epoch) do
        {:ok, new_director, monitor_ref} ->
          trace_director_changed(new_director)
          Logger.info("Director successfully restarted: #{inspect(new_director)}")

          # Reset retry state on successful start
          t
          |> put_epoch(new_epoch)
          |> put_director(new_director)
          |> put_director_monitor(monitor_ref)
          |> reset_director_retry_state()
          |> write_updated_config_with_new_director(new_director, new_epoch)

        {:error, reason} ->
          Logger.warning("Director restart failed: #{inspect(reason)}")
          schedule_director_restart(t, reason)
      end
    else
      t
    end
  end

  @spec reset_director_retry_state(State.t()) :: State.t()
  def reset_director_retry_state(t) do
    put_director_retry_state(t, %{
      consecutive_failures: 0,
      last_failure_time: nil,
      last_failure_reason: nil
    })
  end

  @spec calculate_backoff_delay(non_neg_integer()) :: non_neg_integer()
  def calculate_backoff_delay(failure_count) do
    base_delay = timeout_in_ms(:director_restart_delay)
    max_delay = timeout_in_ms(:director_restart_backoff_max)

    # Exponential backoff: base_delay * 2^failure_count, capped at max_delay
    delay = base_delay * :math.pow(2, failure_count)
    min(trunc(delay), max_delay)
  end

  @spec write_updated_config_with_new_director(State.t(), pid(), non_neg_integer()) :: State.t()
  defp write_updated_config_with_new_director(t, new_director, new_epoch) do
    # Create updated config with new director and epoch
    updated_transaction_system_layout = %{
      t.config.transaction_system_layout
      | director: new_director
    }

    updated_config = %{
      t.config
      | epoch: new_epoch,
        transaction_system_layout: updated_transaction_system_layout
    }

    # Write the updated config to the distributed store
    # Use an empty ack function since we don't need to wait for acknowledgment
    case durably_write_config(t, updated_config, fn -> :ok end) do
      {:ok, new_state} ->
        Logger.info(
          "Successfully wrote updated config with new director #{inspect(new_director)} at epoch #{new_epoch}"
        )

        new_state

      {:error, reason} ->
        Logger.warning("Failed to write updated config with new director: #{inspect(reason)}")
        t
    end
  end
end
