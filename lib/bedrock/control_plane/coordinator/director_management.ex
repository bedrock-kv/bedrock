defmodule Bedrock.ControlPlane.Coordinator.DirectorManagement do
  @moduledoc false

  import Bedrock.ControlPlane.Coordinator.State.Changes,
    only: [
      put_director: 2,
      put_config: 2,
      convert_to_capability_map: 1
    ]

  import Bedrock.ControlPlane.Coordinator.Telemetry,
    only: [
      trace_director_changed: 1,
      trace_director_failure_detected: 2,
      trace_director_launch: 2,
      trace_director_shutdown: 2
    ]

  alias Bedrock.ControlPlane.Config
  alias Bedrock.ControlPlane.Coordinator.State
  alias Bedrock.ControlPlane.Director

  require Logger

  @spec try_to_start_director(State.t()) :: State.t()
  def try_to_start_director(t) when t.leader_node == t.my_node and t.director == :unavailable do
    t = maybe_put_default_config(t)

    trace_director_launch(t.epoch, t.transaction_system_layout)

    {:ok, new_director} = start_director_with_monitoring!(t)

    trace_director_changed(new_director)

    put_director(t, new_director)
  end

  def try_to_start_director(t), do: t

  @spec maybe_put_default_config(State.t()) :: State.t()
  defp maybe_put_default_config(%{config: nil} = t), do: put_config(t, Config.new(Bedrock.Raft.known_peers(t.raft)))

  defp maybe_put_default_config(t), do: t

  @spec start_director_with_monitoring!(State.t()) ::
          {:ok, pid()} | no_return()
  defp start_director_with_monitoring!(t) do
    t.supervisor_otp_name
    |> DynamicSupervisor.start_child(
      {Director,
       [
         cluster: t.cluster,
         config: t.config,
         old_transaction_system_layout: t.transaction_system_layout,
         epoch: t.epoch,
         coordinator: self(),
         services: t.service_directory,
         node_capabilities: convert_to_capability_map(t.node_capabilities)
       ]}
    )
    |> case do
      {:ok, director_pid} ->
        Process.monitor(director_pid)
        {:ok, director_pid}

      {:error, reason} ->
        raise "Failed to start director: #{inspect(reason)}"
    end
  end

  @spec handle_director_failure(State.t(), director_pid :: pid(), reason :: term()) :: State.t()
  def handle_director_failure(t, director_pid, reason) when t.director == director_pid and t.leader_node == t.my_node do
    trace_director_failure_detected(t.director, reason)
    Logger.warning("Director #{inspect(t.director)} failed with reason: #{inspect(reason)}")

    updated_t = put_director(t, :unavailable)

    # Only attempt restart if we have necessary state (not in tests)
    if t.raft != nil and t.supervisor_otp_name != nil do
      try_to_start_director(updated_t)
    else
      updated_t
    end
  end

  def handle_director_failure(t, _director_pid, _reason) do
    # If the director is not the current one or we're not the leader, we ignore the failure
    t
  end

  @doc """
  Gracefully shut down the current director if we are the leader and a director is running.
  This is typically called when ending an epoch via consensus.
  """
  @spec shutdown_director_if_running(State.t()) :: State.t()
  def shutdown_director_if_running(t) when t.leader_node == t.my_node and is_pid(t.director) do
    trace_director_shutdown(t.director, :epoch_end)

    # Terminate the director process via the supervisor
    case DynamicSupervisor.terminate_child(t.supervisor_otp_name, t.director) do
      :ok ->
        trace_director_changed(:unavailable)

      {:error, :not_found} ->
        # Director was already gone, that's fine
        :ok
    end

    put_director(t, :unavailable)
  end

  def shutdown_director_if_running(t), do: t

  @doc """
  Clean up director references when losing leadership, regardless of current leader status.
  This handles cases where we have stale director references after leadership transitions.
  """
  @spec cleanup_director_on_leadership_loss(State.t()) :: State.t()
  def cleanup_director_on_leadership_loss(t) when is_pid(t.director) do
    trace_director_shutdown(t.director, :leadership_loss)

    case DynamicSupervisor.terminate_child(t.supervisor_otp_name, t.director) do
      :ok -> trace_director_changed(:unavailable)
      {:error, _reason} -> :ok
    end

    put_director(t, :unavailable)
  end

  def cleanup_director_on_leadership_loss(t), do: t
end
