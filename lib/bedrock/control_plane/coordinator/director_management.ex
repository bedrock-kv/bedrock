defmodule Bedrock.ControlPlane.Coordinator.DirectorManagement do
  @moduledoc false

  import Bedrock.ControlPlane.Coordinator.State.Changes,
    only: [
      put_director: 2,
      put_config: 2,
      put_leader_startup_state: 2,
      clear_transaction_system_layout: 1,
      convert_to_capability_map: 1
    ]

  import Bedrock.ControlPlane.Coordinator.Telemetry,
    only: [
      trace_director_changed: 1,
      trace_director_failure_detected: 2,
      trace_director_launch: 2,
      trace_director_shutdown: 2,
      trace_recovery_retry_attempt: 1,
      trace_recovery_failed: 1
    ]

  alias Bedrock.ControlPlane.Config
  alias Bedrock.ControlPlane.Coordinator.RecoveryGeneration
  alias Bedrock.ControlPlane.Coordinator.State
  alias Bedrock.ControlPlane.Director
  alias Bedrock.Raft

  require Logger

  @spec try_to_start_director(State.t()) :: State.t()
  def try_to_start_director(t) when t.leader_node == t.my_node and t.director == :unavailable do
    RecoveryGeneration.request(t)
  end

  def try_to_start_director(t), do: t

  @spec launch_reserved(State.t()) :: State.t()
  def launch_reserved(%{bootstrap_reservation: %{generation: generation}} = t) do
    if authoritative_leader?(t) and t.epoch == generation and t.director == :unavailable, do: start_director(t), else: t
  end

  def launch_reserved(t), do: t

  @doc false
  @spec authoritative_leader?(State.t()) :: boolean()
  def authoritative_leader?(%{raft: nil}), do: false

  def authoritative_leader?(t) do
    t.leader_node == t.my_node and Raft.am_i_the_leader?(t.raft) and
      Raft.leadership(t.raft) == {t.my_node, t.raft_term}
  end

  @doc false
  @spec current_director?(State.t(), pid(), Bedrock.epoch()) :: boolean()
  def current_director?(t, director, epoch) do
    is_pid(director) and t.director == director and t.epoch == epoch and
      t.director_raft_term == t.raft_term and authoritative_leader?(t)
  end

  defp start_director(t) do
    t = t |> clear_transaction_system_layout() |> maybe_put_default_config()

    trace_director_launch(t.epoch, t.prior_core_state)

    case start_director_with_monitoring(t) do
      {:ok, new_director} ->
        trace_director_changed(new_director)
        # The recovery source stays with the Coordinator, but Links must wait
        # for the Director to publish the next runnable layout.
        t
        |> clear_transaction_system_layout()
        |> put_director(new_director)

      {:error, reason} ->
        Logger.warning("Failed to start director: #{inspect(reason)}")
        trace_recovery_failed(reason)
        t
    end
  end

  @spec maybe_put_default_config(State.t()) :: State.t()
  defp maybe_put_default_config(%{config: nil} = t), do: put_config(t, Config.new(Raft.known_peers(t.raft)))

  defp maybe_put_default_config(t), do: t

  @spec start_director_with_monitoring(State.t()) ::
          {:ok, pid()} | {:error, term()}
  defp start_director_with_monitoring(t) do
    case DynamicSupervisor.start_child(
           t.supervisor_otp_name,
           {Director,
            [
              cluster: t.cluster,
              config: t.config,
              prior_core_state: t.prior_core_state,
              epoch: t.epoch,
              bootstrap_reservation: t.bootstrap_reservation,
              coordinator: self(),
              services: t.service_directory,
              node_capabilities: convert_to_capability_map(t.node_capabilities)
            ]}
         ) do
      {:ok, director_pid} ->
        Process.monitor(director_pid)
        {:ok, director_pid}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec handle_director_failure(State.t(), director_pid :: pid(), reason :: term()) :: State.t()
  def handle_director_failure(t, director_pid, reason) when t.director == director_pid and t.leader_node == t.my_node do
    trace_director_failure_detected(t.director, reason)
    Logger.warning("Director #{inspect(t.director)} failed with reason: #{inspect(reason)}")

    updated_t = t |> put_director(:unavailable) |> clear_transaction_system_layout()

    # Only attempt restart if we have necessary state (not in tests) and we're in a state that allows recovery
    if t.raft != nil and t.supervisor_otp_name != nil and t.leader_startup_state == :leader_ready do
      trace_recovery_retry_attempt(:director_failure)
      try_to_start_director(updated_t)
    else
      # Mark as recovery failed if we can't retry
      case t.leader_startup_state do
        :leader_ready -> put_leader_startup_state(updated_t, :recovery_failed)
        _ -> updated_t
      end
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

    t |> put_director(:unavailable) |> clear_transaction_system_layout()
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

    t |> put_director(:unavailable) |> clear_transaction_system_layout()
  end

  def cleanup_director_on_leadership_loss(t), do: clear_transaction_system_layout(t)
end
