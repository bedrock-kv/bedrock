defmodule Bedrock.ControlPlane.Coordinator.ControllerManagement do
  alias Bedrock.ControlPlane.ClusterController
  alias Bedrock.ControlPlane.Coordinator.State

  import Bedrock.ControlPlane.Coordinator.State,
    only: [
      update_controller: 2,
      update_controller_in_config: 2
    ]

  import Bedrock.ControlPlane.Coordinator.Telemetry,
    only: [
      emit_cluster_controller_changed: 2
    ]

  def timeout_in_ms(:old_controller_stop), do: 100

  @spec start_cluster_controller_if_necessary(State.t()) :: State.t()
  def start_cluster_controller_if_necessary(t)
      when t.leader_node == t.my_node do
    DynamicSupervisor.start_child(
      t.supervisor_otp_name,
      {ClusterController,
       [
         cluster: t.cluster,
         config: t.config,
         epoch: t.config.epoch + 1,
         coordinator: self(),
         otp_name: t.controller_otp_name
       ]}
    )
    |> case do
      {:ok, new_controller} ->
        t
        |> update_controller(new_controller)
        |> update_controller_in_config(new_controller)
        |> emit_cluster_controller_changed(new_controller)

      {:error, reason} ->
        raise "Bedrock: failed to start controller: #{inspect(reason)}"
    end
  end

  def start_cluster_controller_if_necessary(t), do: t

  @spec stop_any_cluster_controller_on_this_node!(State.t()) :: State.t()
  def stop_any_cluster_controller_on_this_node!(t) do
    Process.whereis(t.controller_otp_name)
    |> stop_process_with_pid(timeout_in_ms(:old_controller_stop))
    |> case do
      :ok -> t |> update_controller(:unavailable)
      :timeout -> raise "Bedrock: failed to stop controller (#{inspect(t.controller)})"
    end
  end

  @spec stop_process_with_pid(pid() | nil, timeout()) :: :ok | :timeout
  def stop_process_with_pid(nil, _timeout_in_ms), do: :ok

  def stop_process_with_pid(pid, timeout_in_ms) do
    try do
      ref = Process.monitor(pid)
      :ok = GenServer.stop(pid, :shutdown, timeout_in_ms)

      receive do
        {:DOWN, ^ref, :process, _pid, _reason} -> :ok
      after
        timeout_in_ms -> :timeout
      end
    catch
      :exit, {:noproc, _} -> :ok
    end
  end
end
