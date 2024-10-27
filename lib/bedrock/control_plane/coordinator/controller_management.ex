defmodule Bedrock.ControlPlane.Coordinator.ControllerManagement do
  alias Bedrock.ControlPlane.ClusterController
  alias Bedrock.ControlPlane.Coordinator.State

  import Bedrock.ControlPlane.Coordinator.State.Changes,
    only: [
      put_controller: 2
    ]

  import Bedrock.ControlPlane.Coordinator.Telemetry,
    only: [
      emit_cluster_controller_changed: 2
    ]

  def timeout_in_ms(:old_controller_stop), do: 100

  @spec start_cluster_controller_if_necessary(State.t()) :: State.t()
  def start_cluster_controller_if_necessary(t)
      when t.leader_node == t.my_node do
    t.supervisor_otp_name
    |> DynamicSupervisor.start_child(
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
        |> put_controller(new_controller)
        |> emit_cluster_controller_changed(new_controller)

      {:error, reason} ->
        raise "Bedrock: failed to start controller: #{inspect(reason)}"
    end
  end

  def start_cluster_controller_if_necessary(t), do: t

  @spec stop_any_cluster_controller_on_this_node!(State.t()) :: State.t()
  def stop_any_cluster_controller_on_this_node!(t) do
    Process.whereis(t.controller_otp_name)
    |> case do
      nil ->
        t

      controller_pid ->
        DynamicSupervisor.terminate_child(t.supervisor_otp_name, controller_pid)

        t
        |> put_controller(:unavailable)
        |> emit_cluster_controller_changed(:unavailable)
    end
  end
end
