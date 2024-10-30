defmodule Bedrock.ControlPlane.Coordinator.ControllerManagement do
  alias Bedrock.ControlPlane.ClusterController
  alias Bedrock.ControlPlane.Coordinator.State

  import Bedrock.ControlPlane.Coordinator.State.Changes,
    only: [put_controller: 2]

  import Bedrock.ControlPlane.Coordinator.Telemetry,
    only: [emit_cluster_controller_changed: 2]

  def timeout_in_ms(:old_controller_stop), do: 100

  @spec start_cluster_controller_if_necessary(State.t()) :: State.t()
  def start_cluster_controller_if_necessary(t)
      when t.leader_node == t.my_node do
    new_epoch = t.config.epoch + 1

    t.supervisor_otp_name
    |> DynamicSupervisor.start_child(
      {ClusterController,
       [
         cluster: t.cluster,
         config: t.config,
         epoch: new_epoch,
         coordinator: self(),
         relieving: {t.config.epoch, t.controller}
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
  def stop_any_cluster_controller_on_this_node!(t) when t.controller == :unavailable, do: t

  def stop_any_cluster_controller_on_this_node!(t) do
    if node() == node(t.controller) do
      DynamicSupervisor.terminate_child(t.supervisor_otp_name, t.controller)

      t
      |> put_controller(:unavailable)
      |> emit_cluster_controller_changed(:unavailable)
    end
  end
end
