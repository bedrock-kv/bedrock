defmodule Bedrock.ControlPlane.Coordinator.ControllerManagement do
  alias Bedrock.ControlPlane.ClusterController
  alias Bedrock.ControlPlane.Coordinator.State
  alias Bedrock.Raft

  import Bedrock.ControlPlane.Coordinator.State,
    only: [
      update_leader_node: 2,
      update_controller: 2
    ]

  import Bedrock.ControlPlane.Coordinator.Telemetry,
    only: [
      emit_cluster_controller_changed: 1,
      emit_cluster_leadership_changed: 1
    ]

  require Logger

  def timeout_in_ms(:new_controller_start), do: 100
  def timeout_in_ms(:old_controller_stop), do: 100

  @spec start_or_find_a_new_controller!(
          State.t(),
          new_leader :: node() | :undecided,
          Raft.election_term()
        ) ::
          State.t()
  def start_or_find_a_new_controller!(%{my_node: my_node} = t, new_leader, election_term) do
    new_controller =
      case new_leader do
        ^my_node -> start_new_controller!(t, election_term)
        :undecided -> :unavailable
        _other_node -> find_controller_on_node!(t, new_leader)
      end

    t
    |> update_leader_node_if_necessary(new_leader)
    |> update_controller_if_necessary(new_controller)
  end

  @spec update_leader_node_if_necessary(State.t(), node() | :undecided) :: State.t()
  def update_leader_node_if_necessary(t, new_leader) when t.leader_node != new_leader do
    t
    |> update_leader_node(new_leader)
    |> emit_cluster_leadership_changed()
  end

  def update_leader_node_if_necessary(t, _new_leader), do: t

  @spec update_controller_if_necessary(State.t(), ClusterController.ref() | :unavailable) ::
          State.t()
  def update_controller_if_necessary(t, new_controller) when t.controller != new_controller do
    t
    |> update_controller(new_controller)
    |> emit_cluster_controller_changed()
  end

  def update_controller_if_necessary(t, _new_controller), do: t

  @spec start_new_controller!(State.t(), Raft.election_term()) :: ClusterController.ref()
  def start_new_controller!(t, election_term) do
    DynamicSupervisor.start_child(
      t.supervisor_otp_name,
      {ClusterController,
       [
         cluster: t.cluster,
         config: t.config,
         epoch: election_term,
         coordinator: self(),
         otp_name: t.controller_otp_name
       ]}
    )
    |> case do
      {:ok, controller} ->
        controller

      {:error, reason} ->
        raise "Bedrock: failed to start controller: #{inspect(reason)}"
    end
  end

  @spec find_controller_on_node!(State.t(), node()) :: ClusterController.ref() | :unavailable
  def find_controller_on_node!(t, node) do
    :rpc.call(
      node,
      Process,
      :whereis,
      [t.controller_otp_name],
      timeout_in_ms(:new_controller_start)
    ) ||
      :unavailable
  end

  @spec stop_any_running_controller_on_this_node!(State.t()) :: State.t()
  def stop_any_running_controller_on_this_node!(%{controller: controller} = t) do
    with true <- is_pid(controller),
         true <- t.my_node == node(controller),
         timeout_in_ms <- timeout_in_ms(:old_controller_stop) do
      try do
        ref = Process.monitor(controller)
        :ok = GenServer.stop(controller, :shutdown, timeout_in_ms)

        receive do
          {:DOWN, ^ref, :process, _pid, _reason} -> :ok
        after
          timeout_in_ms ->
            raise Logger.error("Bedrock: failed to stop controller (#{inspect(t.controller)})")
        end
      catch
        :exit, {:noproc, _} -> :ok
      end
    end

    t
  end
end
