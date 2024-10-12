defmodule Bedrock.ControlPlane.Coordinator.ControllerManagement do
  alias Bedrock.ControlPlane.ClusterController
  alias Bedrock.ControlPlane.Coordinator.State
  alias Bedrock.Raft

  import Bedrock.ControlPlane.Coordinator.State,
    only: [
      update_am_i_the_leader: 2,
      update_controller: 2
    ]

  require Logger

  def timeout_in_ms(:new_controller_start), do: 100
  def timeout_in_ms(:old_controller_stop), do: 100

  @spec start_or_find_a_new_controller!(State.t(), new_leader :: node(), Raft.election_term()) ::
          State.t()
  def start_or_find_a_new_controller!(%{my_node: my_node} = t, new_leader, election_term) do
    {am_i_the_leader, controller} =
      case new_leader do
        ^my_node ->
          with {:ok, controller} <-
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
                 ) do
            {true, controller}
          else
            {:error, reason} -> raise "Bedrock: failed to start controller: #{inspect(reason)}"
          end

        :undecided ->
          {false, :unavailable}

        other_node ->
          {false,
           :rpc.call(
             other_node,
             Process,
             :whereis,
             [t.controller_otp_name],
             timeout_in_ms(:new_controller_start)
           ) ||
             :unavailable}
      end

    t
    |> update_controller(controller)
    |> update_am_i_the_leader(am_i_the_leader)
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
