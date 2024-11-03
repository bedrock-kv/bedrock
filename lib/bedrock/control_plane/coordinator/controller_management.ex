defmodule Bedrock.ControlPlane.Coordinator.DirectorManagement do
  alias Bedrock.ControlPlane.Director
  alias Bedrock.ControlPlane.Coordinator.State

  import Bedrock.ControlPlane.Coordinator.State.Changes,
    only: [put_director: 2, put_epoch: 2]

  import Bedrock.ControlPlane.Coordinator.Telemetry,
    only: [emit_director_changed: 2]

  def timeout_in_ms(:old_director_stop), do: 100

  @spec start_director_if_necessary(State.t()) :: State.t()
  def start_director_if_necessary(t)
      when t.leader_node == t.my_node do
    new_epoch = 1 + t.config.epoch

    t.supervisor_otp_name
    |> DynamicSupervisor.start_child(
      {Director,
       [
         cluster: t.cluster,
         config: t.config,
         epoch: new_epoch,
         coordinator: self(),
         relieving: {t.config.epoch, t.director}
       ]}
    )
    |> case do
      {:ok, new_director} ->
        t
        |> put_epoch(new_epoch)
        |> put_director(new_director)
        |> emit_director_changed(new_director)

      {:error, reason} ->
        raise "Bedrock: failed to start director: #{inspect(reason)}"
    end
  end

  def start_director_if_necessary(t), do: t

  @spec stop_any_director_on_this_node!(State.t()) :: State.t()
  def stop_any_director_on_this_node!(t) when t.director == :unavailable, do: t

  def stop_any_director_on_this_node!(t) do
    if node() == node(t.director) do
      DynamicSupervisor.terminate_child(t.supervisor_otp_name, t.director)

      t
      |> put_director(:unavailable)
      |> emit_director_changed(:unavailable)
    else
      t
    end
  end
end
