defmodule Bedrock.ControlPlane.Coordinator.DirectorManagement do
  alias Bedrock.ControlPlane.Director
  alias Bedrock.ControlPlane.Coordinator.State

  import Bedrock.ControlPlane.Coordinator.State.Changes,
    only: [put_director: 2, put_epoch: 2]

  import Bedrock.ControlPlane.Coordinator.Telemetry,
    only: [
      trace_director_changed: 1,
      trace_director_failure_detected: 2
    ]

  require Logger

  @spec start_director_if_necessary(State.t()) :: State.t()
  def start_director_if_necessary(t)
      when t.leader_node == t.my_node do
    new_epoch = Bedrock.Raft.term(t.raft)

    if t.director != :unavailable and t.epoch == new_epoch do
      t
    else
      {:ok, new_director, _monitor_ref} = start_director_with_monitoring!(t, new_epoch)
      trace_director_changed(new_director)

      t
      |> put_epoch(new_epoch)
      |> put_director(new_director)
    end
  end

  def start_director_if_necessary(t), do: t

  @spec start_director_with_monitoring!(State.t(), non_neg_integer()) ::
          {:ok, pid(), reference()} | no_return()
  defp start_director_with_monitoring!(t, epoch) do
    t.supervisor_otp_name
    |> DynamicSupervisor.start_child(
      {Director,
       [
         cluster: t.cluster,
         config: t.config,
         epoch: epoch,
         coordinator: self(),
         relieving: {t.epoch, t.director}
       ]}
    )
    |> case do
      {:ok, director_pid} ->
        monitor_ref = Process.monitor(director_pid)
        {:ok, director_pid, monitor_ref}

      {:error, reason} ->
        raise "Failed to start director: #{inspect(reason)}"
    end
  end

  @spec handle_director_failure(State.t(), director_pid :: pid(), reason :: term()) :: State.t()
  def handle_director_failure(t, director_pid, reason) do
    if t.director == director_pid and t.leader_node == t.my_node do
      trace_director_failure_detected(t.director, reason)
      Logger.warning("Director #{inspect(t.director)} failed with reason: #{inspect(reason)}")

      t
      |> put_director(:unavailable)
    else
      t
    end
  end
end
