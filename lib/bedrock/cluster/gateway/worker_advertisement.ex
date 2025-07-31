defmodule Bedrock.Cluster.Gateway.WorkerAdvertisement do
  @moduledoc """
  Handles worker advertisement and service registration.
  Manages individual worker registration with coordinator via Raft consensus.
  """

  alias Bedrock.Cluster.Gateway.State
  alias Bedrock.ControlPlane.Coordinator
  alias Bedrock.Service.Worker

  @spec advertise_worker_with_leader_check(State.t(), Worker.ref()) :: State.t()
  def advertise_worker_with_leader_check(%{known_coordinator: :unavailable} = t, _worker_pid) do
    # Ignore service notifications when no coordinator is available
    t
  end

  def advertise_worker_with_leader_check(t, worker_pid) do
    # Coordinator is available, process worker advertisement via coordinator
    register_single_worker_via_coordinator(worker_pid, t.known_coordinator)
    t
  end

  # Private helper functions

  @spec register_single_worker_via_coordinator(
          Worker.ref(),
          Coordinator.ref() | :unavailable
        ) :: :ok
  defp register_single_worker_via_coordinator(_worker_pid, :unavailable), do: :ok

  defp register_single_worker_via_coordinator(worker_pid, coordinator_ref) do
    case Worker.info(worker_pid, [:id, :otp_name, :kind, :pid]) do
      {:ok, worker_info} ->
        service_info =
          {worker_info[:id], worker_info[:kind], {worker_info[:otp_name], Node.self()}}

        Coordinator.register_services(coordinator_ref, [service_info])
        :ok

      {:error, _} ->
        :ok
    end
  end
end
