defmodule Bedrock.Cluster.Gateway.DirectorRelations do
  @moduledoc """
  Simplified module focused only on service advertisement.
  Handles worker registration with coordinator via Raft consensus.
  """

  alias Bedrock.Cluster.Gateway.State
  alias Bedrock.ControlPlane.Coordinator
  alias Bedrock.Service.Foreman
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

  @spec pull_services_from_foreman_and_register(State.t()) :: State.t()
  def pull_services_from_foreman_and_register(%{known_coordinator: :unavailable} = t), do: t

  def pull_services_from_foreman_and_register(t) do
    case get_all_services_from_foreman(t) do
      {:ok, []} -> t
      {:ok, services} -> register_services_with_coordinator(t, services)
      {:error, _reason} -> t
    end
  end

  # Private helper functions

  @spec get_all_services_from_foreman(State.t()) ::
          {:ok, [{String.t(), :log | :storage, {atom(), node()}}]} | {:error, term()}
  defp get_all_services_from_foreman(t),
    do: Foreman.get_all_running_services(t.cluster.otp_name(:foreman))

  @spec register_services_with_coordinator(State.t(), [
          {String.t(), :log | :storage, {atom(), node()}}
        ]) :: State.t()
  defp register_services_with_coordinator(t, services) do
    case t.known_coordinator do
      coordinator_ref when coordinator_ref != :unavailable ->
        Coordinator.register_services(coordinator_ref, services)
        t

      :unavailable ->
        t
    end
  end

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
