defmodule Bedrock.Cluster.Gateway.Discovery do
  alias Bedrock.Cluster.Gateway.State
  alias Bedrock.Cluster.PubSub
  alias Bedrock.ControlPlane.Coordinator

  import Bedrock.Cluster.Gateway.Telemetry
  import Bedrock.Cluster.Gateway.DirectorRelations

  use Bedrock.Internal.TimerManagement

  @doc """
  Find a live coordinator. We make a ping call to all of the nodes that we know
  about and return the first one that responds. If none respond, we return an
  error.
  """
  @spec find_a_live_coordinator(State.t()) ::
          {State.t(), :ok}
          | {State.t(), {:error, :unavailable}}
  def find_a_live_coordinator(t) do
    trace_searching_for_coordinator(t.cluster)

    if t.node in t.descriptor.coordinator_nodes do
      {:ok, t.cluster.otp_name(:coordinator)}
    else
      first_coordinator_that_responds(t.cluster, t.descriptor.coordinator_nodes)
    end
    |> case do
      {:ok, coordinator} ->
        trace_found_coordinator(t.cluster, coordinator)

        t
        |> cancel_timer(:find_a_live_coordinator)
        |> change_coordinator(coordinator)
        |> then(&{&1, :ok})

      {:error, _reason} = error ->
        t
        |> cancel_timer(:find_a_live_coordinator)
        |> set_timer(:find_a_live_coordinator, t.cluster.gateway_ping_timeout_in_ms())
        |> change_coordinator(:unavailable)
        |> then(&{&1, error})
    end
  end

  @spec first_coordinator_that_responds(module(), [node()]) ::
          {:ok, pid()} | {:error, :unavailable}
  defp first_coordinator_that_responds(cluster, coordinator_nodes) do
    GenServer.multi_call(
      coordinator_nodes,
      cluster.otp_name(:coordinator),
      :ping,
      cluster.coordinator_ping_timeout_in_ms()
    )
    |> case do
      {[{_first_node, {:pong, coordinator_pid}} | _other_coordinators], _failures} ->
        {:ok, coordinator_pid}

      {[], _failures} ->
        {:error, :unavailable}
    end
  end

  @doc """
  Change the coordinator. If the coordinator is the same as the one we already
  have we do nothing, otherwise we publish a message to a topic to let everyone
  on this node know that the coordinator has changed.
  """
  @spec change_coordinator(State.t(), Coordinator.ref() | :unavailable) :: State.t()
  def change_coordinator(t, coordinator) when t.coordinator == coordinator, do: t
  def change_coordinator(t, :unavailable), do: t |> Map.put(:coordinator, :unavailable)

  @spec change_coordinator(State.t(), Coordinator.ref()) :: State.t()
  def change_coordinator(t, coordinator) do
    PubSub.publish(t.cluster, :coordinator_changed, {:coordinator_changed, coordinator})
    Process.monitor(t.coordinator)

    t
    |> Map.put(:coordinator, coordinator)
  end

  @doc """
  Find the currently active director. The function attempts to retrieve the
  director from the coordinator within a specified timeout. If successful, it
  updates the state with the new director. If unsuccessful due to a timeout or
  unavailability, it marks the director as unavailable and sets a timer to
  retry.
  """
  @spec find_current_director(State.t()) :: State.t()
  def find_current_director(t) do
    trace_searching_for_director(t.cluster)

    timeout_in_ms = t.cluster.gateway_ping_timeout_in_ms()

    t.coordinator
    |> Coordinator.fetch_director_and_epoch(timeout_in_ms: timeout_in_ms)
    |> case do
      {:ok, {director_pid, epoch} = director} ->
        trace_found_director(t.cluster, director_pid, epoch)

        t
        |> change_director(director)
        |> cancel_timer(:find_current_director)

      {:error, reason} when reason in [:timeout, :unavailable] ->
        t
        |> change_director(:unavailable)
        |> cancel_timer(:find_current_director)
        |> set_timer(:find_current_director, timeout_in_ms)
    end
  end
end
