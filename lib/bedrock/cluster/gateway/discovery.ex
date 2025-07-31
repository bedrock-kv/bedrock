defmodule Bedrock.Cluster.Gateway.Discovery do
  @moduledoc false

  alias Bedrock.Cluster.Gateway.State
  alias Bedrock.Cluster.PubSub
  alias Bedrock.ControlPlane.Coordinator
  alias Bedrock.Internal.TimerManagement

  import Bedrock.Cluster.Gateway.Telemetry

  use TimerManagement

  @doc """
  Find the leader coordinator. This implements enhanced two-phase discovery:
  - If we have a known leader, try direct call first
  - If direct call fails or no known leader, use multi_call discovery
  - Select leader with highest epoch and non-nil leader pid
  """
  @spec find_a_live_coordinator(State.t()) ::
          {State.t(), :ok}
          | {State.t(), {:error, :unavailable}}
  def find_a_live_coordinator(t) do
    trace_searching_for_coordinator(t.cluster)

    t
    |> search_for_coordinator()
    |> handle_coordinator_discovery_result(t)
  end

  defp search_for_coordinator(t) do
    t.known_coordinator
    |> try_known_coordinator_first(t)
    |> fallback_to_discovery_if_needed(t)
  end

  defp try_known_coordinator_first(:unavailable, _t), do: :fallback_needed

  defp try_known_coordinator_first(coordinator_ref, t) do
    coordinator_ref
    |> try_direct_coordinator_call(t.cluster)
    |> case do
      {:ok, _} = success -> success
      {:error, _} -> :fallback_needed
    end
  end

  defp fallback_to_discovery_if_needed(:fallback_needed, t),
    do: discover_leader_coordinator(t.cluster, t.descriptor.coordinator_nodes)

  defp fallback_to_discovery_if_needed(result, _t), do: result

  defp handle_coordinator_discovery_result({:ok, {coordinator_ref, _epoch}}, t) do
    trace_found_coordinator(t.cluster, coordinator_ref)

    t
    |> cancel_timer(:find_a_live_coordinator)
    |> change_coordinator(coordinator_ref)
    |> then(&{&1, :ok})
  end

  defp handle_coordinator_discovery_result({:error, _reason} = error, t) do
    t
    |> cancel_timer(:find_a_live_coordinator)
    |> set_timer(:find_a_live_coordinator, t.cluster.gateway_ping_timeout_in_ms())
    |> change_coordinator(:unavailable)
    |> then(&{&1, error})
  end

  @spec try_direct_coordinator_call(Coordinator.ref(), module()) ::
          {:ok, {Coordinator.ref(), Bedrock.epoch()}} | {:error, :unavailable}
  defp try_direct_coordinator_call(coordinator_ref, cluster) do
    coordinator_ref
    |> GenServer.call(:ping, cluster.coordinator_ping_timeout_in_ms())
    |> case do
      {:pong, _epoch, nil} ->
        {:error, :unavailable}

      {:pong, epoch, leader_pid} ->
        {:ok, {leader_pid, epoch}}

      _ ->
        {:error, :unavailable}
    end
  catch
    :exit, _ -> {:error, :unavailable}
  end

  @spec discover_leader_coordinator(module(), [node()]) ::
          {:ok, {Coordinator.ref(), Bedrock.epoch()}} | {:error, :unavailable}
  defp discover_leader_coordinator(cluster, coordinator_nodes) do
    coordinator_nodes
    |> try_local_coordinator_first(cluster)
    |> case do
      {:ok, _} = success ->
        success

      {:error, _reason} ->
        multi_call_coordinator_discovery(cluster, coordinator_nodes)
    end
  end

  defp try_local_coordinator_first(coordinator_nodes, cluster) do
    if Node.self() in coordinator_nodes do
      cluster.otp_name(:coordinator)
      |> try_direct_coordinator_call(cluster)
    else
      {:error, :not_local}
    end
  end

  @spec multi_call_coordinator_discovery(module(), [node()]) ::
          {:ok, {Coordinator.ref(), Bedrock.epoch()}} | {:error, :unavailable}
  defp multi_call_coordinator_discovery(cluster, coordinator_nodes) do
    coordinator_nodes
    |> GenServer.multi_call(
      cluster.otp_name(:coordinator),
      :ping,
      cluster.coordinator_ping_timeout_in_ms()
    )
    |> case do
      {[], _failures} ->
        {:error, :unavailable}

      {responses, _failures} ->
        responses |> select_leader_from_responses()
    end
  end

  @spec select_leader_from_responses([{node(), {:pong, Bedrock.epoch(), Coordinator.ref()}}]) ::
          {:ok, {Coordinator.ref(), Bedrock.epoch()}} | {:error, :unavailable}
  def select_leader_from_responses(responses) do
    responses
    |> filter_valid_leaders()
    |> select_highest_epoch_leader()
  end

  defp filter_valid_leaders(responses) do
    responses
    |> Enum.filter(fn
      {_node, {:pong, _epoch, leader_pid}} -> !is_nil(leader_pid)
    end)
  end

  defp select_highest_epoch_leader([]),
    do: {:error, :unavailable}

  defp select_highest_epoch_leader(leader_responses) do
    leader_responses
    |> Enum.max_by(fn {_node, {:pong, epoch, _leader}} -> epoch end)
    |> extract_leader_info()
  end

  defp extract_leader_info({_node, {:pong, best_epoch, best_leader}}),
    do: {:ok, {best_leader, best_epoch}}

  @doc """
  Change the known leader coordinator. If the leader is the same as the one we already
  have we do nothing, otherwise we publish a message to a topic to let everyone
  on this node know that the coordinator has changed.
  """
  @spec change_coordinator(State.t(), {Coordinator.ref(), Bedrock.epoch()} | :unavailable) ::
          State.t()
  def change_coordinator(t, coordinator) when t.known_coordinator == coordinator, do: t
  def change_coordinator(t, :unavailable), do: t |> Map.put(:known_coordinator, :unavailable)

  @spec change_coordinator(State.t(), Coordinator.ref()) :: State.t()
  def change_coordinator(t, coordinator_ref) do
    PubSub.publish(t.cluster, :coordinator_changed, {:coordinator_changed, coordinator_ref})
    Process.monitor(coordinator_ref)

    t
    |> Map.put(:known_coordinator, coordinator_ref)
    |> trigger_service_discovery_when_available()
  end

  @spec trigger_service_discovery_when_available(State.t()) :: State.t()
  defp trigger_service_discovery_when_available(t) do
    # When a coordinator becomes available, trigger pull-based service discovery
    send(self(), :pull_services_from_foreman)
    t
  end
end
