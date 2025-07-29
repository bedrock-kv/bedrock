defmodule Bedrock.Cluster.Gateway.Discovery do
  alias Bedrock.Cluster.Gateway.State
  alias Bedrock.Cluster.PubSub
  alias Bedrock.ControlPlane.Coordinator
  alias Bedrock.Internal.TimerManagement

  import Bedrock.Cluster.Gateway.Telemetry
  import Bedrock.Cluster.Gateway.DirectorRelations

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
    t
    |> trace_searching_for_coordinator_and_get_result()
    |> handle_coordinator_discovery_result(t)
  end

  defp trace_searching_for_coordinator_and_get_result(t) do
    trace_searching_for_coordinator(t.cluster)

    t.known_leader
    |> try_known_leader_first(t)
    |> fallback_to_discovery_if_needed(t)
  end

  defp try_known_leader_first({coordinator_ref, _epoch}, t) do
    coordinator_ref
    |> try_direct_coordinator_call(t.cluster)
    |> case do
      {:ok, _} = success -> success
      {:error, _} -> :fallback_needed
    end
  end

  defp try_known_leader_first(:unavailable, _t), do: :fallback_needed

  defp fallback_to_discovery_if_needed(:fallback_needed, t),
    do: discover_leader_coordinator(t.cluster, t.descriptor.coordinator_nodes)

  defp fallback_to_discovery_if_needed(result, _t), do: result

  defp handle_coordinator_discovery_result({:ok, {coordinator_ref, epoch}}, t) do
    trace_found_coordinator(t.cluster, coordinator_ref)

    t
    |> cancel_timer(:find_a_live_coordinator)
    |> change_coordinator({coordinator_ref, epoch})
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
    try do
      case GenServer.call(coordinator_ref, :ping, cluster.coordinator_ping_timeout_in_ms()) do
        {:pong, epoch, leader_pid} when leader_pid != nil ->
          {:ok, {leader_pid, epoch}}

        {:pong, _epoch, nil} ->
          {:error, :unavailable}

        _ ->
          {:error, :unavailable}
      end
    catch
      :exit, _ -> {:error, :unavailable}
    end
  end

  @spec discover_leader_coordinator(module(), [node()]) ::
          {:ok, {Coordinator.ref(), Bedrock.epoch()}} | {:error, :unavailable}
  defp discover_leader_coordinator(cluster, coordinator_nodes) do
    coordinator_nodes
    |> try_local_coordinator_first(cluster)
    |> fallback_to_multi_call_if_needed(cluster, coordinator_nodes)
  end

  defp try_local_coordinator_first(coordinator_nodes, cluster) do
    if Node.self() in coordinator_nodes do
      cluster.otp_name(:coordinator)
      |> try_direct_coordinator_call(cluster)
    else
      {:error, :not_local}
    end
  end

  defp fallback_to_multi_call_if_needed({:ok, _} = success, _cluster, _coordinator_nodes),
    do: success

  defp fallback_to_multi_call_if_needed({:error, _}, cluster, coordinator_nodes),
    do: multi_call_coordinator_discovery(cluster, coordinator_nodes)

  @spec multi_call_coordinator_discovery(module(), [node()]) ::
          {:ok, {Coordinator.ref(), Bedrock.epoch()}} | {:error, :unavailable}
  defp multi_call_coordinator_discovery(cluster, coordinator_nodes) do
    coordinator_nodes
    |> GenServer.multi_call(
      cluster.otp_name(:coordinator),
      :ping,
      cluster.coordinator_ping_timeout_in_ms()
    )
    |> extract_responses_and_select_leader()
  end

  defp extract_responses_and_select_leader({responses, _failures}) when responses != [],
    do: responses |> select_leader_from_responses()

  defp extract_responses_and_select_leader({[], _failures}),
    do: {:error, :unavailable}

  @spec select_leader_from_responses([{node(), {:pong, Bedrock.epoch(), pid() | nil}}]) ::
          {:ok, {Coordinator.ref(), Bedrock.epoch()}} | {:error, :unavailable}
  def select_leader_from_responses(responses) do
    responses
    |> filter_valid_leaders()
    |> select_highest_epoch_leader()
  end

  defp filter_valid_leaders(responses) do
    responses
    |> Enum.filter(fn
      {_node, {:pong, _epoch, leader_pid}} when leader_pid != nil -> true
      _ -> false
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
  def change_coordinator(t, leader) when t.known_leader == leader, do: t

  def change_coordinator(t, :unavailable) do
    t
    |> Map.put(:known_leader, :unavailable)
    |> trigger_service_discovery_when_leader_available()
  end

  @spec change_coordinator(State.t(), {Coordinator.ref(), Bedrock.epoch()}) :: State.t()
  def change_coordinator(t, {coordinator_ref, _epoch} = leader) do
    PubSub.publish(t.cluster, :coordinator_changed, {:coordinator_changed, coordinator_ref})
    Process.monitor(coordinator_ref)

    t
    |> Map.put(:known_leader, leader)
    |> trigger_service_discovery_when_leader_available()
  end

  @spec trigger_service_discovery_when_leader_available(State.t()) :: State.t()
  defp trigger_service_discovery_when_leader_available(%{known_leader: :unavailable} = t), do: t

  defp trigger_service_discovery_when_leader_available(t) do
    # When a new leader becomes available, trigger pull-based service discovery
    send(self(), :pull_services_from_foreman)
    t
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

    t
    |> fetch_director_from_known_leader()
    |> handle_director_fetch_result(t)
  end

  defp fetch_director_from_known_leader(%{known_leader: {coordinator_ref, _epoch}} = t) do
    timeout_in_ms = t.cluster.gateway_ping_timeout_in_ms()

    coordinator_ref
    |> Coordinator.fetch_director_and_epoch(timeout_in_ms: timeout_in_ms)
    |> tag_with_timeout(timeout_in_ms)
  end

  defp fetch_director_from_known_leader(%{known_leader: :unavailable} = t),
    do: {:no_leader, t.cluster.gateway_ping_timeout_in_ms()}

  defp tag_with_timeout(result, timeout_in_ms),
    do: {result, timeout_in_ms}

  defp handle_director_fetch_result({{:ok, {director_pid, epoch} = director}, _timeout}, t) do
    trace_found_director(t.cluster, director_pid, epoch)

    t
    |> change_director(director)
    |> cancel_timer(:find_current_director)
  end

  defp handle_director_fetch_result({{:error, reason}, timeout_in_ms}, t)
       when reason in [:timeout, :unavailable] do
    t
    |> change_director(:unavailable)
    |> cancel_timer(:find_current_director)
    |> set_timer(:find_current_director, timeout_in_ms)
  end

  defp handle_director_fetch_result({:no_leader, timeout_in_ms}, t) do
    t
    |> change_director(:unavailable)
    |> cancel_timer(:find_current_director)
    |> set_timer(:find_current_director, timeout_in_ms)
  end
end
