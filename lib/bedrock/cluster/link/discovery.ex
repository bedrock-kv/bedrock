defmodule Bedrock.Cluster.Link.Discovery do
  @moduledoc false

  use Bedrock.Internal.TimerManagement

  import Bedrock.Cluster.Link.Telemetry

  alias Bedrock.Cluster.Link.State
  alias Bedrock.ControlPlane.Coordinator

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

  defp fallback_to_discovery_if_needed(
         :fallback_needed,
         %{descriptor: %Bedrock.Cluster.Descriptor{coordinator_nodes: nodes}} = t
       ),
       do: discover_leader_coordinator(t.cluster, nodes)

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
      :coordinator
      |> cluster.otp_name()
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
        select_leader_from_responses(responses)
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
    Enum.filter(responses, fn
      {_node, {:pong, _epoch, leader_pid}} -> !is_nil(leader_pid)
    end)
  end

  defp select_highest_epoch_leader([]), do: {:error, :unavailable}

  defp select_highest_epoch_leader(leader_responses) do
    leader_responses
    |> Enum.max_by(fn {_node, {:pong, epoch, _leader}} -> epoch end)
    |> extract_leader_info()
  end

  defp extract_leader_info({_node, {:pong, best_epoch, best_leader}}), do: {:ok, {best_leader, best_epoch}}

  @spec change_coordinator(State.t(), {Coordinator.ref(), Bedrock.epoch()} | :unavailable) ::
          State.t()
  def change_coordinator(t, coordinator) when t.known_coordinator == coordinator, do: t
  def change_coordinator(t, :unavailable), do: Map.put(t, :known_coordinator, :unavailable)

  @spec change_coordinator(State.t(), Coordinator.ref()) :: State.t()
  def change_coordinator(t, coordinator_ref) do
    t
    |> Map.put(:known_coordinator, coordinator_ref)
    |> monitor_known_coordinator()
    |> register_node_capabilities()
  end

  @spec monitor_known_coordinator(State.t()) :: State.t()
  defp monitor_known_coordinator(t) do
    Process.monitor(t.known_coordinator)
    t
  end

  @spec register_node_capabilities(State.t()) :: State.t()
  defp register_node_capabilities(t) do
    # Register node capabilities with the coordinator immediately upon connection.
    # This ensures the coordinator's node_capabilities map is populated before Director starts.
    if t.capabilities != [] do
      # CRITICAL: Also pull any already-running services from Foreman and register them atomically
      # with capabilities. This prevents a race condition on successive boots where:
      # 1. Services (e.g., storage workers) start and Foreman begins registering them
      # 2. Link connects and registers empty capabilities
      # 3. This triggers consensus, which completes immediately
      # 4. Director starts with 0 services (before Foreman finishes registration)
      # 5. Director fails with :unable_to_meet_log_quorum
      #
      # By pulling running services from Foreman here, we ensure Director sees all
      # already-running services in its initial service directory.
      compact_services = get_running_services_from_foreman(t)
      Coordinator.register_node_resources(t.known_coordinator, self(), compact_services, t.capabilities)
    end

    t
  end

  @spec get_running_services_from_foreman(State.t()) :: [Coordinator.compact_service_info()]
  defp get_running_services_from_foreman(t) do
    # Only query Foreman if we have storage or log capabilities
    if :storage in t.capabilities or :log in t.capabilities do
      foreman_ref = t.cluster.otp_name(:foreman)

      case GenServer.call(foreman_ref, :get_all_running_services, 1000) do
        {:ok, services} when is_list(services) -> services
        _ -> []
      end
    else
      []
    end
  rescue
    _ -> []
  end
end
