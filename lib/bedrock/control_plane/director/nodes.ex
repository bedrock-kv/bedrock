defmodule Bedrock.ControlPlane.Director.Nodes do
  @moduledoc """
  Manages node lifecycle and service discovery integration for the director.

  This module handles the registration and tracking of services as nodes join
  and leave the cluster. It serves as the bridge between the coordinator's
  service discovery and the director's internal service representation.

  ## Service Discovery Integration

  Services discovered through the coordinator are registered directly in
  coordinator format without conversion. This unified approach maintains
  consistent service identity throughout the system while eliminating
  format translation overhead.

  The coordinator format `{kind, {otp_name, node}}` flows directly from
  service registration through recovery phases, ensuring that service
  identity and location information remains consistent across all system
  components. Status information is tracked separately when needed, allowing
  the core service identity to remain simple and cacheable.

  Node rejoin operations batch service registrations to minimize state
  transitions while maintaining atomic updates to the director's service
  directory.
  """

  alias Bedrock.Cluster
  alias Bedrock.ControlPlane.Config
  alias Bedrock.ControlPlane.Director
  alias Bedrock.ControlPlane.Director.State
  alias Bedrock.Internal.TimerManagement
  alias Bedrock.Service.Foreman
  alias Bedrock.Service.Worker

  use TimerManagement

  @type worker_creation_error ::
          {:node_lacks_capability, node(), :log | :storage}
          | {:worker_creation_failed, any()}
          | {:worker_info_failed, any()}

  @spec request_to_rejoin(
          State.t(),
          node(),
          capabilities :: [Cluster.capability()],
          running_services :: [Director.running_service_info()],
          at :: DateTime.t()
        ) :: {:ok, State.t()}
  def request_to_rejoin(t, node, _capabilities, running_services, _at) do
    # With capabilities now managed by coordinator, simply add running services
    t
    |> add_running_services(node, running_services)
    |> then(&{:ok, &1})
  end

  @spec node_added_worker(State.t(), node(), Director.running_service_info(), DateTime.t()) ::
          State.t()
  def node_added_worker(t, node, info, _at) do
    # Simply add the running service without node tracking
    t |> add_running_service(node, info)
  end

  @spec ping_all_coordinators(State.t()) :: State.t()
  def ping_all_coordinators(t) do
    GenServer.abcast(
      Config.coordinators(t.config),
      t.cluster.otp_name(:coordinator),
      {:ping, {t.epoch, self()}}
    )

    t
    |> cancel_timer(:ping_all_coordinators)
    |> set_timer(:ping_all_coordinators, Config.ping_rate_in_ms(t.config))
  end

  @spec update_last_seen_at(State.t(), node(), at :: DateTime.t()) :: State.t()
  def update_last_seen_at(t, _node, _at) do
    # Node liveness now handled by coordinator registration
    t
  end

  @spec update_minimum_read_version(
          State.t(),
          node(),
          minimum_read_version :: Bedrock.version() | nil
        ) :: State.t()
  def update_minimum_read_version(t, _node, _minimum_read_version) do
    # Minimum read version tracking removed with NodeTracking
    t
  end

  @spec determine_dead_nodes(State.t(), at :: DateTime.t()) :: State.t()
  def determine_dead_nodes(t, _at) do
    # Dead node detection now handled by coordinator
    t
  end

  @spec maybe_add_node(State.t(), node()) :: State.t()
  def maybe_add_node(t, _node) do
    # Node addition now handled by coordinator registration
    t
  end

  @spec update_capabilities(State.t(), node(), [Cluster.capability()]) :: State.t()
  def update_capabilities(t, _node, _capabilities) do
    # Capabilities now handled by coordinator
    t
  end

  @spec add_running_services(
          State.t(),
          node(),
          service_infos :: [Director.running_service_info()]
        ) ::
          State.t()
  def add_running_services(t, node, service_infos) do
    t
    |> Map.update!(:services, fn services ->
      service_infos
      |> Enum.reduce(services, fn service_info, services ->
        services
        |> Map.put(
          service_info[:id],
          {service_info[:kind], {service_info[:otp_name], node}}
        )
      end)
    end)
  end

  @spec add_running_service(State.t(), node(), service_info :: Director.running_service_info()) ::
          State.t()
  def add_running_service(t, node, service_info) do
    t
    |> Map.update!(:services, fn services ->
      services
      |> Map.put(
        service_info[:id],
        {service_info[:kind], {service_info[:otp_name], node}}
      )
    end)
  end

  @spec node_down(State.t(), node()) :: State.t()
  def node_down(t, node) do
    t
    |> Map.update!(:services, fn services ->
      services
      |> Enum.map(fn
        {id, %{last_seen: {_, ^node}} = service} -> {id, %{service | status: :down}}
        id_and_service -> id_and_service
      end)
      |> Map.new()
    end)
  end

  @spec request_worker_creation(State.t(), node(), Worker.id(), :log | :storage) ::
          {:ok, Director.running_service_info()} | {:error, worker_creation_error()}
  def request_worker_creation(t, node, worker_id, kind) do
    # Check if the node has the required capability using director's capability map
    nodes_with_capability = Map.get(t.node_capabilities, kind, [])

    if node in nodes_with_capability do
      # Contact the foreman on the target node to create the worker
      foreman_ref = {t.cluster.otp_name(:foreman), node}

      case Foreman.new_worker(foreman_ref, worker_id, kind, timeout: 10_000) do
        {:ok, worker_ref} ->
          get_worker_info(worker_ref)

        {:error, reason} ->
          {:error, {:worker_creation_failed, reason}}
      end
    else
      {:error, {:node_lacks_capability, node, kind}}
    end
  end

  @spec get_worker_info(Worker.ref()) ::
          {:ok, Director.running_service_info()} | {:error, {:worker_info_failed, term()}}
  defp get_worker_info(worker_ref) do
    case Worker.info(worker_ref, [:id, :otp_name, :kind, :pid]) do
      {:ok, worker_info} -> {:ok, worker_info}
      {:error, reason} -> {:error, {:worker_info_failed, reason}}
    end
  end
end
