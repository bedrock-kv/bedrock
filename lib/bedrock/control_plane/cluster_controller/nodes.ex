defmodule Bedrock.ControlPlane.ClusterController.Nodes do
  @moduledoc false

  alias Bedrock.ControlPlane.ClusterController
  alias Bedrock.ControlPlane.ClusterController.NodeTracking
  alias Bedrock.ControlPlane.ClusterController.State
  alias Bedrock.ControlPlane.Config
  alias Bedrock.ControlPlane.Config.ServiceDescriptor
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout

  use Bedrock.Internal.TimerManagement

  import Bedrock.ControlPlane.ClusterController.State.Changes,
    only: [update_config: 2]

  import Bedrock.ControlPlane.Config.Changes,
    only: [update_transaction_system_layout: 2]

  import Bedrock.ControlPlane.Config.TransactionSystemLayout.Changes,
    only: [upsert_service_descriptor: 2]

  @spec request_to_rejoin(
          State.t(),
          node(),
          capabilities :: [Bedrock.Cluster.capability()],
          running_services :: [ClusterController.running_service_info()],
          at :: DateTime.t()
        ) ::
          {:ok, State.t()} | {:error, :nodes_must_be_added_by_an_administrator}
  def request_to_rejoin(t, node, capabilities, running_services, at) do
    t =
      t
      |> maybe_add_node(node)
      |> node_last_seen_at(node, at)
      |> update_capabilities(node, capabilities)

    if NodeTracking.authorized?(t.node_tracking, node) do
      running_services
      |> Enum.reduce(t, &add_running_service(&2, node, &1))
      |> then(&{:ok, &1})
    else
      {:error, :nodes_must_be_added_by_an_administrator}
    end
  end

  def node_added_worker(t, node, info, at) do
    t
    |> node_last_seen_at(node, at)
    |> add_running_service(node, info)
  end

  @spec ping_all_coordinators(State.t()) :: State.t()
  def ping_all_coordinators(t) do
    t.cluster.ping_nodes(Config.coordinators(t.config), self(), t.epoch)

    t
    |> cancel_timer(:ping_all_nodes)
    |> set_timer(:ping_all_nodes, Config.ping_rate_in_ms(t.config))
  end

  @spec node_last_seen_at(State.t(), node(), at :: DateTime.t()) :: State.t()
  def node_last_seen_at(t, node, at) do
    node_up = not NodeTracking.alive?(t.node_tracking, node)
    NodeTracking.update_last_seen_at(t.node_tracking, node, at |> to_milliseconds())
    if node_up, do: t |> node_up(node), else: t
  end

  @spec determine_dead_nodes(State.t(), at :: DateTime.t()) :: State.t()
  def determine_dead_nodes(t, at) do
    t.node_tracking
    |> NodeTracking.dying_nodes(at |> to_milliseconds(), 3 * Config.ping_rate_in_ms(t.config))
    |> Enum.reduce(t, fn dying_node, t ->
      t.node_tracking |> NodeTracking.down(dying_node)
      t |> node_down(dying_node)
    end)
  end

  @spec to_milliseconds(DateTime.t()) :: integer()
  def to_milliseconds(dt), do: DateTime.to_unix(dt, :millisecond)

  @spec maybe_add_node(State.t(), node()) :: State.t()
  def maybe_add_node(t, node) do
    if not NodeTracking.exists?(t.node_tracking, node) do
      NodeTracking.add_node(
        t.node_tracking,
        node,
        Config.allow_volunteer_nodes_to_join?(t.config)
      )
    end

    t
  end

  @spec update_capabilities(State.t(), node(), [Bedrock.Cluster.capability()]) :: State.t()
  def update_capabilities(t, node, capabilities) do
    NodeTracking.update_capabilities(t.node_tracking, node, capabilities)
    t
  end

  @spec add_running_service(State.t(), node(), info :: [ClusterController.running_service_info()]) ::
          State.t()
  def add_running_service(t, node, info) do
    t
    |> update_config(fn config ->
      config
      |> update_transaction_system_layout(fn transaction_system_layout ->
        transaction_system_layout
        |> upsert_service_descriptor(
          ServiceDescriptor.new(info[:id], info[:kind])
          |> ServiceDescriptor.up(info[:pid], info[:otp_name], node)
        )
      end)
    end)
  end

  @spec node_up(State.t(), node()) :: State.t()
  def node_up(t, _node), do: t

  @spec node_down(State.t(), node()) :: State.t()
  def node_down(t, node) do
    t
    |> update_config(fn config ->
      config
      |> update_transaction_system_layout(&TransactionSystemLayout.Changes.node_down(&1, node))
    end)
  end
end
