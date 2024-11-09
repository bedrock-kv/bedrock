defmodule Bedrock.ControlPlane.Director.Nodes do
  @moduledoc false

  alias Bedrock.ControlPlane.Director
  alias Bedrock.ControlPlane.Director.NodeTracking
  alias Bedrock.ControlPlane.Director.State
  alias Bedrock.ControlPlane.Config
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout

  use Bedrock.Internal.TimerManagement

  @spec request_to_rejoin(
          State.t(),
          node(),
          capabilities :: [Bedrock.Cluster.capability()],
          running_services :: [Director.running_service_info()],
          at :: DateTime.t()
        ) ::
          {:ok, State.t()} | {:error, :nodes_must_be_added_by_an_administrator}
  def request_to_rejoin(t, node, capabilities, running_services, at) do
    t =
      t
      |> maybe_add_node(node)
      |> update_last_seen_at(node, at)
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
    |> update_last_seen_at(node, at)
    |> add_running_service(node, info)
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
  def update_last_seen_at(t, node, at) do
    node_up = not NodeTracking.alive?(t.node_tracking, node)
    NodeTracking.update_last_seen_at(t.node_tracking, node, at |> to_milliseconds())
    if node_up, do: t |> node_up(node), else: t
  end

  @spec update_minimum_read_version(
          State.t(),
          node(),
          minimum_read_version :: Bedrock.version() | nil
        ) :: State.t()
  def update_minimum_read_version(t, node, minimum_read_version) do
    NodeTracking.update_minimum_read_version(t.node_tracking, node, minimum_read_version)
    t
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

  @spec add_running_service(State.t(), node(), info :: [Director.running_service_info()]) ::
          State.t()
  def add_running_service(t, node, info) do
    t
    |> Map.update!(:config, fn config ->
      config
      |> Map.update!(:transaction_system_layout, fn transaction_system_layout ->
        transaction_system_layout
        |> Map.put(:id, TransactionSystemLayout.random_id())
        |> Map.update(:services, %{}, fn services ->
          services
          |> Map.put(
            info[:id],
            %{
              kind: info[:kind],
              last_seen: {info[:otp_name], node},
              status: {:up, info[:pid]}
            }
          )
        end)
      end)
    end)
  end

  @spec node_up(State.t(), node()) :: State.t()
  def node_up(t, _node), do: t

  @spec node_down(State.t(), node()) :: State.t()
  def node_down(t, node) do
    t
    |> Map.update!(:config, fn config ->
      config
      |> Map.update!(:transaction_system_layout, fn transaction_system_layout ->
        transaction_system_layout
        |> Map.put(:id, TransactionSystemLayout.random_id())
        |> Map.update(:services, %{}, fn services ->
          services
          |> Enum.map(fn
            {id, %{last_seen: {_, ^node}} = service} -> {id, %{service | status: :down}}
            service -> service
          end)
          |> Map.new()
        end)
      end)
    end)
  end
end
