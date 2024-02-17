defmodule Bedrock.ControlPlane.Coordinator do
  use GenServer

  alias Bedrock.DataPlane.TransactionSystem.ReadVersionProxy
  alias Bedrock.ControlPlane.ClusterController
  alias Bedrock.Raft
  alias Bedrock.Raft.Log.InMemoryLog

  require Logger

  @type t :: %__MODULE__{
          cluster: Module.t(),
          controller: :unavailable | {atom(), atom()} | pid(),
          controller_otp_name: atom(),
          my_node: node(),
          otp_name: atom(),
          raft: Raft.t(),
          read_version_proxies: [ReadVersionProxy.t()],
          supervisor_otp_name: atom()
        }
  defstruct cluster: nil,
            controller: :unavailable,
            controller_otp_name: nil,
            my_node: nil,
            otp_name: nil,
            raft: nil,
            read_version_proxies: [],
            supervisor_otp_name: nil

  @spec get_controller(coordinator :: atom()) :: {:ok, atom()} | {:error, :unavailable}
  def get_controller(coordinator, timeout \\ 5_000) do
    GenServer.call(coordinator, :get_controller, timeout)
  catch
    :exit, _ -> {:error, :unavailable}
  end

  def get_nearest_read_version_proxy(coordinator) do
    GenServer.call(coordinator, :get_nearest_read_version_proxy)
  catch
    :exit, _ -> {:error, :unavailable}
  end

  @spec join_cluster(any(), any(), any()) :: :ok | {:error, :unavailable}
  def join_cluster(coordinator, worker_otp_name, services) do
    GenServer.call(coordinator, {:join_cluster, worker_otp_name, services})
  catch
    :exit, _ -> {:error, :unavailable}
  end

  def child_spec(opts) do
    cluster = opts[:cluster] || raise "Missing :cluster option"
    otp_name = cluster.otp_name(:coordinator)

    %{
      id: __MODULE__,
      start:
        {GenServer, :start_link,
         [
           __MODULE__,
           {cluster, otp_name},
           [name: otp_name]
         ]},
      restart: :permanent
    }
  end

  @impl GenServer
  def init({cluster, otp_name}) do
    my_node = Node.self()

    with {:ok, coordinator_nodes} <- cluster.coordinator_nodes(),
         true <- my_node in coordinator_nodes || {:error, :not_a_coordinator} do
      {:ok,
       %__MODULE__{
         cluster: cluster,
         my_node: my_node,
         otp_name: otp_name,
         controller_otp_name: cluster.otp_name(:controller),
         supervisor_otp_name: cluster.otp_name(:sup),
         raft:
           Raft.new(
             my_node,
             coordinator_nodes |> Enum.reject(&(&1 == my_node)),
             InMemoryLog.new(),
             __MODULE__.RaftInterface
           )
       }}
    else
      {:error, :not_a_coordinator} ->
        Logger.warning(
          "Bedrock [#{cluster.name()}]: #{my_node} is not a coordinator; shutting down."
        )

        :ignore
    end
  end

  @impl GenServer
  def handle_call({:controller_started, {_controller, node} = controller}, _from, state) do
    Logger.debug("Bedrock [#{state.cluster.name()}]: #{node} is the controller")
    {:reply, :ok, state |> update_controller(controller)}
  end

  def handle_call(:get_configuration, _from, state),
    do: {:reply, state.configuration, state}

  def handle_call(:ping, _from, state),
    do: {:reply, :pong, state}

  def handle_call(:get_controller, _from, state) do
    state.controller
    |> case do
      :unavailable ->
        {:reply, {:error, :unavailable}, state}

      controller ->
        {:reply, {:ok, controller}, state}
    end
  end

  def handle_call(:get_nearest_read_version_proxy, _from, state) do
    {state, read_version_proxy} = state |> get_or_create_read_version_proxy()
    {:reply, {:ok, read_version_proxy}, state}
  end

  @impl GenServer
  def handle_info({:raft, :leadership_changed, leadership}, state) do
    {new_leader, epoch} = leadership
    cluster_name = state.cluster.name()
    my_node = state.my_node

    if is_pid(state.controller) do
      Logger.debug("Bedrock [#{cluster_name}]: shutting down our controller")
      GenServer.stop(state.controller, :shutdown)
    end

    controller =
      case new_leader do
        :undecided ->
          if state.controller != :unavailable do
            Logger.debug("Bedrock [#{cluster_name}]: leadership lost")
          end

          :unavailable

        ^my_node ->
          Logger.debug("Bedrock [#{cluster_name}]: starting up our controller for epoch #{epoch}")

          {:ok, controller} =
            DynamicSupervisor.start_child(
              state.supervisor_otp_name,
              {ClusterController,
               [
                 cluster: state.cluster,
                 epoch: epoch,
                 coordinator: state.otp_name,
                 otp_name: state.controller_otp_name
               ]}
            )

          controller

        other_node ->
          Logger.debug(
            "Bedrock [#{cluster_name}]: leadership changed to #{other_node} for epoch #{epoch}"
          )

          {state.controller_otp_name, other_node}
      end

    {:noreply, state |> update_controller(controller)}
  end

  def handle_info({:raft, :timer, event}, state) do
    raft = state.raft |> Raft.handle_event(event, :timer)
    {:noreply, %{state | raft: raft}}
  end

  def handle_info({:raft, :send_rpc, event, target}, state) do
    GenServer.cast({state.otp_name, target}, {:raft, :rpc, event, Node.self()})
    {:noreply, state}
  end

  @impl GenServer
  def handle_cast({:raft, :rpc, event, source}, state) do
    raft = state.raft |> Raft.handle_event(event, source)
    {:noreply, %{state | raft: raft}}
  end

  def update_controller(state, :unavailable),
    do: %{state | controller: :unavailable}

  def update_controller(state, controller),
    do: %{state | controller: controller}

  def get_or_create_read_version_proxy(%{read_version_proxies: []} = state) do
    {:ok, read_version_proxy} =
      DynamicSupervisor.start_child(
        state.supervisor_otp_name,
        {ReadVersionProxy,
         [
           id: :rand.uniform(100_000_000),
           controller: state.controller
         ]}
      )

    {%{state | read_version_proxies: [read_version_proxy]}, read_version_proxy}
  end

  def get_or_create_read_version_proxy(%{read_version_proxies: proxies} = state) do
    {state, proxies |> Enum.random()}
  end

  defmodule RaftInterface do
    @moduledoc false
    @behaviour Raft.Interface

    defp determine_timeout(min_ms, max_ms) when min_ms == max_ms, do: min_ms
    defp determine_timeout(min_ms, max_ms) when min_ms > max_ms, do: raise("invalid_timeout")
    defp determine_timeout(min_ms, max_ms), do: min_ms + :rand.uniform(max_ms - min_ms)

    def ignored_event(_event, _from), do: :ok

    def leadership_changed(leadership), do: send(self(), {:raft, :leadership_changed, leadership})

    def send_event(to, event) do
      send(self(), {:raft, :send_rpc, event, to})
      :ok
    end

    def timer(name, min_ms, max_ms) do
      determine_timeout(min_ms, max_ms)
      |> :timer.send_after({:raft, :timer, name})
      |> case do
        {:ok, ref} ->
          fn -> :timer.cancel(ref) end

        {:error, _} ->
          raise "Bedrock: failed to start timer for raft #{inspect(name)}"

          fn -> :ok end
      end
    end
  end
end
