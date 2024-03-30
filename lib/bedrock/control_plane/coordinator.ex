defmodule Bedrock.ControlPlane.Coordinator do
  use GenServer

  alias Bedrock.ControlPlane.Config.Policies
  alias Bedrock.DataPlane.TransactionSystem.ReadVersionProxy
  alias Bedrock.ControlPlane.Config
  alias Bedrock.ControlPlane.ClusterController
  alias Bedrock.Raft
  alias Bedrock.Raft.Log
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
  def handle_call({:controller_started, {_controller, node} = controller}, _from, t) do
    Logger.debug("Bedrock [#{t.cluster.name()}]: #{node} is the controller")
    {:reply, :ok, t |> update_controller(controller)}
  end

  def handle_call(:get_configuration, _from, t),
    do: {:reply, t.configuration, t}

  def handle_call(:ping, _from, t),
    do: {:reply, :pong, t}

  def handle_call(:get_controller, _from, t) when t.controller == :unavailable,
    do: {:reply, {:error, :unavailable}, t}

  def handle_call(:get_controller, _from, t),
    do: {:reply, {:ok, t.controller}, t}

  def handle_call(:get_nearest_read_version_proxy, _from, t) do
    {t, read_version_proxy} = t |> get_or_create_read_version_proxy()
    {:reply, {:ok, read_version_proxy}, t}
  end

  @impl GenServer
  def handle_info({:raft, :leadership_changed, {new_leader, epoch}}, t) do
    if is_pid(t.controller) and t.my_node == node(t.controller) do
      try do
        GenServer.stop(t.controller, :shutdown, 50)
      catch
        :exit, {:noproc, _} -> :ok
      end
    end

    my_node = t.my_node

    controller =
      case new_leader do
        :undecided -> :unavailable
        ^my_node -> start_controller_on_this_node(t, epoch)
        other_node -> :rpc.call(other_node, Process, :whereis, [t.controller_otp_name], 100)
      end

    {:noreply, t |> update_controller(controller)}
  end

  def handle_info({:raft, :timer, event}, t) do
    raft = t.raft |> Raft.handle_event(event, :timer)
    {:noreply, %{t | raft: raft}}
  end

  def handle_info({:raft, :send_rpc, event, target}, t) do
    GenServer.cast({t.otp_name, target}, {:raft, :rpc, event, Node.self()})
    {:noreply, t}
  end

  @impl GenServer
  def handle_cast({:raft, :rpc, event, source}, t) do
    raft = t.raft |> Raft.handle_event(event, source)
    {:noreply, %{t | raft: raft}}
  end

  def start_controller_on_this_node(t, epoch) do
    with {:ok, coordinator_nodes} <- t.cluster.coordinator_nodes(),
         {:ok, config} <- latest_safe_config(t, coordinator_nodes),
         {:ok, controller} <-
           DynamicSupervisor.start_child(
             t.supervisor_otp_name,
             {ClusterController,
              [
                cluster: t.cluster,
                config: config,
                epoch: epoch,
                coordinator: t.otp_name,
                otp_name: t.controller_otp_name
              ]}
           ) do
      controller
    else
      {:error, reason} -> raise "Bedrock: failed to start controller: #{inspect(reason)}"
    end
  end

  def latest_safe_config(t, coordinator_nodes) do
    t.raft
    |> Raft.log()
    |> Log.transactions_to(:newest_safe)
    |> List.last()
    |> case do
      nil ->
        {:ok,
         initial_config_for_new_system(
           coordinator_nodes,
           1000.0 / t.cluster.coordinator_ping_timeout_in_ms()
         )}

      {_transaction_id, config} ->
        {:ok, config}
    end
  end

  def initial_config_for_new_system(coordinator_nodes, retransmission_rate_in_hz) do
    %Config{
      state: :initializing,
      parameters: %Config.Parameters{
        nodes: coordinator_nodes,
        retransmission_rate_in_hz: retransmission_rate_in_hz,
        replication_factor: 1,
        desired_coordinators: length(coordinator_nodes),
        desired_logs: 1,
        desired_get_read_version_proxies: 1,
        desired_commit_proxies: 1,
        desired_transaction_resolvers: 1
      },
      policies: %Policies{
        allow_volunteer_nodes_to_join: true
      },
      transaction_system_layout: %Config.TransactionSystemLayout{
        storage_teams: [
          %Config.StorageTeamDescriptor{
            tag: 1,
            start_key: <<>>,
            storage_worker_ids: []
          },
          %Config.StorageTeamDescriptor{
            tag: 0,
            start_key: <<0xFF>>,
            storage_worker_ids: []
          }
        ]
      }
    }
  end

  def update_controller(t, new_controller), do: %{t | controller: new_controller}

  def get_or_create_read_version_proxy(%{read_version_proxies: []} = t) do
    {:ok, read_version_proxy} =
      DynamicSupervisor.start_child(
        t.supervisor_otp_name,
        {ReadVersionProxy,
         [
           id: :rand.uniform(100_000_000),
           controller: t.controller
         ]}
      )

    {%{t | read_version_proxies: [read_version_proxy]}, read_version_proxy}
  end

  def get_or_create_read_version_proxy(%{read_version_proxies: proxies} = t) do
    {t, proxies |> Enum.random()}
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

    def consensus_reached(log, transaction_id) do
      send(self(), {:raft, :consensus_reached, log, transaction_id})
      :ok
    end
  end
end
