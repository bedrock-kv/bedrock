defmodule Bedrock.ControlPlane.Coordinator do
  @moduledoc """
  The Coordinator module is responsible for managing the state of the cluster.
  """
  alias Bedrock.ControlPlane.ClusterController
  alias Bedrock.ControlPlane.Config
  alias Bedrock.DataPlane.Proxy
  alias Bedrock.Raft
  alias Bedrock.Raft.Log
  alias Bedrock.Raft.Log.InMemoryLog

  require Logger

  @type ref :: GenServer.name()
  @typep timeout_in_ms :: Bedrock.timeout_in_ms()

  @spec fetch_controller(coordinator :: ref(), timeout_in_ms()) ::
          {:ok, ClusterController.ref()} | {:error, :unavailable}
  def fetch_controller(coordinator, timeout \\ 5_000),
    do: call_service(coordinator, :get_controller, timeout)

  @spec fetch_proxy(coordinator :: ref(), timeout_in_ms()) ::
          {:ok, Proxy.t()} | {:error, :unavailable}
  def fetch_proxy(coordinator, timeout \\ 5_000),
    do: call_service(coordinator, :get_proxy, timeout)

  @spec fetch_config(coordinator :: ref(), timeout_in_ms()) ::
          {:ok, Config.t()} | {:error, :unavailable}
  def fetch_config(coordinator, timeout \\ 5_000),
    do: call_service(coordinator, :get_configuration, timeout)

  @spec call_service(coordinator :: ref(), message :: any(), timeout_in_ms()) ::
          {:ok, any()} | {:error, :unavailable}
  defp call_service(coordinator, message, timeout) do
    GenServer.call(coordinator, message, timeout)
    |> case do
      :unavailable -> {:error, :unavailable}
      config -> {:ok, config}
    end
  catch
    :exit, _ -> {:error, :unavailable}
  end

  @doc false
  defdelegate child_spec(opts), to: __MODULE__.Service

  defmodule Service do
    @moduledoc false
    use GenServer

    @type t :: %__MODULE__{
            cluster: module(),
            am_i_the_leader: boolean(),
            controller: :unavailable | ClusterController.ref(),
            controller_otp_name: atom(),
            my_node: node(),
            otp_name: atom(),
            raft: Raft.t(),
            proxies: [Proxy.t()],
            supervisor_otp_name: atom()
          }
    defstruct cluster: nil,
              am_i_the_leader: false,
              controller: :unavailable,
              controller_otp_name: nil,
              my_node: nil,
              otp_name: nil,
              raft: nil,
              proxies: [],
              supervisor_otp_name: nil

    @spec child_spec(opts :: keyword()) :: Supervisor.child_spec()
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

    def handle_call(:get_controller, _from, t),
      do: {:reply, t.controller, t}

    def handle_call(:get_proxy, _from, t) do
      {t, read_version_proxy} = t |> get_or_create_read_version_proxy()
      {:reply, read_version_proxy, t}
    end

    @impl GenServer
    def handle_info({:raft, :leadership_changed, {new_leader, epoch}}, t) do
      if is_pid(t.controller) and t.my_node == node(t.controller) do
        :ok = stop_controller_on_this_node!(t)
      end

      my_node = t.my_node

      {am_i_the_leader, controller} =
        case new_leader do
          ^my_node ->
            {true, start_controller_on_this_node!(t, epoch)}

          :undecided ->
            {false, :unavailable}

          other_node ->
            IO.inspect(t.controller_otp_name)

            {false,
             :rpc.call(other_node, Process, :whereis, [t.controller_otp_name], 100) ||
               :unavailable}
        end

      {:noreply, t |> update_controller(controller) |> update_am_i_the_leader(am_i_the_leader)}
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

    def handle_cast({:raft, :consensus_reached, _log, _transaction_id}, t) do
      if t.i_am_the_leader do
        IO.inspect("i am the leader")
      end

      {:noreply, t}
    end

    def start_controller_on_this_node!(t, epoch) do
      with {:ok, config} <- latest_safe_config(t),
           {:ok, controller} <-
             DynamicSupervisor.start_child(
               t.supervisor_otp_name,
               {ClusterController,
                [
                  cluster: t.cluster,
                  config: config,
                  epoch: epoch,
                  coordinator: self(),
                  otp_name: t.controller_otp_name
                ]}
             ) do
        controller
      else
        {:error, reason} -> raise "Bedrock: failed to start controller: #{inspect(reason)}"
      end
    end

    def stop_controller_on_this_node!(t, timeout_in_ms \\ 250) do
      ref = Process.monitor(t.controller)

      :ok = GenServer.stop(t.controller, :shutdown, timeout_in_ms)

      receive do
        {:DOWN, ^ref, :process, _pid, _reason} -> :ok
      after
        timeout_in_ms ->
          raise Logger.error("Bedrock: failed to stop controller (#{inspect(t.controller)})")
      end
    catch
      :exit, {:noproc, _} -> :ok
    end

    def latest_safe_config(t) do
      t.raft
      |> Raft.log()
      |> Log.transactions_to(:newest_safe)
      |> List.last()
      |> case do
        nil ->
          {:ok, nodes} = t.cluster.coordinator_nodes()
          {:ok, Config.new(nodes)}

        # retransmission_rate_in_hz = 1000.0 / t.cluster.coordinator_ping_timeout_in_ms()

        # {:ok,
        #  initial_config_for_new_system(
        #    coordinator_nodes,
        #    retransmission_rate_in_hz
        #  )}

        {_transaction_id, config} ->
          {:ok, config}
      end
    end

    def initial_config_for_new_system(coordinator_nodes, retransmission_rate_in_hz) do
      alias Config.StorageTeamDescriptor
      alias Config.TransactionSystemLayout

      team_1 = StorageTeamDescriptor.new(1, {<<>>, <<0xFF>>}, ["storage_id_1"])
      team_0 = StorageTeamDescriptor.new(0, {<<0xFF>>, nil}, ["storage_id_2"])

      tsl =
        %TransactionSystemLayout{
          transaction_resolvers: [
            Config.TransactionResolverDescriptor.new({<<>>, <<0xFF>>}, nil)
          ]
        }
        |> TransactionSystemLayout.insert_storage_team(team_1)
        |> TransactionSystemLayout.insert_storage_team(team_0)

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
        policies: %Config.Policies{
          allow_volunteer_nodes_to_join: true
        },
        transaction_system_layout: tsl
      }
    end

    @spec update_controller(t :: t(), new_controller :: ClusterController.ref()) :: t()
    def update_controller(t, new_controller), do: %{t | controller: new_controller}

    @spec update_am_i_the_leader(t :: t(), am_i_the_leader :: boolean()) :: t()
    def update_am_i_the_leader(t, am_i_the_leader), do: %{t | am_i_the_leader: am_i_the_leader}

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

      def leadership_changed(leadership),
        do: send(self(), {:raft, :leadership_changed, leadership})

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

    defmodule Logic do
    end
  end
end
