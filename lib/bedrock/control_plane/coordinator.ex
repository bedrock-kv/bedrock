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
    do: call_service(coordinator, :get_config, timeout)

  @spec write_config(coordinator :: ref(), config :: Config.t(), timeout()) ::
          :ok | {:error, :unavailable}
  def write_config(coordinator, config, timeout \\ 5_000),
    do: call_service(coordinator, {:write_config, config}, timeout)

  @spec call_service(coordinator :: ref(), message :: any(), timeout_in_ms()) ::
          :ok | {:ok, any()} | {:error, :unavailable}
  defp call_service(coordinator, message, timeout) do
    GenServer.call(coordinator, message, timeout)
    |> case do
      :unavailable -> {:error, :unavailable}
      :ok -> :ok
      result -> {:ok, result}
    end
  catch
    :exit, _ -> {:error, :unavailable}
  end

  @doc false
  defdelegate child_spec(opts), to: __MODULE__.Service

  defmodule Data do
    @type t :: %__MODULE__{
            cluster: module(),
            am_i_the_leader: boolean(),
            controller: :unavailable | ClusterController.ref(),
            controller_otp_name: atom(),
            my_node: node(),
            otp_name: atom(),
            raft: Raft.t(),
            proxies: [Proxy.t()],
            supervisor_otp_name: atom(),
            last_durable_txn_id: Raft.transaction_id(),
            config: Config.t(),
            waiting_list: %{Raft.transaction_id() => pid()}
          }
    defstruct cluster: nil,
              am_i_the_leader: false,
              controller: :unavailable,
              controller_otp_name: nil,
              my_node: nil,
              otp_name: nil,
              raft: nil,
              proxies: [],
              supervisor_otp_name: nil,
              last_durable_txn_id: nil,
              config: nil,
              waiting_list: %{}
  end

  defmodule Logic do
    alias Bedrock.Raft

    @type t :: Data.t()

    @spec durably_write_config(t(), Config.t(), GenServer.from()) ::
            {:ok, t()} | {:error, :not_leader}
    def durably_write_config(t, config, from) do
      with {:ok, raft, txn_id} <- t.raft |> Raft.add_transaction(config) do
        {:ok,
         t
         |> update_raft(raft)
         |> add_to_waiting_list_for_txn_id(from, txn_id)}
      end
    end

    @spec update_raft(t(), Raft.t()) :: t()
    def update_raft(t, raft), do: put_in(t.raft, raft)

    @spec add_to_waiting_list_for_txn_id(t(), GenServer.from(), Raft.transaction_id()) :: t()
    def add_to_waiting_list_for_txn_id(t, from, txn_id),
      do: update_in(t.waiting_list, &Map.put(&1, txn_id, from))
  end

  defmodule Service do
    @moduledoc false
    use GenServer

    @type t :: Data.t()

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
           true <- my_node in coordinator_nodes || {:error, :not_a_coordinator},
           raft_log <- InMemoryLog.new() do
        {last_durable_txn_id, config} =
          raft_log
          |> Log.transactions_to(:newest_safe)
          |> List.last()
          |> case do
            nil -> {Log.initial_transaction_id(raft_log), Config.new(coordinator_nodes)}
            txn -> txn
          end

        {:ok,
         %Data{
           cluster: cluster,
           my_node: my_node,
           otp_name: otp_name,
           controller_otp_name: cluster.otp_name(:controller),
           supervisor_otp_name: cluster.otp_name(:sup),
           raft:
             Raft.new(
               my_node,
               coordinator_nodes |> Enum.reject(&(&1 == my_node)),
               raft_log,
               __MODULE__.RaftInterface
             ),
           config: config,
           last_durable_txn_id: last_durable_txn_id
         }}
      else
        {:error, :not_a_coordinator} ->
          :ignore
      end
    end

    @impl GenServer
    def handle_call({:controller_started, {_controller, node} = controller}, _from, t) do
      Logger.debug("Bedrock [#{t.cluster.name()}]: #{node} is the controller")
      {:reply, :ok, t |> update_controller(controller)}
    end

    def handle_call(:get_config, _from, t),
      do: {:reply, t.config, t}

    @spec handle_call({:write_config, Config.t()}, GenServer.from(), t()) ::
            {:noreply, t()} | {:reply, {:error, :failed}, t()}
    def handle_call({:write_config, config}, from, t) do
      t
      |> Logic.durably_write_config(config, from)
      |> case do
        {:ok, t} -> {:noreply, t}
        {:error, _reason} -> {:reply, {:error, :failed}, t}
      end
    end

    def handle_call(:ping, _from, t),
      do: {:reply, {:pong, self()}, t}

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

    def handle_info({:raft, :consensus_reached, log, durable_txn_id}, t) do
      t =
        Log.transactions_from(log, t.last_durable_txn_id, durable_txn_id)
        |> Enum.reduce(t, fn {txn_id, newest_durable_config}, t ->
          t =
            update_in(t.waiting_list, fn waiting_list ->
              Map.get(waiting_list, txn_id)
              |> case do
                nil ->
                  waiting_list

                reply_to ->
                  GenServer.reply(reply_to, :ok)
                  Map.delete(waiting_list, txn_id)
              end
            end)

          %{t | config: newest_durable_config, last_durable_txn_id: txn_id}
        end)

      {:noreply, t}
    end

    @impl GenServer
    def handle_cast({:raft, :rpc, event, source}, t) do
      raft = t.raft |> Raft.handle_event(event, source)
      {:noreply, %{t | raft: raft}}
    end

    def start_controller_on_this_node!(t, epoch) do
      with {:ok, controller} <-
             DynamicSupervisor.start_child(
               t.supervisor_otp_name,
               {ClusterController,
                [
                  cluster: t.cluster,
                  config: t.config,
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

    def stop_controller_on_this_node!(t, timeout_in_ms \\ 100) do
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

      @impl true
      def ignored_event(_event, _from), do: :ok

      @impl true
      def leadership_changed(leadership),
        do: send(self(), {:raft, :leadership_changed, leadership})

      @impl true
      def send_event(to, event) do
        send(self(), {:raft, :send_rpc, event, to})
        :ok
      end

      @impl true
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

      @impl true
      def consensus_reached(log, transaction_id) do
        send(self(), {:raft, :consensus_reached, log, transaction_id})
        :ok
      end
    end
  end
end
