defmodule Bedrock.ControlPlane.Coordinator.Server do
  @moduledoc false
  alias Bedrock.Raft
  alias Bedrock.Raft.Log
  alias Bedrock.ControlPlane.Coordinator.DiskRaftLog
  alias Bedrock.Raft.Log.InMemoryLog
  alias Bedrock.Raft.Log.TupleInMemoryLog
  alias Bedrock.ControlPlane.Coordinator.RaftAdapter
  alias Bedrock.ControlPlane.Coordinator.State

  alias Bedrock.ControlPlane.Coordinator.Commands

  import Bedrock.ControlPlane.Coordinator.Durability,
    only: [
      durably_write_config: 3,
      durably_write_transaction_system_layout: 3,
      durable_write_completed: 3
    ]

  import Bedrock.ControlPlane.Coordinator.DirectorManagement,
    only: [
      try_to_start_director: 1,
      handle_director_failure: 3
    ]

  import Bedrock.ControlPlane.Coordinator.State.Changes,
    only: [
      put_leader_node: 2,
      put_epoch: 2,
      update_raft: 2
    ]

  import Bedrock.ControlPlane.Coordinator.Telemetry,
    only: [
      trace_started: 2,
      trace_election_completed: 1,
      trace_consensus_reached: 1
    ]

  require Logger

  use GenServer
  import Bedrock.Internal.GenServer.Replies

  @spec child_spec(opts :: [cluster: module()]) :: Supervisor.child_spec()
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

  @impl true
  def init({cluster, otp_name}) do
    trace_started(cluster, otp_name)

    with my_node <- Node.self(),
         {:ok, coordinator_nodes} <- cluster.fetch_coordinator_nodes(),
         true <- my_node in coordinator_nodes || {:error, :not_a_coordinator},
         {:ok, raft_log} <- init_raft_log(cluster) do
      {:ok,
       %State{
         cluster: cluster,
         my_node: my_node,
         otp_name: otp_name,
         supervisor_otp_name: cluster.otp_name(:sup),
         raft:
           Raft.new(
             my_node,
             coordinator_nodes |> Enum.reject(&(&1 == my_node)),
             raft_log,
             RaftAdapter
           ),
         last_durable_txn_id: Log.initial_transaction_id(raft_log)
       }}
    else
      {:error, :unavailable} -> :ignore
      {:error, :not_a_coordinator} -> :ignore
    end
  end

  @impl true
  def handle_call(:fetch_config, _from, t), do: t |> reply({:ok, t.config})

  def handle_call(:fetch_transaction_system_layout, _from, t),
    do: t |> reply({:ok, t.transaction_system_layout})

  def handle_call(:fetch_director_and_epoch, _from, t) when t.director == :unavailable,
    do: t |> reply({:error, :unavailable})

  def handle_call(:fetch_director_and_epoch, _from, t),
    do: t |> reply({:ok, {t.director, t.epoch}})

  def handle_call({:update_config, config}, from, t) do
    command = Commands.update_config(config)

    t
    |> durably_write_config(command, ack_fn(from))
    |> case do
      {:ok, t} -> t |> noreply()
      {:error, _reason} = error -> t |> reply(error)
    end
  end

  def handle_call({:update_transaction_system_layout, transaction_system_layout}, from, t) do
    command = Commands.update_transaction_system_layout(transaction_system_layout)

    t
    |> durably_write_transaction_system_layout(command, ack_fn(from))
    |> case do
      {:ok, t} -> t |> noreply()
      {:error, _reason} = error -> t |> reply(error)
    end
  end

  @impl true
  def handle_info({:raft, :leadership_changed, {:undecided, _raft_epoch}}, t) do
    Logger.info("Bedrock: Received :undecided leadership change")

    t
    |> put_leader_node(:undecided)
    |> noreply()
  end

  def handle_info({:raft, :leadership_changed, {new_leader, raft_epoch}}, t) do
    trace_election_completed(new_leader)

    t
    |> put_leader_node(new_leader)
    |> put_epoch(raft_epoch)
    |> try_to_start_director()
    |> noreply()
  end

  def handle_info({:raft, :timer, event}, t) do
    t
    |> update_raft(&Raft.handle_event(&1, event, :timer))
    |> noreply()
  end

  def handle_info({:raft, :send_rpc, event, target}, t) do
    GenServer.cast({t.otp_name, target}, {:raft, :rpc, event, Node.self()})
    t |> noreply()
  end

  def handle_info({:raft, :consensus_reached, _log, _durable_txn_id, :behind}, t),
    do: noreply(t)

  def handle_info({:raft, :consensus_reached, log, durable_txn_id, :latest}, t) do
    trace_consensus_reached(durable_txn_id)

    t
    |> durable_write_completed(log, durable_txn_id)
    |> noreply()
  end

  def handle_info({:DOWN, monitor_ref, :process, _pid, reason}, t) do
    t
    |> handle_director_failure(monitor_ref, reason)
    |> noreply()
  end

  @impl true
  def handle_cast({:ping, {epoch, director}}, t) when t.epoch == epoch do
    GenServer.cast(director, {:pong, self()})
    t |> noreply()
  end

  def handle_cast({:ping, _}, t) do
    t |> noreply()
  end

  def handle_cast({:raft, :rpc, event, source}, t) do
    t
    |> update_raft(&Raft.handle_event(&1, event, source))
    |> noreply()
  end

  @spec ack_fn(GenServer.from()) :: (term() -> :ok)
  defp ack_fn(from), do: fn result -> GenServer.reply(from, result) end

  @spec init_raft_log(module()) ::
          {:ok, DiskRaftLog.t() | TupleInMemoryLog.t()} | {:error, term()}
  def init_raft_log(cluster) do
    # Use same pattern as logs/storage: get base path from coordinator config
    coordinator_config = cluster.node_config() |> Keyword.get(:coordinator, [])

    case Keyword.get(coordinator_config, :path) do
      nil ->
        # No path supplied - use in-memory log (non-persistent)
        Logger.info("Bedrock: Using in-memory raft log (no path configured)")
        {:ok, InMemoryLog.new(:tuple)}

      base_path ->
        # Path supplied - use persistent disk-based log
        working_directory = Path.join(base_path, "raft")
        Logger.info("Bedrock: Using persistent raft log at #{working_directory}")
        File.mkdir_p!(working_directory)

        raft_log = DiskRaftLog.new(log_dir: working_directory)
        DiskRaftLog.open(raft_log)
    end
  end
end
