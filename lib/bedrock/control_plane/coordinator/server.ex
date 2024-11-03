defmodule Bedrock.ControlPlane.Coordinator.Server do
  @moduledoc false
  alias Bedrock.ControlPlane.Config
  alias Bedrock.Raft
  alias Bedrock.Raft.Log
  alias Bedrock.Raft.Log.InMemoryLog

  alias Bedrock.ControlPlane.Coordinator.RaftAdapter
  alias Bedrock.ControlPlane.Coordinator.State

  import Bedrock.ControlPlane.Coordinator.Durability,
    only: [
      durably_write_config: 3,
      durable_write_to_config_completed: 3
    ]

  import Bedrock.ControlPlane.Coordinator.ControllerManagement,
    only: [
      start_cluster_controller_if_necessary: 1,
      stop_any_cluster_controller_on_this_node!: 1
    ]

  import Bedrock.ControlPlane.Coordinator.State.Changes,
    only: [
      put_leader_node: 2,
      update_raft: 2
    ]

  import Bedrock.ControlPlane.Coordinator.Telemetry,
    only: [
      emit_cluster_leadership_changed: 1,
      trace_consensus_reached: 2,
      emit_cluster_controller_changed: 2
    ]

  require Logger

  use GenServer

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

  @impl true
  def init({cluster, otp_name}) do
    with my_node <- Node.self(),
         {:ok, coordinator_nodes} <- cluster.fetch_coordinator_nodes(),
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
         config: config,
         last_durable_txn_id: last_durable_txn_id
       }}
    else
      {:error, :unavailable} -> :ignore
      {:error, :not_a_coordinator} -> :ignore
    end
  end

  @impl true
  def handle_call(:fetch_config, _from, t), do: t |> reply({:ok, t.config})

  def handle_call(:fetch_controller, _from, t) when t.controller == :unavailable,
    do: t |> reply({:error, :unavailable})

  def handle_call(:fetch_controller, _from, t), do: t |> reply({:ok, t.controller})

  def handle_call({:write_config, config}, from, t) do
    t
    |> durably_write_config(config, ack_fn(from))
    |> case do
      {:ok, t} -> t |> noreply()
      {:error, _reason} -> t |> reply({:error, :failed})
    end
  end

  @impl true
  def handle_info({:raft, :leadership_changed, {:undecided, _raft_epoch}}, t)
      when :undecided == t.leader_node do
    t |> noreply()
  end

  def handle_info({:raft, :leadership_changed, {new_leader, _raft_epoch}}, t) do
    t
    |> put_leader_node(new_leader)
    |> emit_cluster_leadership_changed()
    |> stop_any_cluster_controller_on_this_node!()
    |> start_cluster_controller_if_necessary()
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
    t
    |> trace_consensus_reached(durable_txn_id)
    |> durable_write_to_config_completed(log, durable_txn_id)
    |> noreply()
  end

  @impl true
  def handle_cast({:ping, {epoch, controller}}, t) do
    GenServer.cast(controller, {:pong, self()})

    if is_nil(t.epoch) or t.epoch < epoch do
      t
      |> State.Changes.put_epoch(epoch)
      |> State.Changes.put_controller(controller)
      |> emit_cluster_controller_changed(controller)
    else
      t
    end
    |> noreply()
  end

  def handle_cast({:raft, :rpc, event, source}, t) do
    t
    |> update_raft(&Raft.handle_event(&1, event, source))
    |> noreply()
  end

  defp ack_fn(from), do: fn -> GenServer.reply(from, :ok) end

  defp noreply(t), do: {:noreply, t}
  defp reply(t, result), do: {:reply, result, t}
end
