defmodule Bedrock.ControlPlane.Coordinator.Server do
  @moduledoc false
  use GenServer

  import Bedrock.ControlPlane.Coordinator.DirectorManagement,
    only: [
      try_to_start_director: 1,
      current_director?: 3,
      handle_director_failure: 3,
      cleanup_director_on_leadership_loss: 1
    ]

  import Bedrock.ControlPlane.Coordinator.Durability,
    only: [
      durably_write_service_registration: 3,
      durable_write_completed: 3
    ]

  import Bedrock.ControlPlane.Coordinator.State.Changes,
    only: [
      put_leader_node: 2,
      put_leader_startup_state: 2,
      put_config: 2,
      put_transaction_system_layout: 3,
      update_raft: 2,
      add_tsl_subscriber: 2,
      replay_tsl_to: 2,
      remove_tsl_subscriber: 2,
      check_for_recovery_capability_changes: 1,
      update_recovery_capability_hash: 1
    ]

  import Bedrock.ControlPlane.Coordinator.Telemetry,
    only: [
      trace_started: 2,
      trace_election_completed: 1,
      trace_consensus_reached: 1,
      trace_leader_ready_starting_director: 1,
      trace_recovery_capability_change_detected: 0,
      trace_recovery_retry_attempt: 1,
      trace_recovery_failed: 1
    ]

  import Bedrock.Internal.GenServer.Replies

  alias Bedrock.ClusterBootstrap.Publication
  alias Bedrock.ControlPlane.Config.CoreState
  alias Bedrock.ControlPlane.Coordinator.Commands
  alias Bedrock.ControlPlane.Coordinator.DiskRaftLog
  alias Bedrock.ControlPlane.Coordinator.Durability
  alias Bedrock.ControlPlane.Coordinator.RaftAdapter
  alias Bedrock.ControlPlane.Coordinator.RecoveryGeneration
  alias Bedrock.ControlPlane.Coordinator.State
  alias Bedrock.Raft
  alias Bedrock.Raft.Log
  alias Bedrock.Raft.Log.InMemoryLog
  alias Bedrock.Raft.Log.TupleInMemoryLog

  require Logger

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

    my_node = Node.self()

    with {:ok, coordinator_nodes} <- cluster.fetch_coordinator_nodes(),
         true <- my_node in coordinator_nodes || {:error, :not_a_coordinator},
         {:ok, raft_log} <- init_raft_log(cluster) do
      case Publication.load(cluster) do
        {:ok, loaded} ->
          bootstrap = loaded.bootstrap

          restored =
            Durability.restore(
              %State{
                cluster: cluster,
                cluster_id: bootstrap.cluster_id,
                my_node: my_node,
                otp_name: otp_name,
                supervisor_otp_name: cluster.otp_name(:sup),
                epoch: bootstrap.epoch,
                config: Publication.config(bootstrap, cluster),
                prior_core_state: CoreState.from_bootstrap(bootstrap),
                last_durable_txn_id: Log.initial_transaction_id(raft_log)
              },
              raft_log
            )

          raft = Raft.new(my_node, Enum.reject(coordinator_nodes, &(&1 == my_node)), raft_log, RaftAdapter)
          {:ok, %{restored | raft: raft}, {:continue, :check_recovery_consensus}}

        {:error, reason} ->
          if is_struct(raft_log, DiskRaftLog), do: DiskRaftLog.close(raft_log)
          {:stop, {:bootstrap_unavailable, reason}}
      end
    else
      {:error, :unavailable} -> :ignore
      {:error, :not_a_coordinator} -> :ignore
    end
  end

  @impl true
  def handle_continue(:check_recovery_consensus, t) do
    # Check if this is a single-node cluster that needs recovery consensus
    if Raft.am_i_the_leader?(t.raft) and t.raft.quorum == 0 do
      # Single-node cluster: check for already-committed transactions that need consensus
      log = Raft.log(t.raft)
      newest_safe_txn_id = Log.newest_safe_transaction_id(log)

      # Find and send consensus for already-committed transactions
      send_recovery_consensus_for_committed_transactions(t, log, newest_safe_txn_id)
    end

    {:noreply, t}
  end

  @impl true
  def handle_call(:fetch_config, _from, t), do: reply(t, {:ok, t.config})

  def handle_call(:fetch_transaction_system_layout, _from, %{transaction_system_layout: nil} = t),
    do: reply(t, {:error, :unavailable})

  def handle_call(:fetch_transaction_system_layout, _from, t), do: reply(t, {:ok, t.transaction_system_layout})

  @impl true
  def handle_call(:fetch_service_directory, _from, t), do: reply(t, {:ok, t.service_directory})

  def handle_call({:register_services, services}, from, t) do
    caller_node = Node.self()
    command = Commands.merge_node_resources(caller_node, services, [])

    t
    |> durably_write_service_registration(command, ack_fn(from))
    |> case do
      {:ok, t} -> noreply(t)
      {:error, _reason} = error -> reply(t, error)
    end
  end

  def handle_call({:deregister_services, service_ids}, from, t) do
    command = Commands.deregister_services(service_ids)

    t
    |> durably_write_service_registration(command, ack_fn(from))
    |> case do
      {:ok, t} -> noreply(t)
      {:error, _reason} = error -> reply(t, error)
    end
  end

  def handle_call({:register_node_resources, client_pid, compact_services, capabilities}, from, t) do
    # Always subscribe client for TSL updates (monitor to clean up on death),
    # and replay the current layout so a late joiner learns it immediately
    # instead of waiting for the next broadcast (i.e., the next recovery).
    Process.monitor(client_pid)

    updated_state =
      t
      |> add_tsl_subscriber(client_pid)
      |> replay_tsl_to(client_pid)

    # Expand compact services to full format
    caller_node = node(client_pid)
    expanded_services = expand_compact_services(compact_services, caller_node)

    case updated_state.leader_node do
      node when node == updated_state.my_node ->
        command = Commands.set_node_resources(caller_node, expanded_services, capabilities)

        updated_state
        |> durably_write_service_registration(command, ack_fn(from))
        |> case do
          {:ok, final_state} -> noreply(final_state)
          {:error, _reason} = error -> reply(updated_state, error)
        end

      leader_node ->
        # Not leader - forward async to prevent blocking Raft consensus
        leader_coordinator = {updated_state.otp_name, leader_node}

        GenServer.cast(
          leader_coordinator,
          {:forward_register_node_resources, caller_node, expanded_services, capabilities, from}
        )

        noreply(updated_state)
    end
  end

  def handle_call(:ping, _from, t) do
    leader = if t.leader_node == t.my_node, do: self()
    reply(t, {:pong, t.epoch, leader})
  end

  @impl true
  def handle_info({:raft, :leadership_changed, {new_leader, raft_term} = leadership}, t) do
    cond do
      t.raft == nil or Raft.leadership(t.raft) != leadership ->
        noreply(t)

      {t.leader_node, t.raft_term} == leadership ->
        # A duplicate callback must not reset publication ordering or retry a
        # retired instance. Capability changes own failed-start retries.
        noreply(t)

      true ->
        trace_election_completed(new_leader)

        # Retire the old instance even when this node won a newer term.
        updated_t =
          t
          |> RecoveryGeneration.cancel()
          |> cleanup_director_on_leadership_loss()
          |> put_leader_node(new_leader)
          |> Map.put(:raft_term, raft_term)

        if new_leader == t.my_node and Raft.am_i_the_leader?(t.raft) do
          trace_leader_ready_starting_director(map_size(updated_t.service_directory))

          updated_t
          |> put_leader_startup_state(:leader_ready)
          |> update_recovery_capability_hash()
          |> attempt_director_recovery(:leadership_change)
          |> noreply()
        else
          updated_t |> put_leader_startup_state(:not_leader) |> noreply()
        end
    end
  end

  def handle_info({:raft, :timer, event}, t) do
    t
    |> update_raft(&Raft.handle_event(&1, event, :timer))
    |> noreply()
  end

  def handle_info({:raft, :send_rpc, event, target}, t) do
    GenServer.cast({t.otp_name, target}, {:raft, :rpc, event, Node.self()})
    noreply(t)
  end

  def handle_info({:raft, :consensus_reached, log, durable_txn_id, consistency}, t)
      when consistency in [:behind, :latest] do
    trace_consensus_reached(durable_txn_id)

    updated_t = durable_write_completed(t, log, durable_txn_id)

    # Check if capability changes should trigger recovery retry
    updated_t
    |> RecoveryGeneration.advance()
    |> maybe_retry_recovery_on_capability_change()
    |> noreply()
  end

  def handle_info({:recovery_io_result, id, result}, t), do: noreply(RecoveryGeneration.result(t, id, result))
  def handle_info({:recovery_io_timeout, id}, t), do: noreply(RecoveryGeneration.timeout(t, id))

  def handle_info({:DOWN, monitor_ref, :process, pid, reason}, t) do
    t
    |> RecoveryGeneration.down(monitor_ref, reason)
    |> handle_director_failure(pid, reason)
    |> remove_tsl_subscriber(pid)
    |> noreply()
  end

  @impl true
  def handle_cast({:ping, {epoch, director}}, t) do
    if current_director?(t, director, epoch), do: GenServer.cast(director, {:pong, self()})
    noreply(t)
  end

  def handle_cast({:ping, _}, t), do: noreply(t)

  def handle_cast(
        {:notify_transaction_system_layout, {director, epoch, sequence}, publication_id, layout, core_state},
        t
      ) do
    authorized = reserved_publication?(t, director, epoch, publication_id, layout)

    cond do
      authorized and current_publication?(t, director, epoch, sequence, :layout) ->
        updated = t |> accept_publication(sequence, :layout) |> put_transaction_system_layout(layout, core_state)
        GenServer.cast(director, {:publication_ack, self(), publication_id, sequence})
        noreply(updated)

      authorized and duplicate_layout?(t, sequence, layout, core_state) ->
        GenServer.cast(director, {:publication_ack, self(), publication_id, sequence})
        noreply(t)

      true ->
        noreply(t)
    end
  end

  def handle_cast({:notify_transaction_system_layout, _identity, _layout, _core_state}, t), do: noreply(t)

  def handle_cast({:notify_config, {director, epoch, sequence}, config}, t) do
    if current_publication?(t, director, epoch, sequence, :config) do
      t |> accept_publication(sequence, :config) |> put_config(config) |> noreply()
    else
      noreply(t)
    end
  end

  # Legacy messages have no instance attribution and cannot authorize changes.
  def handle_cast({:notify_transaction_system_layout, _layout, _core_state}, t), do: noreply(t)
  def handle_cast({:notify_config, _config}, t), do: noreply(t)

  def handle_cast({:forward_register_node_resources, node, services, capabilities, original_from}, t) do
    command = Commands.set_node_resources(node, services, capabilities)

    t
    |> durably_write_service_registration(command, ack_fn(original_from))
    |> case do
      {:ok, updated_state} ->
        noreply(updated_state)

      {:error, _reason} = error ->
        # Reply directly to original caller
        GenServer.reply(original_from, error)
        noreply(t)
    end
  end

  def handle_cast({:raft, :rpc, event, source}, t) do
    t
    |> update_raft(&Raft.handle_event(&1, event, source))
    |> noreply()
  end

  defp reserved_publication?(t, director, epoch, publication_id, %{epoch: epoch}) do
    case t.bootstrap_reservation do
      %{generation: ^epoch, recovery_id: ^publication_id} -> current_director?(t, director, epoch)
      _ -> false
    end
  end

  defp reserved_publication?(_t, _director, _epoch, _id, _layout), do: false

  defp duplicate_layout?(t, sequence, layout, core_state),
    do:
      sequence == t.publication_sequences.layout and t.transaction_system_layout == layout and
        t.prior_core_state == core_state

  defp current_publication?(t, director, epoch, sequence, kind) do
    is_integer(sequence) and sequence > t.publication_sequences[kind] and
      current_director?(t, director, epoch)
  end

  defp accept_publication(t, sequence, kind) do
    %{t | publication_sequences: Map.put(t.publication_sequences, kind, sequence)}
  end

  # Private helper functions
  @spec ack_fn(GenServer.from()) :: (term() -> :ok)
  defp ack_fn(from), do: fn result -> GenServer.reply(from, result) end

  @spec init_raft_log(module()) ::
          {:ok, DiskRaftLog.t() | TupleInMemoryLog.t()} | {:error, term()}
  def init_raft_log(cluster) do
    # Use same pattern as logs/storage: get base path from coordinator config
    coordinator_config = Keyword.get(cluster.node_config(), :coordinator, [])

    case Keyword.get(coordinator_config, :path) do
      nil ->
        # No path supplied - use in-memory log (non-persistent)
        {:ok, InMemoryLog.new(:tuple)}

      base_path ->
        # Path supplied - use persistent disk-based log
        working_directory = Path.join(base_path, "raft")
        File.mkdir_p!(working_directory)

        raft_log = DiskRaftLog.new(log_dir: working_directory)
        DiskRaftLog.open(raft_log)
    end
  end

  @spec send_recovery_consensus_for_committed_transactions(
          State.t(),
          Log.t(),
          {non_neg_integer(), non_neg_integer()}
        ) ::
          :ok
  defp send_recovery_consensus_for_committed_transactions(t, log, newest_safe_txn_id) do
    # Find any pending transactions that are actually already committed
    already_committed_txns =
      t.waiting_list
      |> Map.keys()
      |> Enum.filter(fn txn_id ->
        # Transaction is committed if it's <= newest_safe_transaction_id
        txn_id <= newest_safe_txn_id
      end)
      |> Enum.sort()

    if length(already_committed_txns) > 0 do
      Logger.info(
        "Bedrock [#{t.cluster}]: Sending recovery consensus for #{length(already_committed_txns)} already-committed transactions: #{inspect(already_committed_txns)}"
      )

      # Send consensus_reached messages for each already-committed transaction
      Enum.each(already_committed_txns, fn txn_id ->
        send(self(), {:raft, :consensus_reached, log, txn_id, :latest})
      end)
    end
  end

  @spec attempt_director_recovery(State.t(), :leadership_change | :capability_change) :: State.t()
  defp attempt_director_recovery(t, reason) when t.leader_node == t.my_node do
    case t.leader_startup_state do
      :leader_ready ->
        trace_recovery_retry_attempt(reason)

        case try_to_start_director(t) do
          %{recovery_generation: %{phase: phase}} = pending
          when phase in [:barrier, :reading, :allocation, :reserving] ->
            pending

          %{director: :unavailable} = failed_state ->
            # Recovery failed - mark as such and don't retry automatically
            trace_recovery_failed(:director_start_failed)
            put_leader_startup_state(failed_state, :recovery_failed)

          successful_state ->
            # Recovery succeeded
            successful_state
        end

      :recovery_failed ->
        # Don't retry if we've already failed - wait for meaningful capability changes
        case reason do
          :capability_change ->
            # Capability change detected - worth retrying
            attempt_director_recovery(put_leader_startup_state(t, :leader_ready), reason)

          _ ->
            # Other reasons don't trigger retry from failed state
            t
        end

      :not_leader ->
        # Not leader - shouldn't attempt recovery
        t
    end
  end

  defp attempt_director_recovery(t, _reason), do: t

  @spec maybe_retry_recovery_on_capability_change(State.t()) :: State.t()
  defp maybe_retry_recovery_on_capability_change(t) when t.leader_node == t.my_node do
    case check_for_recovery_capability_changes(t) do
      {:changed, updated_t} ->
        trace_recovery_capability_change_detected()
        attempt_director_recovery(updated_t, :capability_change)

      {:unchanged, updated_t} ->
        updated_t
    end
  end

  defp maybe_retry_recovery_on_capability_change(t), do: t

  @spec expand_compact_services([{String.t(), atom(), atom()}], node()) :: [
          Commands.service_info()
        ]
  defp expand_compact_services(compact_services, caller_node) do
    Enum.map(compact_services, fn {service_id, kind, name} ->
      {service_id, kind, {name, caller_node}}
    end)
  end
end
