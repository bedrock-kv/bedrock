defmodule Bedrock.ControlPlane.Coordinator.Durability do
  @moduledoc false

  import Bedrock.ControlPlane.Coordinator.State.Changes

  alias Bedrock.ControlPlane.Coordinator.Checkpoint
  alias Bedrock.ControlPlane.Coordinator.Commands
  alias Bedrock.ControlPlane.Coordinator.DirectorManagement
  alias Bedrock.ControlPlane.Coordinator.State
  alias Bedrock.ControlPlane.Director
  alias Bedrock.Raft
  alias Bedrock.Raft.Log
  alias Bedrock.Raft.TransactionID

  @type ack_fn :: (term() -> :ok)
  @type waiting_list :: %{Raft.transaction_id() => ack_fn()}

  @spec durably_write_service_registration(State.t(), Commands.command(), ack_fn()) ::
          {:ok, State.t()} | {:error, :not_leader}
  def durably_write_service_registration(t, command, ack_fn) do
    case Raft.add_transaction(t.raft, command) do
      {:ok, raft, txn_id} ->
        {:ok,
         t
         |> set_raft(raft)
         |> wait_for_durable_write_to_complete(ack_fn, txn_id)}

      {:error, _reason} = error ->
        ack_fn.(error)
        error
    end
  end

  @spec wait_for_durable_write_to_complete(State.t(), ack_fn(), Raft.transaction_id()) ::
          State.t()
  def wait_for_durable_write_to_complete(t, ack_fn, txn_id), do: update_in(t.waiting_list, &Map.put(&1, txn_id, ack_fn))

  @spec durable_write_completed(State.t(), Log.t(), Raft.transaction_id()) :: State.t()
  def durable_write_completed(t, log, durable_txn_id) do
    apply_committed(t, log, durable_txn_id, true)
  end

  @spec restore(State.t(), Log.t()) :: State.t()
  def restore(t, log) do
    t |> Checkpoint.restore(log) |> apply_committed(log, Log.newest_safe_transaction_id(log), false)
  end

  defp apply_committed(t, supplied_log, notified_id, effects?) do
    log = if t.raft, do: Raft.log(t.raft), else: supplied_log
    upper = min(notified_id, Log.newest_safe_transaction_id(log))

    if upper <= t.last_durable_txn_id do
      t
    else
      entries = committed_entries(log, t.last_durable_txn_id, upper)
      applied = fold_entries(t, entries)

      :ok = Checkpoint.persist(applied, log)

      if effects? do
        notify_director_of_resource_changes(
          applied.director,
          changed_services(t.service_directory, applied.service_directory),
          applied.node_capabilities,
          t.node_capabilities != applied.node_capabilities
        )

        Enum.reduce(entries, applied, fn {id, command}, state ->
          state = update_in(state.waiting_list, &reply_to_waiter(&1, id))

          case command do
            {:end_epoch, _} -> DirectorManagement.shutdown_director_if_running(state)
            _ -> state
          end
        end)
      else
        applied
      end
    end
  end

  # All committed history is retained in this protocol version. Validate each
  # sequence step before folding, including links that skip a physical record.
  @spec replay_prefix(State.t(), Log.t(), Raft.transaction_id()) :: State.t()
  def replay_prefix(t, log, upper) do
    initial = %State{cluster_id: t.cluster_id, last_durable_txn_id: Log.initial_transaction_id(log)}
    fold_entries(initial, committed_entries(log, initial.last_durable_txn_id, upper))
  end

  defp committed_entries(_log, same, same), do: []

  defp committed_entries(log, from, upper) do
    entries = Log.transactions_from(log, from, upper, TransactionID.index(upper) - TransactionID.index(from))

    final =
      Enum.reduce(entries, from, fn {id, _}, previous ->
        if TransactionID.index(id) != TransactionID.index(previous) + 1 or
             TransactionID.term(id) < TransactionID.term(previous),
           do: exit(:missing_committed_prefix)

        id
      end)

    if final != upper, do: exit(:missing_committed_prefix)
    entries
  end

  defp fold_entries(t, entries) do
    Enum.reduce(entries, t, fn {id, command}, state ->
      director = state.director

      %{state | director: :unavailable}
      |> apply_entry(command, id)
      |> Map.put(:director, director)
      |> put_last_durable_txn_id(id)
    end)
  end

  defp changed_services(before, after_directory) do
    for {id, {kind, ref}} <- after_directory, Map.get(before, id) != {kind, ref}, do: {id, kind, ref}
  end

  defp apply_entry(
         t,
         {:begin_recovery, %{generation: generation, owner_term: term, request_id: request} = allocation},
         {entry_term, _}
       )
       when is_integer(generation) and generation > 0 and generation <= 0xFFFFFFFFFFFFFFFF and is_binary(request) and
              byte_size(request) > 0 and is_integer(term) and term > 0 and term == entry_term do
    if generation > t.generation_floor do
      %{t | generation_floor: generation, last_allocation: allocation}
    else
      exit(:non_monotonic_recovery_allocation)
    end
  end

  defp apply_entry(_t, {:begin_recovery, _}, _id), do: exit(:invalid_recovery_allocation)

  defp apply_entry(t, {:recovery_barrier, %{owner_term: term, request_id: request}}, {term, _})
       when is_binary(request) and byte_size(request) > 0 and is_integer(term) and term > 0, do: t

  defp apply_entry(_t, {:recovery_barrier, _}, _id), do: exit(:invalid_recovery_barrier)
  defp apply_entry(t, command, _id), do: process_command(t, command)

  @spec reply_to_waiter(waiting_list(), Raft.transaction_id()) :: waiting_list()
  def reply_to_waiter(waiting_list, txn_id) do
    waiting_list
    |> Map.pop(txn_id)
    |> case do
      {nil, waiting_list} ->
        waiting_list

      {ack_fn, waiting_list} ->
        ack_fn.({:ok, txn_id})
        waiting_list
    end
  end

  @spec process_command(State.t(), Commands.command()) :: State.t()
  def process_command(t, {:end_epoch, _previous_epoch}) do
    DirectorManagement.shutdown_director_if_running(t)
  end

  def process_command(t, {:set_node_resources, %{node: node, services: services, capabilities: capabilities}}) do
    existing_services_for_node =
      t.service_directory
      |> Enum.filter(fn {_service_id, {_kind, {_name, service_node}}} -> service_node == node end)
      |> Enum.map(fn {service_id, _} -> service_id end)

    new_or_changed_services =
      Enum.filter(services, fn {service_id, kind, worker_ref} ->
        case Map.get(t.service_directory, service_id) do
          {^kind, ^worker_ref} -> false
          _ -> true
        end
      end)

    current_capabilities = Map.get(t.node_capabilities, node, [])
    capabilities_changed = current_capabilities != capabilities

    updated_state =
      t
      |> update_service_directory(fn directory ->
        directory
        |> Map.drop(existing_services_for_node)
        |> Map.merge(
          Map.new(services, fn {service_id, kind, worker_ref} ->
            {service_id, {kind, worker_ref}}
          end)
        )
      end)
      |> update_node_capabilities(node, capabilities)

    notify_director_of_resource_changes(
      updated_state.director,
      new_or_changed_services,
      updated_state.node_capabilities,
      capabilities_changed
    )

    updated_state
  end

  def process_command(t, {:merge_node_resources, %{node: node, services: services, capabilities: capabilities}}) do
    new_or_changed_services =
      Enum.filter(services, fn {service_id, kind, worker_ref} ->
        case Map.get(t.service_directory, service_id) do
          {^kind, ^worker_ref} -> false
          _ -> true
        end
      end)

    current_capabilities = Map.get(t.node_capabilities, node, [])
    merged_capabilities = Enum.uniq(current_capabilities ++ capabilities)
    capabilities_changed = current_capabilities != merged_capabilities

    updated_state =
      t
      |> update_service_directory(fn directory ->
        Enum.into(services, directory, fn {service_id, kind, worker_ref} ->
          {service_id, {kind, worker_ref}}
        end)
      end)
      |> update_node_capabilities(node, merged_capabilities)

    notify_director_of_resource_changes(
      updated_state.director,
      new_or_changed_services,
      updated_state.node_capabilities,
      capabilities_changed
    )

    updated_state
  end

  def process_command(t, {:register_services, %{services: services}}) do
    update_service_directory(t, fn directory ->
      Enum.into(services, directory, fn {service_id, kind, worker_ref} ->
        {service_id, {kind, worker_ref}}
      end)
    end)
  end

  def process_command(t, {:deregister_services, %{service_ids: service_ids}}) do
    update_service_directory(t, fn directory ->
      Map.drop(directory, service_ids)
    end)
  end

  defp notify_director_of_resource_changes(:unavailable, _services, _node_capabilities, _capabilities_changed), do: :ok

  defp notify_director_of_resource_changes(director, new_or_changed_services, node_capabilities, capabilities_changed) do
    if !Enum.empty?(new_or_changed_services) do
      Director.notify_services_registered(director, new_or_changed_services)
    end

    if capabilities_changed do
      capability_map = convert_to_capability_map(node_capabilities)
      Director.notify_capabilities_updated(director, capability_map)
    end
  end
end
