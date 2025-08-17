defmodule Bedrock.ControlPlane.Coordinator.Durability do
  @moduledoc false

  import Bedrock.ControlPlane.Coordinator.State.Changes

  alias Bedrock.ControlPlane.Coordinator.Commands
  alias Bedrock.ControlPlane.Coordinator.DirectorManagement
  alias Bedrock.ControlPlane.Coordinator.State
  alias Bedrock.ControlPlane.Director
  alias Bedrock.Raft
  alias Bedrock.Raft.Log

  @type ack_fn :: (term() -> :ok)
  @type waiting_list :: %{Raft.transaction_id() => ack_fn()}

  @spec durably_write_config(State.t(), Commands.command(), ack_fn()) ::
          {:ok, State.t()} | {:error, :not_leader} | {:error, :director_not_set}
  def durably_write_config(t, command, ack_fn) do
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

  @spec durably_write_transaction_system_layout(State.t(), Commands.command(), ack_fn()) ::
          {:ok, State.t()} | {:error, :not_leader} | {:error, :director_not_set}
  def durably_write_transaction_system_layout(t, command, ack_fn) do
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
    log
    |> Log.transactions_from(t.last_durable_txn_id, durable_txn_id)
    |> Enum.reduce(t, fn {txn_id, command}, t ->
      t
      |> update_in([Access.key!(:waiting_list)], &reply_to_waiter(&1, txn_id))
      |> process_command(command)
      |> put_last_durable_txn_id(txn_id)
    end)
  end

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
  def process_command(t, {:update_config, %{config: config}}) do
    put_config(t, config)
  end

  def process_command(t, {:update_transaction_system_layout, %{transaction_system_layout: transaction_system_layout}}) do
    t
    |> put_transaction_system_layout(transaction_system_layout)
    |> put_epoch(transaction_system_layout.epoch)
  end

  def process_command(t, {:end_epoch, _previous_epoch}) do
    # End of epoch - shut down the current director if we're running one
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
      capabilities_changed || true
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

  # Private helper to notify director of resource changes
  defp notify_director_of_resource_changes(:unavailable, _services, _node_capabilities, _capabilities_changed), do: :ok

  defp notify_director_of_resource_changes(director, new_or_changed_services, node_capabilities, capabilities_changed) do
    case director do
      :unavailable ->
        :ok

      director ->
        if !Enum.empty?(new_or_changed_services) do
          Director.notify_services_registered(director, new_or_changed_services)
        end

        if capabilities_changed do
          capability_map = convert_to_capability_map(node_capabilities)
          Director.notify_capabilities_updated(director, capability_map)
        end
    end
  end
end
