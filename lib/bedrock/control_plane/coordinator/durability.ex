defmodule Bedrock.ControlPlane.Coordinator.Durability do
  alias Bedrock.Raft
  alias Bedrock.Raft.Log
  alias Bedrock.ControlPlane.Coordinator.State
  alias Bedrock.ControlPlane.Coordinator.Commands
  alias Bedrock.ControlPlane.Director

  import Bedrock.ControlPlane.Coordinator.State.Changes

  @type ack_fn :: (term() -> :ok)
  @type waiting_list :: %{Raft.transaction_id() => ack_fn()}

  @spec durably_write_config(State.t(), Commands.command(), ack_fn()) ::
          {:ok, State.t()} | {:error, :not_leader} | {:error, :director_not_set}
  def durably_write_config(t, command, ack_fn) do
    with {:ok, raft, txn_id} <- t.raft |> Raft.add_transaction(command) do
      {:ok,
       t
       |> set_raft(raft)
       |> wait_for_durable_write_to_complete(ack_fn, txn_id)}
    else
      {:error, _reason} = error ->
        ack_fn.(error)
        error
    end
  end

  @spec durably_write_transaction_system_layout(State.t(), Commands.command(), ack_fn()) ::
          {:ok, State.t()} | {:error, :not_leader} | {:error, :director_not_set}
  def durably_write_transaction_system_layout(t, command, ack_fn) do
    with {:ok, raft, txn_id} <- t.raft |> Raft.add_transaction(command) do
      {:ok,
       t
       |> set_raft(raft)
       |> wait_for_durable_write_to_complete(ack_fn, txn_id)}
    else
      {:error, _reason} = error ->
        ack_fn.(error)
        error
    end
  end

  @spec durably_write_service_registration(State.t(), Commands.command(), ack_fn()) ::
          {:ok, State.t()} | {:error, :not_leader}
  def durably_write_service_registration(t, command, ack_fn) do
    with {:ok, raft, txn_id} <- t.raft |> Raft.add_transaction(command) do
      {:ok,
       t
       |> set_raft(raft)
       |> wait_for_durable_write_to_complete(ack_fn, txn_id)}
    else
      {:error, _reason} = error ->
        ack_fn.(error)
        error
    end
  end

  @spec wait_for_durable_write_to_complete(State.t(), ack_fn(), Raft.transaction_id()) ::
          State.t()
  def wait_for_durable_write_to_complete(t, ack_fn, txn_id),
    do: update_in(t.waiting_list, &Map.put(&1, txn_id, ack_fn))

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
    t
    |> put_config(config)
  end

  def process_command(
        t,
        {:update_transaction_system_layout,
         %{transaction_system_layout: transaction_system_layout}}
      ) do
    t
    |> put_transaction_system_layout(transaction_system_layout)
    |> put_epoch(transaction_system_layout.epoch)
    |> put_director(transaction_system_layout.director)
  end

  def process_command(
        t,
        {:start_epoch, %{epoch: epoch, director: director, relieving: _relieving}}
      ) do
    t
    |> put_epoch(epoch)
    |> put_director(director)
  end

  def process_command(t, {:register_services, %{services: services}}) do
    # Filter only new or changed services before updating
    new_or_changed_services =
      Enum.filter(services, fn {service_id, kind, worker_ref} ->
        case Map.get(t.service_directory, service_id) do
          # Same service info, skip
          {^kind, ^worker_ref} -> false
          # New or changed service
          _ -> true
        end
      end)

    updated_state =
      t
      |> update_service_directory(fn directory ->
        Enum.into(services, directory, fn {service_id, kind, worker_ref} ->
          {service_id, {kind, worker_ref}}
        end)
      end)

    # Notify director of new services
    notify_director_if_needed(updated_state.director, new_or_changed_services)

    updated_state
  end

  def process_command(t, {:deregister_services, %{service_ids: service_ids}}) do
    t
    |> update_service_directory(fn directory ->
      Map.drop(directory, service_ids)
    end)
  end

  # Private helper to notify director only when needed
  defp notify_director_if_needed(:unavailable, _service_infos), do: :ok
  defp notify_director_if_needed(_director, []), do: :ok

  defp notify_director_if_needed(director, service_infos),
    do: Director.notify_services_registered(director, service_infos)
end
