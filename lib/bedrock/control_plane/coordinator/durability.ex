defmodule Bedrock.ControlPlane.Coordinator.Durability do
  alias Bedrock.Raft
  alias Bedrock.Raft.Log
  alias Bedrock.ControlPlane.Coordinator.State
  alias Bedrock.ControlPlane.Config

  import Bedrock.ControlPlane.Coordinator.Telemetry,
    only: [
      emit_cluster_controller_changed: 2
    ]

  import Bedrock.ControlPlane.Coordinator.State,
    only: [
      update_raft: 2,
      update_config: 2,
      update_controller: 2,
      update_last_durable_txn_id: 2
    ]

  @spec durably_write_config(State.t(), Config.t(), GenServer.from()) ::
          {:ok, State.t()} | {:error, :not_leader}
  def durably_write_config(t, config, from) do
    with {:ok, raft, txn_id} <- t.raft |> Raft.add_transaction(config) do
      {:ok,
       t
       |> update_raft(raft)
       |> wait_for_durable_write_to_complete(from, txn_id)}
    end
  end

  @spec wait_for_durable_write_to_complete(State.t(), GenServer.from(), Raft.transaction_id()) ::
          State.t()
  def wait_for_durable_write_to_complete(t, from, txn_id),
    do: update_in(t.waiting_list, &Map.put(&1, txn_id, from))

  @spec durable_write_to_config_completed(State.t(), Log.t(), Raft.transaction_id()) :: State.t()
  def durable_write_to_config_completed(t, log, durable_txn_id) do
    log
    |> Log.transactions_from(t.last_durable_txn_id, durable_txn_id)
    |> Enum.reduce(t, fn {txn_id, newest_durable_config}, t ->
      update_in(t.waiting_list, &reply_to_waiter(&1, txn_id))
      |> update_config(newest_durable_config)
      |> update_last_durable_txn_id(txn_id)
    end)
    |> maybe_update_controller_from_config()
  end

  def maybe_update_controller_from_config(t)
      when t.controller != t.config.transaction_system_layout.controller do
    new_controller = t.config.transaction_system_layout.controller

    t
    |> update_controller(new_controller)
    |> emit_cluster_controller_changed(new_controller)
  end

  def maybe_update_controller_from_config(t), do: t

  def reply_to_waiter(waiting_list, txn_id) do
    waiting_list
    |> Map.pop(txn_id)
    |> case do
      {nil, waiting_list} ->
        waiting_list

      {reply_to, waiting_list} ->
        GenServer.reply(reply_to, :ok)
        waiting_list
    end
  end
end
