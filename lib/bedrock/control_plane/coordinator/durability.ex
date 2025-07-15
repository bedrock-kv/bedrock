defmodule Bedrock.ControlPlane.Coordinator.Durability do
  alias Bedrock.Raft
  alias Bedrock.Raft.Log
  alias Bedrock.ControlPlane.Coordinator.State
  alias Bedrock.ControlPlane.Config

  import Bedrock.ControlPlane.Coordinator.Telemetry,
    only: [
      trace_director_changed: 1
    ]

  import Bedrock.ControlPlane.Coordinator.State.Changes,
    only: [
      set_raft: 2,
      put_config: 2,
      put_last_durable_txn_id: 2
    ]

  require Logger

  @type ack_fn :: (-> :ok)
  @type waiting_list :: %{Raft.transaction_id() => ack_fn()}

  @spec durably_write_config(State.t(), Config.t(), ack_fn()) ::
          {:ok, State.t()} | {:error, :not_leader}
  def durably_write_config(t, config, ack_fn) do
    with {:ok, raft, txn_id} <- t.raft |> Raft.add_transaction(config) do
      {:ok,
       t
       |> set_raft(raft)
       |> wait_for_durable_write_to_complete(ack_fn, txn_id)}
    end
  end

  @spec wait_for_durable_write_to_complete(State.t(), ack_fn(), Raft.transaction_id()) ::
          State.t()
  def wait_for_durable_write_to_complete(t, ack_fn, txn_id),
    do: update_in(t.waiting_list, &Map.put(&1, txn_id, ack_fn))

  @spec durable_write_to_config_completed(State.t(), Log.t(), Raft.transaction_id()) :: State.t()
  def durable_write_to_config_completed(t, log, durable_txn_id) do
    log
    |> Log.transactions_from(t.last_durable_txn_id, durable_txn_id)
    |> Enum.reduce(t, fn {txn_id, newest_durable_config}, t ->
      update_in(t.waiting_list, &reply_to_waiter(&1, txn_id))
      |> put_config(newest_durable_config)
      |> put_last_durable_txn_id(txn_id)
    end)
    |> maybe_put_director_from_config()
  end

  @spec maybe_put_director_from_config(State.t()) :: State.t()
  def maybe_put_director_from_config(t)
      when t.director != t.config.transaction_system_layout.director do
    %{epoch: epoch, transaction_system_layout: %{director: director}} = t.config
    trace_director_changed(director)

    t
    |> State.Changes.put_epoch(epoch)
    |> State.Changes.put_director(director)
  end

  def maybe_put_director_from_config(t), do: t

  @spec reply_to_waiter(waiting_list(), Raft.transaction_id()) :: waiting_list()
  def reply_to_waiter(waiting_list, txn_id) do
    waiting_list
    |> Map.pop(txn_id)
    |> case do
      {nil, waiting_list} ->
        waiting_list

      {ack_fn, waiting_list} ->
        ack_fn.()
        waiting_list
    end
  end
end
