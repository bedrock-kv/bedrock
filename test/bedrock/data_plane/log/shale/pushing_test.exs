defmodule Bedrock.DataPlane.Log.Shale.PushingTest do
  use ExUnit.Case
  alias Bedrock.DataPlane.Log.Shale.Pushing
  alias Bedrock.DataPlane.Log.Shale.State
  alias Bedrock.DataPlane.Transaction

  setup do
    log = :ets.new(:log, [:set, :public, :named_table])
    state = %State{log: log, last_version: 0, pending_transactions: %{}}
    {:ok, state: state}
  end

  test "push/3 with matching expected_version", %{state: state} do
    transaction = Transaction.new(1, %{"key" => "value"})
    ack_fn = fn -> :ok end
    transaction_with_ack_fn = {transaction, ack_fn}

    assert {:ok, new_state} = Pushing.push(state, 0, transaction_with_ack_fn)
    assert new_state.last_version == 1
  end

  test "push/3 with higher expected_version", %{state: state} do
    transaction = Transaction.new(2, %{"key" => "value"})
    ack_fn = fn -> :ok end
    transaction_with_ack_fn = {transaction, ack_fn}

    assert {:waiting, new_state} = Pushing.push(state, 1, transaction_with_ack_fn)
    assert new_state.pending_transactions[1] == transaction_with_ack_fn
  end

  test "push/3 with lower expected_version", %{state: state} do
    transaction = Transaction.new(1, %{"key" => "value"})
    ack_fn = fn -> :ok end
    transaction_with_ack_fn = {transaction, ack_fn}

    assert {:error, :tx_out_of_order} = Pushing.push(state, -1, transaction_with_ack_fn)
  end

  test "apply_pending_transactions/1", %{state: state} do
    transaction = Transaction.new(1, %{"key" => "value"})
    ack_fn = fn -> :ok end
    transaction_with_ack_fn = {transaction, ack_fn}
    state = %{state | pending_transactions: %{0 => transaction_with_ack_fn}}

    assert {:ok, new_state} = Pushing.apply_pending_transactions(state)
    assert new_state.last_version == 1
    assert new_state.pending_transactions == %{}
  end

  test "apply_transaction/2", %{state: state} do
    transaction = Transaction.new(1, %{"key" => "value"})
    ack_fn = fn -> :ok end
    transaction_with_ack_fn = {transaction, ack_fn}

    assert {:ok, 1} = Pushing.apply_transaction(state.log, transaction_with_ack_fn)
    assert :ets.lookup(state.log, 1) == [{1, %{"key" => "value"}}]
  end
end
