defmodule Bedrock.DataPlane.Log.Shale.PushingTest do
  use ExUnit.Case
  alias Bedrock.DataPlane.Log.Shale.Pushing
  alias Bedrock.DataPlane.Log.Shale.State
  alias Bedrock.DataPlane.Transaction

  setup do
    log = :ets.new(:log, [:set, :public, :named_table])
    state = %State{mode: :running, last_version: 1, log: log, pending_transactions: %{}}
    {:ok, state: state}
  end

  test "push returns :error when mode is :locked and from is not director", %{state: state} do
    state = %{state | mode: :locked, director: :director}
    assert {:error, :locked} == Pushing.push(state, 1, :not_director)
  end

  test "push returns :error when mode is not :running", %{state: state} do
    state = %{state | mode: :stopped}
    assert {:error, :unavailable} == Pushing.push(state, 1, {:transaction, fn -> :ok end})
  end

  test "push returns :ok and updates state when expected_version matches last_version", %{
    state: state
  } do
    transaction = Transaction.new(1, %{key: "value"})
    assert {:ok, _} = Pushing.push(state, 1, {transaction, fn -> :ok end})
  end

  test "push returns :waiting and updates pending_transactions when expected_version is greater than last_version",
       %{state: state} do
    transaction = Transaction.new(2, %{key: "value"})
    assert {:waiting, _} = Pushing.push(state, 2, {transaction, fn -> :ok end})
  end

  test "push returns :error when expected_version is less than last_version", %{state: state} do
    transaction = Transaction.new(0, %{key: "value"})
    assert {:error, :tx_out_of_order} == Pushing.push(state, 0, {transaction, fn -> :ok end})
  end

  test "apply_transaction returns :ok when insertion succeeds", %{state: state} do
    transaction = Transaction.new(1, %{key: "value"})
    assert {:ok, 1} == Pushing.apply_transaction(state.log, {transaction, fn -> :ok end})
  end

  test "apply_transaction returns :error when insertion fails", %{state: state} do
    transaction = Transaction.new(1, %{key: "value"})
    :ets.insert(state.log, {1, %{key: "existing_value"}})

    assert {:error, :insert_failed} ==
             Pushing.apply_transaction(state.log, {transaction, fn -> :ok end})
  end
end
