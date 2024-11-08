defmodule Bedrock.DataPlane.Log.Shale.LockingTest do
  use ExUnit.Case
  alias Bedrock.DataPlane.Log.Shale.{Locking, State}

  describe "lock_for_recovery/3" do
    test "returns error when a newer epoch exists" do
      state = %State{epoch: 2}
      epoch = 1
      director = self()

      assert {:error, :newer_epoch_exists} = Locking.lock_for_recovery(state, epoch, director)
    end

    test "locks for recovery with valid epoch" do
      state = %State{epoch: nil, mode: :unlocked}
      epoch = 1
      director = self()

      assert {:ok, %State{mode: :locked, epoch: ^epoch, director: ^director}} =
               Locking.lock_for_recovery(state, epoch, director)
    end
  end
end
