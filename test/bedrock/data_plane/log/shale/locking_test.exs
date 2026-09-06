defmodule Bedrock.DataPlane.Log.Shale.LockingTest do
  use ExUnit.Case

  alias Bedrock.DataPlane.Log.Shale
  alias Bedrock.DataPlane.Log.Shale.Locking
  alias Bedrock.DataPlane.Log.Shale.State
  alias Bedrock.Service.RecoveryControl

  @authority %{generation: 1, recovery_id: "locking-test"}

  setup do
    path = Path.join(System.tmp_dir!(), "shale-locking-#{System.unique_integer([:positive])}")
    File.mkdir_p!(path)
    on_exit(fn -> File.rm_rf(path) end)
    control = RecoveryControl.no_grant("locking-test-cluster", "log", Shale)
    :ok = RecoveryControl.write(path, control)
    {:ok, path: path, control: control}
  end

  describe "lock_for_recovery/2" do
    test "returns error when a newer grant exists", %{path: path, control: control} do
      state = %State{
        epoch: 2,
        path: path,
        recovery_control: control,
        recovery_authority: %{generation: 2, recovery_id: "newer"}
      }

      assert {:error, :newer_epoch_exists} = Locking.lock_for_recovery(state, @authority)
    end

    test "locks for recovery with a valid durable grant", %{path: path, control: control} do
      state = %State{epoch: nil, mode: :ready, path: path, recovery_control: control}

      assert {:ok, %State{mode: :locked, epoch: 1, director: nil, recovery_authority: @authority}} =
               Locking.lock_for_recovery(state, @authority)
    end
  end
end
