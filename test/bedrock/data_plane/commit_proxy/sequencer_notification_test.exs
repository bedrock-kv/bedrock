defmodule Bedrock.DataPlane.CommitProxy.SequencerNotificationTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.CommitProxy.Batch
  alias Bedrock.DataPlane.CommitProxy.Finalization

  describe "sequencer notification" do
    test "finalize_batch notifies sequencer after log persistence" do
      # Create a mock sequencer process that tracks notifications
      test_pid = self()

      mock_sequencer =
        spawn(fn ->
          receive do
            {:"$gen_cast", {:report_successful_commit, version}} ->
              send(test_pid, {:sequencer_notified, version})
          after
            1000 -> :timeout
          end
        end)

      batch = %Batch{
        commit_version: 100,
        last_commit_version: 99,
        n_transactions: 0,
        buffer: []
      }

      transaction_system_layout = %{
        sequencer: mock_sequencer,
        resolvers: [],
        logs: %{},
        storage_teams: [],
        services: %{}
      }

      # Mock all the finalization functions to just return :ok
      opts = [
        epoch: 1,
        resolver_fn: fn _, _, _, _, _, _ -> {:ok, []} end,
        batch_log_push_fn: fn _, _, _, _, _ -> :ok end
      ]

      result = Finalization.finalize_batch(batch, transaction_system_layout, opts)

      assert {:ok, 0, 0} = result

      # The mock sequencer should have received the notification
      assert_receive {:sequencer_notified, 100}, 100

      # Clean up
      Process.exit(mock_sequencer, :kill)
    end

    test "sequencer notification uses cast so invalid refs don't crash immediately" do
      # This test documents the current behavior - GenServer.cast always returns :ok
      # even for invalid refs, so sequencer notification doesn't crash the commit proxy.
      # In a real system, the sequencer would be a valid reference.

      batch = %Batch{
        commit_version: 100,
        last_commit_version: 99,
        n_transactions: 0,
        buffer: []
      }

      transaction_system_layout = %{
        # This would be a valid ref in real usage
        sequencer: :invalid_sequencer_ref,
        resolvers: [],
        logs: %{},
        storage_teams: [],
        services: %{}
      }

      opts = [
        epoch: 1,
        resolver_fn: fn _, _, _, _, _, _ -> {:ok, []} end,
        batch_log_push_fn: fn _, _, _, _, _ -> :ok end
      ]

      # GenServer.cast doesn't fail for invalid refs, so this succeeds
      # The real error handling happens at the OTP supervisor level
      result = Finalization.finalize_batch(batch, transaction_system_layout, opts)
      assert {:ok, 0, 0} = result
    end
  end
end
