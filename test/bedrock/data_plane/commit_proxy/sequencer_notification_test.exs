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
            {:"$gen_call", from, {:report_successful_commit, version}} ->
              GenServer.reply(from, :ok)
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

    test "sequencer notification uses call so invalid refs return error" do
      # This test documents the current behavior - GenServer.call returns {:error, :unavailable}
      # for invalid refs, so sequencer notification returns an error but doesn't crash the commit proxy.
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

      # GenServer.call returns error for invalid refs, so this returns error
      result = Finalization.finalize_batch(batch, transaction_system_layout, opts)
      assert {:error, :unavailable} = result
    end
  end
end
