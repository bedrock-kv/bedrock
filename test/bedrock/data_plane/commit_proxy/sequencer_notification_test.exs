defmodule Bedrock.DataPlane.CommitProxy.SequencerNotificationTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.CommitProxy.Batch
  alias Bedrock.DataPlane.CommitProxy.Finalization
  alias Bedrock.DataPlane.CommitProxy.ResolverLayout

  # Common test setup
  defp create_batch do
    %Batch{
      commit_version: 100,
      last_commit_version: 99,
      n_transactions: 0,
      buffer: []
    }
  end

  defp create_transaction_system_layout(sequencer) do
    %{
      sequencer: sequencer,
      resolvers: [{"", :test_resolver}],
      logs: %{},
      storage_teams: [],
      services: %{}
    }
  end

  defp create_finalization_opts do
    [
      epoch: 1,
      resolver_layout: %ResolverLayout.Single{resolver_ref: :test_resolver},
      resolver_fn: fn _, _, _, _, _, _ -> {:ok, []} end,
      batch_log_push_fn: fn _, _, _, _, _ -> :ok end
    ]
  end

  defp create_mock_sequencer do
    test_pid = self()

    spawn(fn ->
      receive do
        {:"$gen_call", from, {:report_successful_commit, version}} ->
          GenServer.reply(from, :ok)
          send(test_pid, {:sequencer_notified, version})
      after
        1000 -> :timeout
      end
    end)
  end

  describe "finalize_batch/3" do
    test "notifies sequencer after log persistence" do
      mock_sequencer = create_mock_sequencer()
      batch = create_batch()
      layout = create_transaction_system_layout(mock_sequencer)
      opts = create_finalization_opts()

      assert {:ok, 0, 0} = Finalization.finalize_batch(batch, layout, opts)
      assert_receive {:sequencer_notified, 100}, 100

      Process.exit(mock_sequencer, :kill)
    end

    test "returns error when sequencer ref is invalid" do
      # Documents current behavior: GenServer.call returns {:error, :unavailable}
      # for invalid refs, so sequencer notification returns an error but doesn't crash the commit proxy.
      batch = create_batch()
      layout = create_transaction_system_layout(:invalid_sequencer_ref)
      opts = create_finalization_opts()

      assert {:error, :unavailable} = Finalization.finalize_batch(batch, layout, opts)
    end
  end
end
