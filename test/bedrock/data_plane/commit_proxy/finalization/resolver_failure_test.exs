defmodule Bedrock.DataPlane.CommitProxy.Finalization.ResolverFailureTest do
  @moduledoc """
  A resolver that cannot answer means the epoch is over - there are no
  retries (bedrock-q67.32). FDB never retries a resolver call
  (brokenPromiseToNever): correctness requires every proxy to see an
  identical metadata stream, so a dead resolver is a recovery event, not a
  retry loop. The failed batch aborts its clients and reports
  {:resolver_unavailable, reason}, which stops the proxy into
  Director-driven recovery.
  """
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.CommitProxy.Batch
  alias Bedrock.DataPlane.CommitProxy.Finalization
  alias Bedrock.DataPlane.CommitProxy.ResolverLayout
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version
  alias Bedrock.Test.DataPlane.FinalizationTestSupport, as: Support

  defp batch_with_one_tx(test_pid) do
    tx =
      Transaction.encode(%{
        mutations: [{:set, "key", "value"}],
        write_conflicts: [{"key", "key" <> <<0>>}],
        read_conflicts: nil
      })

    %Batch{
      commit_version: Version.from_integer(100),
      last_commit_version: Version.from_integer(99),
      n_transactions: 1,
      buffer: [{0, fn result -> send(test_pid, {:tx0, result}) end, tx, :user}]
    }
  end

  defp opts(resolver_fn) do
    layout = %{logs: %{"log_1" => [0]}, services: %{"log_1" => %{kind: :log, status: {:up, self()}}}}

    [
      epoch: 1,
      recovery_authority: %{generation: 1, recovery_id: "commit-proxy-test"},
      sequencer: :test_sequencer,
      resolver_layout: %ResolverLayout.Single{resolver_ref: :test_resolver},
      metadata_apply_fn: Support.metadata_apply_fn(Support.build_routing_data(layout)),
      resolver_fn: resolver_fn,
      batch_log_push_fn: fn _last, _by_log, _commit, _opts -> :ok end,
      sequencer_notify_fn: fn _sequencer, _epoch, _commit, _opts -> :ok end
    ]
  end

  for reason <- [:timeout, :unavailable] do
    test "a #{reason} from the resolver fails the batch immediately - no retry" do
      test_pid = self()
      calls = :counters.new(1, [])

      resolver_fn = fn _ref, _epoch, _last, _commit, _txns, _metadata, _opts ->
        :counters.add(calls, 1, 1)
        {:error, unquote(reason)}
      end

      assert {:error, {:resolver_unavailable, unquote(reason)}} =
               Finalization.finalize_batch(batch_with_one_tx(test_pid), opts(resolver_fn))

      assert :counters.get(calls, 1) == 1
      assert_receive {:tx0, {:error, :aborted}}
    end
  end

  test "non-transport resolver errors surface unchanged" do
    test_pid = self()

    resolver_fn = fn _ref, _epoch, _last, _commit, _txns, _metadata, _opts ->
      {:error, {:epoch_mismatch, expected: 2, received: 1}}
    end

    assert {:error, {:epoch_mismatch, _}} =
             Finalization.finalize_batch(batch_with_one_tx(test_pid), opts(resolver_fn))

    assert_receive {:tx0, {:error, :aborted}}
  end
end
