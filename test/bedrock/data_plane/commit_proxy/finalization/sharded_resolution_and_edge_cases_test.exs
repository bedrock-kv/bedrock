defmodule Bedrock.DataPlane.CommitProxy.FinalizationShardedResolutionAndEdgeCasesTest do
  @moduledoc """
  Behavioral tests for finalization edge cases:

  - Sharded (multi-resolver) conflict resolution: empty batches, abort merging,
    resolver errors, and resolver task exits
  - Transactions without a mutations section
  - Log preparation failures for keys outside shard coverage
  - Cross-shard clear_range clamping and atomic mutation routing
  - Log push accounting for task exits and missing log services
  - Reply helpers that carry the commit version to clients
  """
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.CommitProxy.Batch
  alias Bedrock.DataPlane.CommitProxy.Finalization
  alias Bedrock.DataPlane.CommitProxy.ResolverLayout
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version
  alias Bedrock.Test.DataPlane.FinalizationTestSupport, as: Support

  @commit_version Version.from_integer(100)
  @last_commit_version Version.from_integer(99)

  defp reply_fn(test_pid, tag), do: fn result -> send(test_pid, {tag, result}) end

  defp encode_tx(key, value) do
    Transaction.encode(%{
      mutations: [{:set, key, value}],
      write_conflicts: [{key, key <> <<0>>}],
      read_conflicts: nil
    })
  end

  defp batch_with(transactions) do
    buffer =
      transactions
      |> Enum.with_index()
      # System mode: these batches write \xFF metadata keys, which user-mode
      # commits are rejected for during pipeline validation.
      |> Enum.map(fn {{reply_fn, binary}, idx} -> {idx, reply_fn, binary, :system} end)

    %Batch{
      commit_version: @commit_version,
      last_commit_version: @last_commit_version,
      n_transactions: length(buffer),
      buffer: buffer
    }
  end

  defp empty_batch do
    %Batch{
      commit_version: @commit_version,
      last_commit_version: @last_commit_version,
      n_transactions: 0,
      buffer: []
    }
  end

  defp sharded_layout do
    ResolverLayout.from_layout(%{resolvers: [{"", :resolver_a}, {"m", :resolver_b}]})
  end

  defp single_layout do
    ResolverLayout.from_layout(%{resolvers: [{"", :test_resolver}]})
  end

  defp routing_data(overrides \\ %{}) do
    %{logs: %{"log_1" => [0]}, services: %{"log_1" => %{kind: :log, status: {:up, self()}}}}
    |> Map.merge(overrides)
    |> Support.build_routing_data()
  end

  defp base_opts(resolver_layout, routing_data, overrides) do
    Keyword.merge(
      [
        epoch: 1,
        sequencer: :test_sequencer,
        resolver_layout: resolver_layout,
        metadata_apply_fn: Support.metadata_apply_fn(routing_data),
        batch_log_push_fn: fn _last, _by_log, _commit, _opts -> :ok end,
        sequencer_notify_fn: fn :test_sequencer, _epoch, _commit, _opts -> :ok end
      ],
      overrides
    )
  end

  describe "sharded conflict resolution" do
    test "empty batch sends empty transaction lists to every resolver and commits with no aborts" do
      test_pid = self()

      resolver_fn = fn ref, 1, @last_commit_version, @commit_version, transactions, metadata, _opts ->
        send(test_pid, {:resolved, ref, transactions, metadata})
        {:ok, [], nil}
      end

      opts = base_opts(sharded_layout(), routing_data(), resolver_fn: resolver_fn)

      assert {:ok, 0, 0} = Finalization.finalize_batch(empty_batch(), opts)

      assert_receive {:resolved, :resolver_a, [], []}
      assert_receive {:resolved, :resolver_b, [], []}
    end

    test "merges aborted indices from all resolvers and notifies each aborted client" do
      # tx0 conflicts in resolver_a's range, tx1 in resolver_b's range
      batch =
        batch_with([
          {reply_fn(self(), :tx0), encode_tx("apple", "v0")},
          {reply_fn(self(), :tx1), encode_tx("zebra", "v1")}
        ])

      resolver_fn = fn
        :resolver_a, _epoch, _last, _commit, _txns, _metadata, _opts -> {:ok, [0], nil}
        :resolver_b, _epoch, _last, _commit, _txns, _metadata, _opts -> {:ok, [1], nil}
      end

      opts = base_opts(sharded_layout(), routing_data(), resolver_fn: resolver_fn)

      assert {:ok, 2, 0} = Finalization.finalize_batch(batch, opts)

      assert_receive {:tx0, {:error, :aborted}}
      assert_receive {:tx1, {:error, :aborted}}
    end

    test "commits transactions across resolver shards and replies with the commit version" do
      batch =
        batch_with([
          {reply_fn(self(), :tx0), encode_tx("apple", "v0")},
          {reply_fn(self(), :tx1), encode_tx("zebra", "v1")}
        ])

      resolver_fn = fn _ref, _epoch, _last, _commit, _txns, _metadata, _opts -> {:ok, [], nil} end

      opts = base_opts(sharded_layout(), routing_data(), resolver_fn: resolver_fn)

      assert {:ok, 0, 2} = Finalization.finalize_batch(batch, opts)

      assert_receive {:tx0, {:ok, @commit_version, 0}}
      assert_receive {:tx1, {:ok, @commit_version, 1}}
    end

    test "fails the batch and aborts all pending clients when any resolver returns an error" do
      batch =
        batch_with([
          {reply_fn(self(), :tx0), encode_tx("apple", "v0")},
          {reply_fn(self(), :tx1), encode_tx("zebra", "v1")}
        ])

      resolver_fn = fn
        :resolver_a, _epoch, _last, _commit, _txns, _metadata, _opts -> {:ok, [], nil}
        :resolver_b, _epoch, _last, _commit, _txns, _metadata, _opts -> {:error, :resolver_crashed}
      end

      opts = base_opts(sharded_layout(), routing_data(), resolver_fn: resolver_fn)

      assert {:error, :resolver_crashed} = Finalization.finalize_batch(batch, opts)

      assert_receive {:tx0, {:error, :aborted}}
      assert_receive {:tx1, {:error, :aborted}}
    end

    test "every resolver receives every transaction's metadata mutations" do
      test_pid = self()

      metadata_tx =
        Transaction.encode(%{
          mutations: [{:set, "apple", "v"}, {:set, <<0xFF, "/system/key">>, "meta"}],
          write_conflicts: [{"apple", "apple" <> <<0>>}],
          read_conflicts: nil
        })

      batch = batch_with([{reply_fn(test_pid, :tx0), metadata_tx}])

      resolver_fn = fn ref, _epoch, _last, _commit, _txns, metadata_per_tx, _opts ->
        send(test_pid, {:metadata_at, ref, metadata_per_tx})
        {:ok, [], nil}
      end

      opts = base_opts(sharded_layout(), routing_data(), resolver_fn: resolver_fn)

      assert {:ok, 0, 1} = Finalization.finalize_batch(batch, opts)

      # Both resolvers see the same broadcast metadata, so each can record
      # verdict-carrying entries; the merge ANDs them into the global verdict.
      expected = [[{:set, <<0xFF, "/system/key">>, "meta"}]]
      assert_receive {:metadata_at, :resolver_a, ^expected}
      assert_receive {:metadata_at, :resolver_b, ^expected}
    end

    test "the metadata apply call receives only unanimously-committed mutations" do
      test_pid = self()
      v = &Version.from_integer/1
      meta = [{:set, <<0xFF, "a">>, "1"}]

      # Both resolvers relay the same entry at v(95): resolver_a's slice saw
      # no conflict (committed), resolver_b's did (vetoed). The AND vetoes.
      resolver_fn = fn
        :resolver_a, _epoch, _last, _commit, _txns, _metadata, _opts ->
          {:ok, [], {v.(90), v.(96), [{v.(95), [{meta, true}]}]}}

        :resolver_b, _epoch, _last, _commit, _txns, _metadata, _opts ->
          {:ok, [], {v.(90), v.(96), [{v.(95), [{meta, false}]}]}}
      end

      metadata_apply_fn = fn _version, window ->
        send(test_pid, {:committed_window, window})
        {:ok, routing_data()}
      end

      opts = base_opts(sharded_layout(), routing_data(), resolver_fn: resolver_fn, metadata_apply_fn: metadata_apply_fn)

      assert {:ok, 0, 0} = Finalization.finalize_batch(empty_batch(), opts)

      from90 = v.(90)
      to96 = v.(96)
      assert_receive {:committed_window, {^from90, ^to96, []}}
    end

    test "merged windows claim the weakest coverage and AND verdicts per transaction" do
      test_pid = self()
      v = &Version.from_integer/1
      meta_a = [{:set, <<0xFF, "a">>, "1"}]
      meta_b = [{:set, <<0xFF, "b">>, "2"}]

      # v(95) carries two metadata transactions; the second is vetoed at
      # resolver_b only. resolver_a claims coverage through v(99), resolver_b
      # through v(96): the merged window claims the weakest (min) coverage
      # and truncates entries beyond it.
      resolver_fn = fn
        :resolver_a, _epoch, _last, _commit, _txns, _metadata, _opts ->
          {:ok, [], {v.(90), v.(99), [{v.(95), [{meta_a, true}, {meta_b, true}]}, {v.(98), [{meta_b, true}]}]}}

        :resolver_b, _epoch, _last, _commit, _txns, _metadata, _opts ->
          {:ok, [], {v.(90), v.(96), [{v.(95), [{meta_a, true}, {meta_b, false}]}]}}
      end

      metadata_apply_fn = fn _version, window ->
        send(test_pid, {:committed_window, window})
        {:ok, routing_data()}
      end

      opts = base_opts(sharded_layout(), routing_data(), resolver_fn: resolver_fn, metadata_apply_fn: metadata_apply_fn)

      assert {:ok, 0, 0} = Finalization.finalize_batch(empty_batch(), opts)

      from90 = v.(90)
      to96 = v.(96)
      v95 = v.(95)
      assert_receive {:committed_window, {^from90, ^to96, [{^v95, ^meta_a}]}}
    end

    test "fails the batch when a resolver task exits" do
      exiting_async_stream_fn = fn _enumerable, _fun, _opts -> [{:exit, :killed}] end

      opts = base_opts(sharded_layout(), routing_data(), async_stream_fn: exiting_async_stream_fn)

      assert {:error, {:resolver_exit, :killed}} = Finalization.finalize_batch(empty_batch(), opts)
    end
  end

  describe "single-resolver empty batch" do
    test "resolver error on an empty batch fails the plan" do
      resolver_fn = fn :test_resolver, _epoch, _last, _commit, [], [], _opts -> {:error, :internal_error} end

      opts = base_opts(single_layout(), routing_data(), resolver_fn: resolver_fn)

      assert {:error, :internal_error} = Finalization.finalize_batch(empty_batch(), opts)
    end
  end

  describe "transactions without a mutations section" do
    test "commit succeeds and the client receives the commit version" do
      # Encoded with conflicts only - no mutations section at all
      no_mutations_tx =
        Transaction.encode(%{write_conflicts: [{"k", "k\0"}], read_conflicts: nil})

      batch = batch_with([{reply_fn(self(), :tx0), no_mutations_tx}])

      resolver_fn = fn _ref, _epoch, _last, _commit, _txns, _metadata, _opts -> {:ok, [], nil} end

      opts = base_opts(single_layout(), routing_data(), resolver_fn: resolver_fn)

      assert {:ok, 0, 1} = Finalization.finalize_batch(batch, opts)
      assert_receive {:tx0, {:ok, @commit_version, 0}}
    end
  end

  describe "keys outside shard coverage" do
    test "an out-of-bounds clear_range is rejected per-transaction, not fatal to the batch" do
      # A range starting at the end of the keyspace is out of the commit's
      # legal write range: pipeline validation rejects just this transaction
      # with its specific error. (This used to fail the whole batch - and
      # before that, crash the finalization task.)
      hostile_tx =
        Transaction.encode(%{
          mutations: [{:clear_range, <<0xFF, 0xFF>>, <<0xFF, 0xFF, 0>>}],
          write_conflicts: [{<<0xFF, 0xFF>>, <<0xFF, 0xFF, 0>>}],
          read_conflicts: nil
        })

      batch = batch_with([{reply_fn(self(), :tx0), hostile_tx}])

      resolver_fn = fn _ref, _epoch, _last, _commit, _txns, _metadata, _opts -> {:ok, [], nil} end

      opts = base_opts(single_layout(), routing_data(), resolver_fn: resolver_fn)

      assert {:ok, 1, 0} = Finalization.finalize_batch(batch, opts)

      assert_receive {:tx0, {:error, {:key_out_of_range, <<0xFF, 0xFF>>}}}
    end

    test "an in-bounds clear_range no shard covers fails the batch with a coverage error" do
      # The shard map covers only ["", "m"): the range ["x", "z") is legal to
      # write but no shard owns it - a map/keyspace divergence the batch must
      # not paper over.
      hostile_tx =
        Transaction.encode(%{
          mutations: [{:clear_range, "x", "z"}],
          write_conflicts: [{"x", "z"}],
          read_conflicts: nil
        })

      batch = batch_with([{reply_fn(self(), :tx0), hostile_tx}])

      resolver_fn = fn _ref, _epoch, _last, _commit, _txns, _metadata, _opts -> {:ok, [], nil} end

      opts =
        base_opts(single_layout(), routing_data(%{shard_layout: %{"m" => {0, ""}}}), resolver_fn: resolver_fn)

      assert {:error, {:storage_team_coverage_error, {"x", "z"}}} = Finalization.finalize_batch(batch, opts)

      assert_receive {:tx0, {:error, :aborted}}
    end
  end

  describe "cross-shard mutation splitting" do
    test "clear_range spanning two shards is split and clamped to shard boundaries" do
      routing_data =
        routing_data(%{shard_layout: %{"m" => {0, ""}, <<0xFF, 0xFF>> => {1, "m"}}})

      spanning_tx =
        Transaction.encode(%{
          mutations: [{:clear_range, "apple", "tiger"}],
          write_conflicts: [{"apple", "tiger"}],
          read_conflicts: nil
        })

      batch = batch_with([{reply_fn(self(), :tx0), spanning_tx}])
      test_pid = self()

      resolver_fn = fn _ref, _epoch, _last, _commit, _txns, _metadata, _opts -> {:ok, [], nil} end

      log_push_fn = fn _last, transactions_by_log, _commit, _opts ->
        send(test_pid, {:pushed, transactions_by_log})
        :ok
      end

      opts =
        base_opts(single_layout(), routing_data, resolver_fn: resolver_fn, batch_log_push_fn: log_push_fn)

      assert {:ok, 0, 1} = Finalization.finalize_batch(batch, opts)

      assert_receive {:pushed, %{"log_1" => encoded}}

      assert Enum.to_list(Transaction.mutations!(encoded)) == [
               {:clear_range, "apple", "m"},
               {:clear_range, "m", "tiger"}
             ]
    end

    test "atomic mutations are routed by key, including through non-integer shard tags" do
      # String shard tags exercise the phash2 tag-hashing fallback
      routing_data =
        routing_data(%{shard_layout: %{<<0xFF, 0xFF>> => {"shard_0", ""}}})

      atomic_tx =
        Transaction.encode(%{
          mutations: [{:atomic, :add, "counter", <<1::64-little>>}],
          write_conflicts: [{"counter", "counter\0"}],
          read_conflicts: nil
        })

      batch = batch_with([{reply_fn(self(), :tx0), atomic_tx}])
      test_pid = self()

      resolver_fn = fn _ref, _epoch, _last, _commit, _txns, _metadata, _opts -> {:ok, [], nil} end

      log_push_fn = fn _last, transactions_by_log, _commit, _opts ->
        send(test_pid, {:pushed, transactions_by_log})
        :ok
      end

      opts =
        base_opts(single_layout(), routing_data, resolver_fn: resolver_fn, batch_log_push_fn: log_push_fn)

      assert {:ok, 0, 1} = Finalization.finalize_batch(batch, opts)

      assert_receive {:pushed, %{"log_1" => encoded}}
      assert Enum.to_list(Transaction.mutations!(encoded)) == [{:atomic, :add, "counter", <<1::64-little>>}]
      assert_receive {:tx0, {:ok, @commit_version, 0}}
    end
  end

  test "mutations are delivered to logs when shard tags are atoms" do
    routing_data = routing_data(%{shard_layout: %{<<0xFF, 0xFF>> => {:shard_a, ""}}})

    batch = batch_with([{reply_fn(self(), :tx0), encode_tx("key", "value")}])
    test_pid = self()

    resolver_fn = fn _ref, _epoch, _last, _commit, _txns, _metadata, _opts -> {:ok, [], nil} end

    log_push_fn = fn _last, transactions_by_log, _commit, _opts ->
      send(test_pid, {:pushed, transactions_by_log})
      :ok
    end

    opts = base_opts(single_layout(), routing_data, resolver_fn: resolver_fn, batch_log_push_fn: log_push_fn)

    assert {:ok, 0, 1} = Finalization.finalize_batch(batch, opts)

    assert_receive {:pushed, %{"log_1" => encoded}}
    assert Enum.to_list(Transaction.mutations!(encoded)) == [{:set, "key", "value"}]
  end

  describe "push_transaction_to_logs_direct/4" do
    test "counts a log task exit as a log failure" do
      async_stream_fn = fn _logs, _fun, _opts -> [{:exit, {"log_1", :timeout}}] end

      assert {:error, {:log_failures, [{"log_1", :timeout}]}} =
               Finalization.push_transaction_to_logs_direct(
                 @last_commit_version,
                 %{"log_1" => "encoded"},
                 @commit_version,
                 log_services: %{"log_1" => self()},
                 async_stream_fn: async_stream_fn
               )
    end

    test "counts a real Task.async_stream exit shape ({:exit, reason}) as a log failure" do
      # Task.async_stream emits {:exit, reason} (e.g. {:exit, :timeout} with
      # on_timeout: :kill_task) - the reason is NOT tagged with the log_id.
      # Regression: this shape used to fall through the reducer and raise.
      async_stream_fn = fn _logs, _fun, _opts -> [{:exit, :timeout}] end

      assert {:error, {:log_failures, [{"log_1", :timeout}]}} =
               Finalization.push_transaction_to_logs_direct(
                 @last_commit_version,
                 %{"log_1" => "encoded"},
                 @commit_version,
                 log_services: %{"log_1" => self()},
                 async_stream_fn: async_stream_fn
               )
    end

    test "fails rather than succeeding vacuously when there are no log services" do
      assert {:error, :log_push_failed} =
               Finalization.push_transaction_to_logs_direct(
                 @last_commit_version,
                 %{},
                 @commit_version,
                 log_services: %{}
               )
    end

    test "pushes to a {name, node} service ref and returns its acknowledgment" do
      log = Support.create_mock_log_server()
      name = :finalization_edge_case_test_log
      Process.register(log, name)
      on_exit(fn -> if Process.whereis(name), do: Process.unregister(name) end)

      assert :ok =
               Finalization.try_to_push_transaction_to_log_direct(
                 {name, node()},
                 "encoded_transaction",
                 @last_commit_version,
                 nil
               )
    end
  end
end
