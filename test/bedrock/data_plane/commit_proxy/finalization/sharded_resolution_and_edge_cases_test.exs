defmodule Bedrock.DataPlane.CommitProxy.FinalizationShardedResolutionAndEdgeCasesTest do
  @moduledoc """
  Behavioral tests for finalization edge cases:

  - Sharded (multi-resolver) conflict resolution: empty batches, abort merging,
    resolver errors, and resolver task exits
  - Transactions without a mutations section
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
      |> Enum.map(fn {{reply_fn, binary}, idx} -> {idx, reply_fn, binary, nil} end)

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
        routing_data: routing_data,
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
        {:ok, [], []}
      end

      opts = base_opts(sharded_layout(), routing_data(), resolver_fn: resolver_fn)

      assert {:ok, 0, 0, _routing_data} = Finalization.finalize_batch(empty_batch(), opts)

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
        :resolver_a, _epoch, _last, _commit, _txns, _metadata, _opts -> {:ok, [0], []}
        :resolver_b, _epoch, _last, _commit, _txns, _metadata, _opts -> {:ok, [1], []}
      end

      opts = base_opts(sharded_layout(), routing_data(), resolver_fn: resolver_fn)

      assert {:ok, 2, 0, _routing_data} = Finalization.finalize_batch(batch, opts)

      assert_receive {:tx0, {:error, :aborted}}
      assert_receive {:tx1, {:error, :aborted}}
    end

    test "commits transactions across resolver shards and replies with the commit version" do
      batch =
        batch_with([
          {reply_fn(self(), :tx0), encode_tx("apple", "v0")},
          {reply_fn(self(), :tx1), encode_tx("zebra", "v1")}
        ])

      resolver_fn = fn _ref, _epoch, _last, _commit, _txns, _metadata, _opts -> {:ok, [], []} end

      opts = base_opts(sharded_layout(), routing_data(), resolver_fn: resolver_fn)

      assert {:ok, 0, 2, _routing_data} = Finalization.finalize_batch(batch, opts)

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
        :resolver_a, _epoch, _last, _commit, _txns, _metadata, _opts -> {:ok, [], []}
        :resolver_b, _epoch, _last, _commit, _txns, _metadata, _opts -> {:error, :resolver_crashed}
      end

      opts = base_opts(sharded_layout(), routing_data(), resolver_fn: resolver_fn)

      assert {:error, :resolver_crashed} = Finalization.finalize_batch(batch, opts)

      assert_receive {:tx0, {:error, :aborted}}
      assert_receive {:tx1, {:error, :aborted}}
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

      resolver_fn = fn _ref, _epoch, _last, _commit, _txns, _metadata, _opts -> {:ok, [], []} end

      opts = base_opts(single_layout(), routing_data(), resolver_fn: resolver_fn)

      assert {:ok, 0, 1, _routing_data} = Finalization.finalize_batch(batch, opts)
      assert_receive {:tx0, {:ok, @commit_version, 0}}
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

      resolver_fn = fn _ref, _epoch, _last, _commit, _txns, _metadata, _opts -> {:ok, [], []} end

      log_push_fn = fn _last, transactions_by_log, _commit, _opts ->
        send(test_pid, {:pushed, transactions_by_log})
        :ok
      end

      opts =
        base_opts(single_layout(), routing_data, resolver_fn: resolver_fn, batch_log_push_fn: log_push_fn)

      assert {:ok, 0, 1, _routing_data} = Finalization.finalize_batch(batch, opts)

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

      resolver_fn = fn _ref, _epoch, _last, _commit, _txns, _metadata, _opts -> {:ok, [], []} end

      log_push_fn = fn _last, transactions_by_log, _commit, _opts ->
        send(test_pid, {:pushed, transactions_by_log})
        :ok
      end

      opts =
        base_opts(single_layout(), routing_data, resolver_fn: resolver_fn, batch_log_push_fn: log_push_fn)

      assert {:ok, 0, 1, _routing_data} = Finalization.finalize_batch(batch, opts)

      assert_receive {:pushed, %{"log_1" => encoded}}
      assert Enum.to_list(Transaction.mutations!(encoded)) == [{:atomic, :add, "counter", <<1::64-little>>}]
      assert_receive {:tx0, {:ok, @commit_version, 0}}
    end
  end

  test "mutations are delivered to logs when shard tags are atoms" do
    routing_data = routing_data(%{shard_layout: %{<<0xFF, 0xFF>> => {:shard_a, ""}}})

    batch = batch_with([{reply_fn(self(), :tx0), encode_tx("key", "value")}])
    test_pid = self()

    resolver_fn = fn _ref, _epoch, _last, _commit, _txns, _metadata, _opts -> {:ok, [], []} end

    log_push_fn = fn _last, transactions_by_log, _commit, _opts ->
      send(test_pid, {:pushed, transactions_by_log})
      :ok
    end

    opts = base_opts(single_layout(), routing_data, resolver_fn: resolver_fn, batch_log_push_fn: log_push_fn)

    assert {:ok, 0, 1, _routing_data} = Finalization.finalize_batch(batch, opts)

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
                 @last_commit_version
               )
    end
  end

  describe "reply and descriptor helpers" do
    test "send_reply_with_commit_version/2 delivers the commit version to every waiting client" do
      test_pid = self()
      oks = [fn result -> send(test_pid, {:client_1, result}) end, fn result -> send(test_pid, {:client_2, result}) end]

      assert :ok = Finalization.send_reply_with_commit_version(oks, @commit_version)

      assert_receive {:client_1, {:ok, @commit_version}}
      assert_receive {:client_2, {:ok, @commit_version}}
    end

    test "mutation_to_key_or_range/1 extracts the key from an atomic mutation" do
      assert Finalization.mutation_to_key_or_range({:atomic, :add, "counter", <<1>>}) == "counter"
    end

    test "resolve_log_descriptors/2 keeps only logs with known service descriptors" do
      descriptor = %{kind: :log, status: {:up, self()}}
      services = %{"log_1" => descriptor}

      assert Finalization.resolve_log_descriptors(%{"log_1" => [0], "log_missing" => [1]}, services) ==
               %{"log_1" => descriptor}
    end
  end
end
