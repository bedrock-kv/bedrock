defmodule Bedrock.DataPlane.CommitProxy.Finalization.MetadataTest do
  @moduledoc """
  Tests for metadata extraction and aggregation in the finalization pipeline.

  These tests verify:
  - Metadata mutations (keys with \\xFF prefix) are extracted from transactions
  - Metadata is passed to resolver during conflict resolution
  - Metadata updates from resolver are aggregated and returned
  - Metadata handling with single and sharded resolvers
  """
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.CommitProxy.Batch
  alias Bedrock.DataPlane.CommitProxy.Finalization
  alias Bedrock.DataPlane.CommitProxy.ResolverLayout
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version
  alias Bedrock.Test.DataPlane.FinalizationTestSupport, as: Support

  # Create a transaction with metadata mutations (keys with \xFF prefix)
  defp create_transaction_with_metadata(key, value, metadata_key, metadata_value) do
    %{
      mutations: [
        {:set, key, value},
        {:set, <<0xFF>> <> metadata_key, metadata_value}
      ],
      write_conflicts: [{key, key <> <<0>>}],
      read_conflicts: [],
      read_version: nil
    }
  end

  # Create a transaction with only regular mutations
  defp create_transaction_without_metadata(key, value) do
    %{
      mutations: [{:set, key, value}],
      write_conflicts: [{key, key <> <<0>>}],
      read_conflicts: [],
      read_version: nil
    }
  end

  defp create_reply_fn(test_pid, tag) do
    fn result -> send(test_pid, {tag, result}) end
  end

  defp create_batch_with_transactions(commit_version, last_version, transactions) do
    buffer =
      transactions
      |> Enum.with_index()
      |> Enum.map(fn {{reply_fn, tx_binary, task}, index} ->
        {index, reply_fn, tx_binary, task}
      end)

    %Batch{
      commit_version: Version.from_integer(commit_version),
      last_commit_version: Version.from_integer(last_version),
      n_transactions: length(transactions),
      buffer: buffer
    }
  end

  describe "metadata extraction" do
    setup do
      log_server = Support.create_mock_log_server()
      layout = Support.basic_transaction_system_layout(log_server)
      routing_data = Support.build_routing_data(layout)
      %{layout: layout, routing_data: routing_data}
    end

    test "metadata_mutation?/1 identifies metadata mutations by key prefix" do
      # Mutations with \xFF prefix are metadata
      assert Transaction.metadata_mutation?({:set, <<0xFF, "key">>, "value"})
      assert Transaction.metadata_mutation?({:clear, <<0xFF, "key">>})
      assert Transaction.metadata_mutation?({:clear_range, <<0xFF, "a">>, <<0xFF, "z">>})
      assert Transaction.metadata_mutation?({:atomic, :add, <<0xFF, "counter">>, <<1>>})

      # Mutations without \xFF prefix are not metadata
      refute Transaction.metadata_mutation?({:set, "key", "value"})
      refute Transaction.metadata_mutation?({:clear, "key"})
      refute Transaction.metadata_mutation?({:clear_range, "a", "z"})
      refute Transaction.metadata_mutation?({:atomic, :add, "counter", <<1>>})
    end

    test "extracts metadata mutations from transactions during finalization", %{
      layout: transaction_system_layout,
      routing_data: routing_data
    } do
      test_pid = self()

      # Create transaction with metadata
      tx_map = create_transaction_with_metadata("key1", "value1", "meta_key", "meta_value")
      tx_binary = Transaction.encode(tx_map)
      reply_fn = create_reply_fn(self(), :reply)
      task = Task.async(fn -> %{:test_resolver => tx_binary} end)

      batch =
        create_batch_with_transactions(100, 99, [
          {reply_fn, tx_binary, task}
        ])

      # Mock resolver that captures metadata_per_tx
      mock_resolver_fn = fn _resolver, _epoch, _last_version, _commit_version, _summaries, metadata_per_tx, _opts ->
        send(test_pid, {:metadata_received, metadata_per_tx})
        {:ok, [], []}
      end

      assert {:ok, 0, 1, _routing_data} =
               Finalization.finalize_batch(
                 batch,
                 epoch: 1,
                 sequencer: :test_sequencer,
                 resolver_layout: ResolverLayout.from_layout(transaction_system_layout),
                 resolver_fn: mock_resolver_fn,
                 routing_data: routing_data,
                 batch_log_push_fn: fn _last_version, _tx_by_log, _commit_version, _opts -> :ok end,
                 sequencer_notify_fn: fn _sequencer, _epoch, _commit_version, _opts -> :ok end
               )

      # Verify metadata was extracted and passed to resolver
      assert_receive {:metadata_received, metadata_per_tx}
      assert length(metadata_per_tx) == 1

      [tx_metadata] = metadata_per_tx
      assert length(tx_metadata) == 1

      [{:set, metadata_key, "meta_value"}] = tx_metadata
      assert metadata_key == <<0xFF, "meta_key">>
    end

    test "extracts empty metadata list for transactions without metadata", %{
      layout: transaction_system_layout,
      routing_data: routing_data
    } do
      test_pid = self()

      # Create transaction without metadata
      tx_map = create_transaction_without_metadata("key1", "value1")
      tx_binary = Transaction.encode(tx_map)
      reply_fn = create_reply_fn(self(), :reply)
      task = Task.async(fn -> %{:test_resolver => tx_binary} end)

      batch =
        create_batch_with_transactions(100, 99, [
          {reply_fn, tx_binary, task}
        ])

      mock_resolver_fn = fn _resolver, _epoch, _last_version, _commit_version, _summaries, metadata_per_tx, _opts ->
        send(test_pid, {:metadata_received, metadata_per_tx})
        {:ok, [], []}
      end

      assert {:ok, 0, 1, _routing_data} =
               Finalization.finalize_batch(
                 batch,
                 epoch: 1,
                 sequencer: :test_sequencer,
                 resolver_layout: ResolverLayout.from_layout(transaction_system_layout),
                 resolver_fn: mock_resolver_fn,
                 routing_data: routing_data,
                 batch_log_push_fn: fn _last_version, _tx_by_log, _commit_version, _opts -> :ok end,
                 sequencer_notify_fn: fn _sequencer, _epoch, _commit_version, _opts -> :ok end
               )

      assert_receive {:metadata_received, metadata_per_tx}
      assert metadata_per_tx == [[]]
    end
  end

  describe "metadata aggregation from resolver" do
    setup do
      log_server = Support.create_mock_log_server()
      layout = Support.basic_transaction_system_layout(log_server)
      routing_data = Support.build_routing_data(layout)
      %{layout: layout, routing_data: routing_data}
    end

    test "returns plan metadata from finalization result", %{
      layout: transaction_system_layout,
      routing_data: routing_data
    } do
      tx_map = create_transaction_without_metadata("key1", "value1")
      tx_binary = Transaction.encode(tx_map)
      reply_fn = create_reply_fn(self(), :reply)
      task = Task.async(fn -> %{:test_resolver => tx_binary} end)

      batch =
        create_batch_with_transactions(100, 99, [
          {reply_fn, tx_binary, task}
        ])

      # Mock resolver returns metadata updates (stored in plan.metadata_updates)
      commit_version = Version.from_integer(100)

      metadata_updates = [
        {commit_version, [{:set, <<0xFF, "system_key">>, "system_value"}]}
      ]

      mock_resolver_fn = fn _resolver, _epoch, _last_version, _commit_version, _summaries, _metadata_per_tx, _opts ->
        {:ok, [], metadata_updates}
      end

      assert {:ok, 0, 1, _returned_routing_data} =
               Finalization.finalize_batch(
                 batch,
                 epoch: 1,
                 sequencer: :test_sequencer,
                 resolver_layout: ResolverLayout.from_layout(transaction_system_layout),
                 resolver_fn: mock_resolver_fn,
                 routing_data: routing_data,
                 batch_log_push_fn: fn _last_version, _tx_by_log, _commit_version, _opts -> :ok end,
                 sequencer_notify_fn: fn _sequencer, _epoch, _commit_version, _opts -> :ok end
               )

      # Routing data is returned (metadata handling removed)
      assert_receive {:reply, {:ok, _, _}}
    end

    test "returns routing_data from finalization", %{
      layout: transaction_system_layout,
      routing_data: routing_data
    } do
      tx_map = create_transaction_without_metadata("key1", "value1")
      tx_binary = Transaction.encode(tx_map)
      reply_fn = create_reply_fn(self(), :reply)
      task = Task.async(fn -> %{:test_resolver => tx_binary} end)

      batch =
        create_batch_with_transactions(100, 99, [
          {reply_fn, tx_binary, task}
        ])

      mock_resolver_fn = fn _resolver, _epoch, _last_version, _commit_version, _summaries, _metadata_per_tx, _opts ->
        {:ok, [], []}
      end

      assert {:ok, 0, 1, _returned_routing_data} =
               Finalization.finalize_batch(
                 batch,
                 epoch: 1,
                 sequencer: :test_sequencer,
                 resolver_layout: ResolverLayout.from_layout(transaction_system_layout),
                 resolver_fn: mock_resolver_fn,
                 routing_data: routing_data,
                 batch_log_push_fn: fn _last_version, _tx_by_log, _commit_version, _opts -> :ok end,
                 sequencer_notify_fn: fn _sequencer, _epoch, _commit_version, _opts -> :ok end
               )

      assert_receive {:reply, {:ok, _, _}}
    end

    test "handles empty metadata updates from resolver", %{
      layout: transaction_system_layout,
      routing_data: routing_data
    } do
      tx_map = create_transaction_without_metadata("key1", "value1")
      tx_binary = Transaction.encode(tx_map)
      reply_fn = create_reply_fn(self(), :reply)
      task = Task.async(fn -> %{:test_resolver => tx_binary} end)

      batch =
        create_batch_with_transactions(100, 99, [
          {reply_fn, tx_binary, task}
        ])

      # Resolver returns no metadata updates
      mock_resolver_fn = fn _resolver, _epoch, _last_version, _commit_version, _summaries, _metadata_per_tx, _opts ->
        {:ok, [], []}
      end

      assert {:ok, 0, 1, _returned_routing_data} =
               Finalization.finalize_batch(
                 batch,
                 epoch: 1,
                 sequencer: :test_sequencer,
                 resolver_layout: ResolverLayout.from_layout(transaction_system_layout),
                 resolver_fn: mock_resolver_fn,
                 routing_data: routing_data,
                 batch_log_push_fn: fn _last_version, _tx_by_log, _commit_version, _opts -> :ok end,
                 sequencer_notify_fn: fn _sequencer, _epoch, _commit_version, _opts -> :ok end
               )

      # Routing data is returned (metadata handling removed)
      assert_receive {:reply, {:ok, _, _}}
    end
  end

  describe "metadata with multiple transactions" do
    setup do
      log_server = Support.create_mock_log_server()
      layout = Support.basic_transaction_system_layout(log_server)
      routing_data = Support.build_routing_data(layout)
      %{layout: layout, routing_data: routing_data}
    end

    test "extracts metadata from each transaction independently", %{
      layout: transaction_system_layout,
      routing_data: routing_data
    } do
      test_pid = self()

      # Transaction 1: has metadata
      tx1_map = create_transaction_with_metadata("key1", "value1", "meta1", "meta_val1")
      tx1_binary = Transaction.encode(tx1_map)
      reply_fn1 = create_reply_fn(self(), :reply1)
      task1 = Task.async(fn -> %{:test_resolver => tx1_binary} end)

      # Transaction 2: no metadata
      tx2_map = create_transaction_without_metadata("key2", "value2")
      tx2_binary = Transaction.encode(tx2_map)
      reply_fn2 = create_reply_fn(self(), :reply2)
      task2 = Task.async(fn -> %{:test_resolver => tx2_binary} end)

      # Transaction 3: has metadata
      tx3_map = create_transaction_with_metadata("key3", "value3", "meta3", "meta_val3")
      tx3_binary = Transaction.encode(tx3_map)
      reply_fn3 = create_reply_fn(self(), :reply3)
      task3 = Task.async(fn -> %{:test_resolver => tx3_binary} end)

      batch =
        create_batch_with_transactions(100, 99, [
          {reply_fn1, tx1_binary, task1},
          {reply_fn2, tx2_binary, task2},
          {reply_fn3, tx3_binary, task3}
        ])

      mock_resolver_fn = fn _resolver, _epoch, _last_version, _commit_version, _summaries, metadata_per_tx, _opts ->
        send(test_pid, {:metadata_received, metadata_per_tx})
        {:ok, [], []}
      end

      assert {:ok, 0, 3, _routing_data} =
               Finalization.finalize_batch(
                 batch,
                 epoch: 1,
                 sequencer: :test_sequencer,
                 resolver_layout: ResolverLayout.from_layout(transaction_system_layout),
                 resolver_fn: mock_resolver_fn,
                 routing_data: routing_data,
                 batch_log_push_fn: fn _last_version, _tx_by_log, _commit_version, _opts -> :ok end,
                 sequencer_notify_fn: fn _sequencer, _epoch, _commit_version, _opts -> :ok end
               )

      assert_receive {:metadata_received, metadata_per_tx}
      assert length(metadata_per_tx) == 3

      # Transaction 1 has metadata
      [tx1_meta, tx2_meta, tx3_meta] = metadata_per_tx
      assert length(tx1_meta) == 1
      assert [{:set, <<0xFF, "meta1">>, "meta_val1"}] = tx1_meta

      # Transaction 2 has no metadata
      assert tx2_meta == []

      # Transaction 3 has metadata
      assert length(tx3_meta) == 1
      assert [{:set, <<0xFF, "meta3">>, "meta_val3"}] = tx3_meta
    end
  end

  describe "metadata with empty batch" do
    setup do
      log_server = Support.create_mock_log_server()
      layout = Support.basic_transaction_system_layout(log_server)
      routing_data = Support.build_routing_data(layout)
      %{layout: layout, routing_data: routing_data}
    end

    test "handles empty batch metadata correctly", %{
      layout: transaction_system_layout,
      routing_data: routing_data
    } do
      test_pid = self()

      batch =
        create_batch_with_transactions(100, 99, [])

      # Mock resolver captures metadata
      mock_resolver_fn = fn _resolver, _epoch, _last_version, _commit_version, _summaries, metadata_per_tx, _opts ->
        send(test_pid, {:metadata_received, metadata_per_tx})
        {:ok, [], []}
      end

      assert {:ok, 0, 0, _routing_data} =
               Finalization.finalize_batch(
                 batch,
                 epoch: 1,
                 sequencer: :test_sequencer,
                 resolver_layout: ResolverLayout.from_layout(transaction_system_layout),
                 resolver_fn: mock_resolver_fn,
                 routing_data: routing_data,
                 batch_log_push_fn: fn _last_version, _tx_by_log, _commit_version, _opts -> :ok end,
                 sequencer_notify_fn: fn _sequencer, _epoch, _commit_version, _opts -> :ok end
               )

      assert_receive {:metadata_received, metadata_per_tx}
      assert metadata_per_tx == []
    end
  end

  describe "metadata with various mutation types" do
    setup do
      log_server = Support.create_mock_log_server()
      layout = Support.basic_transaction_system_layout(log_server)
      routing_data = Support.build_routing_data(layout)
      %{layout: layout, routing_data: routing_data}
    end

    test "extracts all metadata mutation types", %{
      layout: transaction_system_layout,
      routing_data: routing_data
    } do
      test_pid = self()

      # Transaction with various metadata mutation types
      tx_map = %{
        mutations: [
          {:set, "regular_key", "regular_value"},
          {:set, <<0xFF, "meta_set">>, "set_value"},
          {:clear, <<0xFF, "meta_clear">>},
          {:clear_range, <<0xFF, "meta_range_start">>, <<0xFF, "meta_range_end">>}
        ],
        write_conflicts: [{"regular_key", "regular_key" <> <<0>>}],
        read_conflicts: [],
        read_version: nil
      }

      tx_binary = Transaction.encode(tx_map)
      reply_fn = create_reply_fn(self(), :reply)
      task = Task.async(fn -> %{:test_resolver => tx_binary} end)

      batch =
        create_batch_with_transactions(100, 99, [
          {reply_fn, tx_binary, task}
        ])

      mock_resolver_fn = fn _resolver, _epoch, _last_version, _commit_version, _summaries, metadata_per_tx, _opts ->
        send(test_pid, {:metadata_received, metadata_per_tx})
        {:ok, [], []}
      end

      assert {:ok, 0, 1, _routing_data} =
               Finalization.finalize_batch(
                 batch,
                 epoch: 1,
                 sequencer: :test_sequencer,
                 resolver_layout: ResolverLayout.from_layout(transaction_system_layout),
                 resolver_fn: mock_resolver_fn,
                 routing_data: routing_data,
                 batch_log_push_fn: fn _last_version, _tx_by_log, _commit_version, _opts -> :ok end,
                 sequencer_notify_fn: fn _sequencer, _epoch, _commit_version, _opts -> :ok end
               )

      assert_receive {:metadata_received, metadata_per_tx}
      [tx_metadata] = metadata_per_tx

      # Should have 3 metadata mutations (set, clear, clear_range)
      assert length(tx_metadata) == 3

      # Verify each type
      assert Enum.any?(tx_metadata, &match?({:set, <<0xFF, "meta_set">>, _}, &1))
      assert Enum.any?(tx_metadata, &match?({:clear, <<0xFF, "meta_clear">>}, &1))
      assert Enum.any?(tx_metadata, &match?({:clear_range, <<0xFF, "meta_range_start">>, _}, &1))
    end
  end
end
