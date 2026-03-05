defmodule Bedrock.DataPlane.CommitProxy.FinalizationDataTransformationTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.CommitProxy.Batch
  alias Bedrock.DataPlane.CommitProxy.Finalization
  alias Bedrock.DataPlane.CommitProxy.RoutingData
  alias Bedrock.DataPlane.Transaction

  # Helper functions for creating test data
  defp create_transaction(mutations, write_conflicts, read_conflicts \\ {nil, []}) do
    %{
      mutations: mutations,
      write_conflicts: write_conflicts,
      read_conflicts: read_conflicts
    }
  end

  defp create_batch(transactions, commit_version) do
    %Batch{
      commit_version: Bedrock.DataPlane.Version.from_integer(commit_version),
      last_commit_version: Bedrock.DataPlane.Version.from_integer(commit_version - 1),
      n_transactions: length(transactions),
      buffer:
        transactions
        |> Enum.with_index()
        |> Enum.reverse()
        |> Enum.map(fn {{reply_fn, tx}, idx} -> {idx, reply_fn, tx} end)
    }
  end

  # Build routing data for tests
  defp build_routing_data(logs) do
    table = :ets.new(:test_shard_keys, [:ordered_set, :public])
    # Default shard layout covering entire keyspace with a single shard (tag 0)
    :ets.insert(table, {<<0xFF, 0xFF>>, 0})

    log_map =
      logs
      |> Map.keys()
      |> Enum.sort()
      |> Enum.with_index()
      |> Map.new(fn {log_id, index} -> {index, log_id} end)

    replication_factor = max(1, map_size(logs))

    %RoutingData{shard_table: table, log_map: log_map, replication_factor: replication_factor}
  end

  defp default_routing_data do
    build_routing_data(%{})
  end

  defp create_ordered_transactions(count) do
    for idx <- 1..count do
      key = "key_#{idx}"
      value = "val_#{idx}"

      {fn _ -> :ok end,
       Transaction.encode(
         create_transaction(
           [{:set, key, value}],
           [{key, key <> "\0"}]
         )
       )}
    end
  end

  describe "mutation_to_key_or_range/1" do
    test "extracts key from set mutation" do
      mutation = {:set, <<"hello">>, <<"world">>}
      assert Finalization.mutation_to_key_or_range(mutation) == <<"hello">>
    end

    test "extracts key from clear mutation" do
      mutation = {:clear, <<"hello">>}
      assert Finalization.mutation_to_key_or_range(mutation) == <<"hello">>
    end

    test "extracts range from clear_range mutation" do
      mutation = {:clear_range, <<"a">>, <<"z">>}
      assert Finalization.mutation_to_key_or_range(mutation) == {<<"a">>, <<"z">>}
    end
  end

  describe "resolver data preparation" do
    test "creates resolver data in correct binary format during plan creation" do
      transaction_map1 = %{
        mutations: [{:set, <<"write_key1">>, <<"write_value1">>}],
        write_conflicts: [{<<"write_key1">>, <<"write_key1\0">>}],
        read_conflicts: {Bedrock.DataPlane.Version.from_integer(100), [{<<"read_key">>, <<"read_key\0">>}]}
      }

      transaction_map2 = %{
        mutations: [{:set, <<"write_key2">>, <<"write_value2">>}],
        write_conflicts: [{<<"write_key2">>, <<"write_key2\0">>}],
        read_conflicts: {nil, []}
      }

      binary_transaction1 = Transaction.encode(transaction_map1)
      binary_transaction2 = Transaction.encode(transaction_map2)

      # Create the plan using the normal flow which pre-populates resolver_data
      batch = %Batch{
        commit_version: Bedrock.DataPlane.Version.from_integer(200),
        last_commit_version: Bedrock.DataPlane.Version.from_integer(199),
        n_transactions: 2,
        buffer:
          Enum.reverse([
            {0, fn _ -> :ok end, binary_transaction1},
            {1, fn _ -> :ok end, binary_transaction2}
          ])
      }

      layout = %{
        logs: %{}
      }

      routing_data = build_routing_data(layout.logs)

      assert %{stage: :ready_for_resolution, transactions: transactions} =
               Finalization.create_finalization_plan(batch, routing_data)

      assert map_size(transactions) == 2

      # Extract conflict sections from the transactions (simulating what resolve_conflicts does)
      conflict_binaries =
        Enum.map(0..1, fn idx ->
          {_idx, _reply_fn, binary} = Map.fetch!(transactions, idx)
          Transaction.extract_sections!(binary, [:read_conflicts, :write_conflicts])
        end)

      assert Enum.all?(conflict_binaries, &is_binary/1)

      # Verify the first binary contains both read and write conflicts
      [conflict_binary1, conflict_binary2] = conflict_binaries

      version_100 = Bedrock.DataPlane.Version.from_integer(100)
      assert {:ok, {^version_100, [{<<"read_key">>, <<"read_key\0">>}]}} = Transaction.read_conflicts(conflict_binary1)

      assert {:ok, [{<<"write_key1">>, <<"write_key1\0">>}]} = Transaction.write_conflicts(conflict_binary1)

      # Verify the second binary has no read version but has write conflicts
      assert {:ok, {nil, _read_conflicts2}} = Transaction.read_conflicts(conflict_binary2)

      assert {:ok, [{<<"write_key2">>, <<"write_key2\0">>}]} = Transaction.write_conflicts(conflict_binary2)
    end

    test "handles empty transaction list" do
      batch = create_batch([], 200)

      assert %{transactions: transactions, stage: :ready_for_resolution} =
               Finalization.create_finalization_plan(batch, default_routing_data())

      assert map_size(transactions) == 0
    end

    test "handles transactions with no reads" do
      transaction_map =
        create_transaction(
          [{:set, <<"key">>, <<"value">>}],
          [{<<"key">>, <<"key\0">>}]
        )

      batch = create_batch([{fn _ -> :ok end, Transaction.encode(transaction_map)}], 200)

      assert %{transactions: transactions} =
               Finalization.create_finalization_plan(batch, default_routing_data())

      assert map_size(transactions) == 1
      {_idx, _reply_fn, binary} = Map.fetch!(transactions, 0)
      conflict_binary = Transaction.extract_sections!(binary, [:read_conflicts, :write_conflicts])
      assert is_binary(conflict_binary)

      # Should have no read version but have write conflicts
      assert {:ok, {nil, _read_conflicts}} = Transaction.read_conflicts(conflict_binary)
      assert {:ok, [{<<"key">>, <<"key\0">>}]} = Transaction.write_conflicts(conflict_binary)
    end

    test "handles transactions with no writes" do
      version_100 = Bedrock.DataPlane.Version.from_integer(100)

      transaction_map =
        create_transaction(
          [],
          [],
          {version_100, [{<<"read_key">>, <<"read_key\0">>}]}
        )

      batch = create_batch([{fn _ -> :ok end, Transaction.encode(transaction_map)}], 200)

      assert %{transactions: transactions} =
               Finalization.create_finalization_plan(batch, default_routing_data())

      assert map_size(transactions) == 1
      {_idx, _reply_fn, binary} = Map.fetch!(transactions, 0)
      conflict_binary = Transaction.extract_sections!(binary, [:read_conflicts, :write_conflicts])
      assert is_binary(conflict_binary)

      # Should have read version and read conflicts but no write conflicts
      version_100 = Bedrock.DataPlane.Version.from_integer(100)
      assert {:ok, {^version_100, [{<<"read_key">>, <<"read_key\0">>}]}} = Transaction.read_conflicts(conflict_binary)
      assert {:ok, []} = Transaction.write_conflicts(conflict_binary)
    end

    test "extracts write conflicts in consistent order" do
      transaction_map =
        create_transaction(
          [
            {:set, <<"z_key">>, <<"value1">>},
            {:set, <<"a_key">>, <<"value2">>},
            {:set, <<"m_key">>, <<"value3">>}
          ],
          [
            {<<"z_key">>, <<"z_key\0">>},
            {<<"a_key">>, <<"a_key\0">>},
            {<<"m_key">>, <<"m_key\0">>}
          ]
        )

      batch = create_batch([{fn _ -> :ok end, Transaction.encode(transaction_map)}], 200)

      assert %{transactions: transactions} =
               Finalization.create_finalization_plan(batch, default_routing_data())

      assert map_size(transactions) == 1
      {_idx, _reply_fn, binary} = Map.fetch!(transactions, 0)
      conflict_binary = Transaction.extract_sections!(binary, [:read_conflicts, :write_conflicts])
      assert is_binary(conflict_binary)

      # Write conflicts must maintain transaction ordering for consistency
      expected_conflicts = [
        {<<"z_key">>, <<"z_key\0">>},
        {<<"a_key">>, <<"a_key\0">>},
        {<<"m_key">>, <<"m_key\0">>}
      ]

      assert {:ok, ^expected_conflicts} = Transaction.write_conflicts(conflict_binary)
    end
  end

  describe "transaction ordering" do
    test "maintains transaction order in resolver data" do
      # Create transactions with identifiable keys to verify ordering
      transactions = create_ordered_transactions(4)
      batch = create_batch(transactions, 100)

      assert %{transactions: transactions} =
               Finalization.create_finalization_plan(batch, default_routing_data())

      # Verify transactions map contains all transactions
      assert map_size(transactions) == 4

      # Extract write conflicts from each transaction in order and verify order
      expected_order = [
        {0, [{<<"key_1">>, <<"key_1\0">>}]},
        {1, [{<<"key_2">>, <<"key_2\0">>}]},
        {2, [{<<"key_3">>, <<"key_3\0">>}]},
        {3, [{<<"key_4">>, <<"key_4\0">>}]}
      ]

      write_conflicts_by_position =
        for idx <- 0..3 do
          {_idx, _reply_fn, binary} = Map.fetch!(transactions, idx)
          conflict_binary = Transaction.extract_sections!(binary, [:read_conflicts, :write_conflicts])
          {:ok, write_conflicts} = Transaction.write_conflicts(conflict_binary)
          {idx, write_conflicts}
        end

      assert ^expected_order = write_conflicts_by_position
    end

    test "transaction indices match resolver data positions" do
      # Verify that transaction index N corresponds to resolver_data[N]
      transactions = [
        {fn _ -> :ok end,
         Transaction.encode(
           create_transaction(
             [{:set, "tx_0", "value"}],
             [{"tx_0", "tx_0\0"}]
           )
         )},
        {fn _ -> :ok end,
         Transaction.encode(
           create_transaction(
             [{:set, "tx_1", "value"}],
             [{"tx_1", "tx_1\0"}]
           )
         )},
        {fn _ -> :ok end,
         Transaction.encode(
           create_transaction(
             [{:set, "tx_2", "value"}],
             [{"tx_2", "tx_2\0"}]
           )
         )}
      ]

      batch = create_batch(transactions, 100)

      assert %{transactions: transactions} =
               Finalization.create_finalization_plan(batch, default_routing_data())

      # For each transaction index, verify the corresponding transaction data
      Enum.each(0..2, fn idx ->
        {_idx, _reply_fn, binary} = Map.fetch!(transactions, idx)
        conflict_binary = Transaction.extract_sections!(binary, [:read_conflicts, :write_conflicts])
        expected_key = "tx_#{idx}"

        assert {:ok, [{^expected_key, _}]} = Transaction.write_conflicts(conflict_binary),
               "Transaction #{idx} should be at position #{idx}"
      end)
    end
  end
end
