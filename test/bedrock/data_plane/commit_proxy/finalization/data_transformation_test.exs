defmodule Bedrock.DataPlane.CommitProxy.FinalizationDataTransformationTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.CommitProxy.Batch
  alias Bedrock.DataPlane.CommitProxy.Finalization
  alias Bedrock.DataPlane.CommitProxy.RoutingData
  alias Bedrock.DataPlane.Transaction

  # Helper functions for creating test data
  defp create_storage_teams do
    [
      %{tag: 0, key_range: {<<>>, <<"m">>}, storage_ids: ["storage_1"]},
      %{tag: 1, key_range: {<<"m">>, <<"z">>}, storage_ids: ["storage_2"]},
      %{tag: 2, key_range: {<<"z">>, <<0xFF, 0xFF>>}, storage_ids: ["storage_3"]}
    ]
  end

  defp create_overlapping_storage_teams do
    [
      %{tag: 0, key_range: {<<"a">>, <<"m">>}, storage_ids: ["storage_1"]},
      %{tag: 1, key_range: {<<"h">>, <<"z">>}, storage_ids: ["storage_2"]}
    ]
  end

  defp create_binary_storage_teams do
    [
      %{tag: 0, key_range: {<<>>, <<0xFF>>}, storage_ids: ["storage_1", "storage_2"]},
      %{tag: 1, key_range: {<<0x80>>, <<0xFF, 0xFF>>}, storage_ids: ["storage_3", "storage_4"]},
      %{tag: 2, key_range: {<<0x40>>, <<0xC0>>}, storage_ids: ["storage_5"]}
    ]
  end

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
  defp build_routing_data(storage_teams, logs) do
    table = :ets.new(:test_shard_keys, [:ordered_set, :public])

    Enum.each(storage_teams, fn team ->
      {_start_key, end_key} = team.key_range
      :ets.insert(table, {end_key, team.tag})
    end)

    log_map =
      logs
      |> Map.keys()
      |> Enum.sort()
      |> Enum.with_index()
      |> Map.new(fn {log_id, index} -> {index, log_id} end)

    replication_factor =
      case storage_teams do
        [] -> max(1, map_size(logs))
        [first | _] -> max(1, length(first.storage_ids))
      end

    %RoutingData{shard_table: table, log_map: log_map, replication_factor: replication_factor}
  end

  defp default_routing_data do
    build_routing_data([], %{})
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

  describe "key_or_range_to_tags/2" do
    setup do
      %{storage_teams: create_storage_teams()}
    end

    test "maps single key to tags", %{storage_teams: storage_teams} do
      assert {:ok, [0]} = Finalization.key_or_range_to_tags(<<"hello">>, storage_teams)
      assert {:ok, [1]} = Finalization.key_or_range_to_tags(<<"orange">>, storage_teams)
      assert {:ok, [2]} = Finalization.key_or_range_to_tags(<<"zebra">>, storage_teams)
    end

    test "maps range to intersecting tags", %{storage_teams: storage_teams} do
      range = {<<"a">>, <<"s">>}
      assert {:ok, tags} = Finalization.key_or_range_to_tags(range, storage_teams)
      assert [0, 1] = Enum.sort(tags)
    end

    test "maps range that spans all storage teams", %{storage_teams: storage_teams} do
      range = {<<>>, <<0xFF, 0xFF>>}
      assert {:ok, tags} = Finalization.key_or_range_to_tags(range, storage_teams)
      assert [0, 1, 2] = Enum.sort(tags)
    end

    test "maps range within single storage team", %{storage_teams: storage_teams} do
      range = {<<"n">>, <<"p">>}
      assert {:ok, tags} = Finalization.key_or_range_to_tags(range, storage_teams)
      assert tags == [1]
    end

    test "handles overlapping storage teams" do
      storage_teams = create_overlapping_storage_teams()

      assert {:ok, tags} = Finalization.key_or_range_to_tags(<<"hello">>, storage_teams)
      assert [0, 1] = Enum.sort(tags)

      range = {<<"i">>, <<"j">>}
      assert {:ok, tags} = Finalization.key_or_range_to_tags(range, storage_teams)
      assert [0, 1] = Enum.sort(tags)
    end

    test "maps key to multiple tags when overlapping (binary keys)" do
      storage_teams = create_binary_storage_teams()

      assert {:ok, tags} = Finalization.key_or_range_to_tags(<<0x90>>, storage_teams)
      assert [0, 1, 2] = Enum.sort(tags)

      assert {:ok, tags} = Finalization.key_or_range_to_tags(<<0x50>>, storage_teams)
      assert [0, 2] = Enum.sort(tags)

      assert {:ok, tags} = Finalization.key_or_range_to_tags(<<0x85>>, storage_teams)
      assert [0, 1, 2] = Enum.sort(tags)
    end

    test "returns empty list for key with no matching teams" do
      storage_teams = [
        %{tag: 0, key_range: {<<0x10>>, <<0x20>>}, storage_ids: ["storage_1"]}
      ]

      assert {:ok, []} = Finalization.key_or_range_to_tags(<<0x05>>, storage_teams)
      assert {:ok, []} = Finalization.key_or_range_to_tags(<<0x25>>, storage_teams)
    end

    test "handles boundary conditions correctly" do
      storage_teams = create_binary_storage_teams()

      # Boundary inclusive behavior at key range edges
      assert {:ok, tags} = Finalization.key_or_range_to_tags(<<0x80>>, storage_teams)
      assert [0, 1, 2] = Enum.sort(tags)

      assert {:ok, tags} = Finalization.key_or_range_to_tags(<<0x40>>, storage_teams)
      assert [0, 2] = Enum.sort(tags)
    end
  end

  describe "find_logs_for_tags/2" do
    test "finds logs that intersect with given tags" do
      logs_by_id = %{
        "log1" => [0, 1],
        "log2" => [1, 2],
        "log3" => [2, 3],
        "log4" => [4]
      }

      assert ["log1", "log2", "log3"] = Enum.sort(Finalization.find_logs_for_tags([1, 2], logs_by_id))
      assert ["log1"] = Finalization.find_logs_for_tags([0], logs_by_id)
      assert ["log4"] = Finalization.find_logs_for_tags([4], logs_by_id)
    end

    test "handles empty tag list" do
      logs_by_id = %{
        "log1" => [0, 1],
        "log2" => [1, 2]
      }

      assert [] = Finalization.find_logs_for_tags([], logs_by_id)
    end

    test "handles tags with no matching logs" do
      logs_by_id = %{
        "log1" => [0, 1],
        "log2" => [1, 2]
      }

      assert [] = Finalization.find_logs_for_tags([5, 6], logs_by_id)
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
        storage_teams: [],
        logs: %{}
      }

      routing_data = build_routing_data(layout.storage_teams, layout.logs)

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

  describe "edge cases and error handling" do
    test "key_or_range_to_tags handles unknown keys by returning empty list" do
      storage_teams = [
        %{tag: 0, key_range: {<<"a">>, <<"m">>}, storage_ids: ["storage_1"]}
      ]

      assert {:ok, []} = Finalization.key_or_range_to_tags(<<"z_unknown">>, storage_teams)
    end

    test "key_or_range_to_tags distributes keys to multiple tags for overlapping teams" do
      storage_teams = create_overlapping_storage_teams()

      assert {:ok, tags} = Finalization.key_or_range_to_tags(<<"hello">>, storage_teams)
      assert [0, 1] = Enum.sort(tags)

      assert {:ok, tags} = Finalization.key_or_range_to_tags(<<"india">>, storage_teams)
      assert [0, 1] = Enum.sort(tags)

      assert {:ok, [0]} = Finalization.key_or_range_to_tags(<<"apple">>, storage_teams)
    end
  end
end
