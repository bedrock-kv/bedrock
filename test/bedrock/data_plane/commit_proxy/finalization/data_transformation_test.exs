defmodule Bedrock.DataPlane.CommitProxy.FinalizationDataTransformationTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.CommitProxy.Batch
  alias Bedrock.DataPlane.CommitProxy.Finalization
  alias Bedrock.DataPlane.Transaction

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
      storage_teams = [
        %{tag: 0, key_range: {<<>>, <<"m">>}, storage_ids: ["storage_1"]},
        %{tag: 1, key_range: {<<"m">>, <<"z">>}, storage_ids: ["storage_2"]},
        %{tag: 2, key_range: {<<"z">>, :end}, storage_ids: ["storage_3"]}
      ]

      %{storage_teams: storage_teams}
    end

    test "maps single key to tags", %{storage_teams: storage_teams} do
      assert {:ok, [0]} = Finalization.key_or_range_to_tags(<<"hello">>, storage_teams)
      assert {:ok, [1]} = Finalization.key_or_range_to_tags(<<"orange">>, storage_teams)
      assert {:ok, [2]} = Finalization.key_or_range_to_tags(<<"zebra">>, storage_teams)
    end

    test "maps range to intersecting tags", %{storage_teams: storage_teams} do
      range = {<<"a">>, <<"s">>}
      assert {:ok, tags} = Finalization.key_or_range_to_tags(range, storage_teams)
      assert Enum.sort(tags) == [0, 1]
    end

    test "maps range that spans all storage teams", %{storage_teams: storage_teams} do
      range = {<<>>, :end}
      assert {:ok, tags} = Finalization.key_or_range_to_tags(range, storage_teams)
      assert Enum.sort(tags) == [0, 1, 2]
    end

    test "maps range within single storage team", %{storage_teams: storage_teams} do
      range = {<<"n">>, <<"p">>}
      assert {:ok, tags} = Finalization.key_or_range_to_tags(range, storage_teams)
      assert tags == [1]
    end

    test "handles overlapping storage teams" do
      storage_teams = [
        %{tag: 0, key_range: {<<"a">>, <<"m">>}, storage_ids: ["storage_1"]},
        %{tag: 1, key_range: {<<"h">>, <<"z">>}, storage_ids: ["storage_2"]}
      ]

      assert {:ok, tags} = Finalization.key_or_range_to_tags(<<"hello">>, storage_teams)
      assert Enum.sort(tags) == [0, 1]

      range = {<<"i">>, <<"j">>}
      assert {:ok, tags} = Finalization.key_or_range_to_tags(range, storage_teams)
      assert Enum.sort(tags) == [0, 1]
    end

    test "maps key to multiple tags when overlapping (binary keys)" do
      storage_teams = [
        %{tag: 0, key_range: {<<>>, <<0xFF>>}, storage_ids: ["storage_1", "storage_2"]},
        %{tag: 1, key_range: {<<0x80>>, :end}, storage_ids: ["storage_3", "storage_4"]},
        %{tag: 2, key_range: {<<0x40>>, <<0xC0>>}, storage_ids: ["storage_5"]}
      ]

      assert {:ok, tags} = Finalization.key_or_range_to_tags(<<0x90>>, storage_teams)
      assert Enum.sort(tags) == [0, 1, 2]

      assert {:ok, tags} = Finalization.key_or_range_to_tags(<<0x50>>, storage_teams)
      assert Enum.sort(tags) == [0, 2]

      assert {:ok, tags} = Finalization.key_or_range_to_tags(<<0x85>>, storage_teams)
      assert Enum.sort(tags) == [0, 1, 2]
    end

    test "returns empty list for key with no matching teams" do
      storage_teams = [
        %{tag: 0, key_range: {<<0x10>>, <<0x20>>}, storage_ids: ["storage_1"]}
      ]

      assert {:ok, []} = Finalization.key_or_range_to_tags(<<0x05>>, storage_teams)
      assert {:ok, []} = Finalization.key_or_range_to_tags(<<0x25>>, storage_teams)
    end

    test "handles boundary conditions correctly" do
      storage_teams = [
        %{tag: 0, key_range: {<<>>, <<0xFF>>}, storage_ids: ["storage_1", "storage_2"]},
        %{tag: 1, key_range: {<<0x80>>, :end}, storage_ids: ["storage_3", "storage_4"]},
        %{tag: 2, key_range: {<<0x40>>, <<0xC0>>}, storage_ids: ["storage_5"]}
      ]

      # Boundary inclusive behavior at key range edges
      assert {:ok, tags} = Finalization.key_or_range_to_tags(<<0x80>>, storage_teams)
      assert Enum.sort(tags) == [0, 1, 2]

      assert {:ok, tags} = Finalization.key_or_range_to_tags(<<0x40>>, storage_teams)
      assert Enum.sort(tags) == [0, 2]
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

      result = Finalization.find_logs_for_tags([1, 2], logs_by_id)
      assert Enum.sort(result) == ["log1", "log2", "log3"]

      result = Finalization.find_logs_for_tags([0], logs_by_id)
      assert result == ["log1"]

      result = Finalization.find_logs_for_tags([4], logs_by_id)
      assert result == ["log4"]
    end

    test "handles empty tag list" do
      logs_by_id = %{
        "log1" => [0, 1],
        "log2" => [1, 2]
      }

      result = Finalization.find_logs_for_tags([], logs_by_id)
      assert result == []
    end

    test "handles tags with no matching logs" do
      logs_by_id = %{
        "log1" => [0, 1],
        "log2" => [1, 2]
      }

      result = Finalization.find_logs_for_tags([5, 6], logs_by_id)
      assert result == []
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

      result = Finalization.create_finalization_plan(batch, layout)

      assert result.stage == :ready_for_resolution
      assert map_size(result.transactions) == 2

      # Extract conflict sections from the transactions (simulating what resolve_conflicts does)
      conflict_binaries =
        Enum.map(0..1, fn idx ->
          {_idx, _reply_fn, binary} = Map.fetch!(result.transactions, idx)
          Transaction.extract_sections!(binary, [:read_conflicts, :write_conflicts])
        end)

      assert Enum.all?(conflict_binaries, &is_binary/1)

      # Verify the first binary contains both read and write conflicts
      [conflict_binary1, conflict_binary2] = conflict_binaries

      assert {:ok, read_version1} = Transaction.extract_read_version(conflict_binary1)
      assert read_version1 == Bedrock.DataPlane.Version.from_integer(100)

      assert {:ok, {_version, read_conflicts1}} = Transaction.extract_read_conflicts(conflict_binary1)
      assert read_conflicts1 == [{<<"read_key">>, <<"read_key\0">>}]

      assert {:ok, write_conflicts1} = Transaction.extract_write_conflicts(conflict_binary1)
      assert write_conflicts1 == [{<<"write_key1">>, <<"write_key1\0">>}]

      # Verify the second binary has no read version but has write conflicts
      assert {:ok, read_version2} = Transaction.extract_read_version(conflict_binary2)
      assert read_version2 == nil

      assert {:ok, write_conflicts2} = Transaction.extract_write_conflicts(conflict_binary2)
      assert write_conflicts2 == [{<<"write_key2">>, <<"write_key2\0">>}]
    end

    test "handles empty transaction list" do
      batch = %Batch{
        commit_version: Bedrock.DataPlane.Version.from_integer(200),
        last_commit_version: Bedrock.DataPlane.Version.from_integer(199),
        n_transactions: 0,
        buffer: []
      }

      layout = %{
        storage_teams: [],
        logs: %{}
      }

      result = Finalization.create_finalization_plan(batch, layout)
      assert map_size(result.transactions) == 0
      assert result.stage == :ready_for_resolution
    end

    test "handles transactions with no reads" do
      transaction_map = %{
        mutations: [{:set, <<"key">>, <<"value">>}],
        write_conflicts: [{<<"key">>, <<"key\0">>}],
        read_conflicts: {nil, []}
      }

      binary_transaction = Transaction.encode(transaction_map)

      batch = %Batch{
        commit_version: Bedrock.DataPlane.Version.from_integer(200),
        last_commit_version: Bedrock.DataPlane.Version.from_integer(199),
        n_transactions: 1,
        # Single transaction, no reversal needed
        buffer: [{0, fn _ -> :ok end, binary_transaction}]
      }

      layout = %{
        storage_teams: [],
        logs: %{}
      }

      result = Finalization.create_finalization_plan(batch, layout)

      assert map_size(result.transactions) == 1
      {_idx, _reply_fn, binary} = Map.fetch!(result.transactions, 0)
      conflict_binary = Transaction.extract_sections!(binary, [:read_conflicts, :write_conflicts])
      assert is_binary(conflict_binary)

      # Should have no read version but have write conflicts
      assert {:ok, read_version} = Transaction.extract_read_version(conflict_binary)
      assert read_version == nil

      assert {:ok, write_conflicts} = Transaction.extract_write_conflicts(conflict_binary)
      assert write_conflicts == [{<<"key">>, <<"key\0">>}]
    end

    test "handles transactions with no writes" do
      transaction_map = %{
        mutations: [],
        write_conflicts: [],
        read_conflicts: {Bedrock.DataPlane.Version.from_integer(100), [{<<"read_key">>, <<"read_key\0">>}]}
      }

      binary_transaction = Transaction.encode(transaction_map)

      batch = %Batch{
        commit_version: Bedrock.DataPlane.Version.from_integer(200),
        last_commit_version: Bedrock.DataPlane.Version.from_integer(199),
        n_transactions: 1,
        # Single transaction, no reversal needed
        buffer: [{0, fn _ -> :ok end, binary_transaction}]
      }

      layout = %{
        storage_teams: [],
        logs: %{}
      }

      result = Finalization.create_finalization_plan(batch, layout)

      assert map_size(result.transactions) == 1
      {_idx, _reply_fn, binary} = Map.fetch!(result.transactions, 0)
      conflict_binary = Transaction.extract_sections!(binary, [:read_conflicts, :write_conflicts])
      assert is_binary(conflict_binary)

      # Should have read version and read conflicts but no write conflicts
      assert {:ok, read_version} = Transaction.extract_read_version(conflict_binary)
      assert read_version == Bedrock.DataPlane.Version.from_integer(100)

      assert {:ok, {_version, read_conflicts}} = Transaction.extract_read_conflicts(conflict_binary)
      assert read_conflicts == [{<<"read_key">>, <<"read_key\0">>}]

      assert {:ok, write_conflicts} = Transaction.extract_write_conflicts(conflict_binary)
      assert write_conflicts == []
    end

    test "extracts write conflicts in consistent order" do
      transaction_map = %{
        mutations: [
          {:set, <<"z_key">>, <<"value1">>},
          {:set, <<"a_key">>, <<"value2">>},
          {:set, <<"m_key">>, <<"value3">>}
        ],
        write_conflicts: [
          {<<"z_key">>, <<"z_key\0">>},
          {<<"a_key">>, <<"a_key\0">>},
          {<<"m_key">>, <<"m_key\0">>}
        ],
        read_conflicts: {nil, []}
      }

      binary_transaction = Transaction.encode(transaction_map)

      batch = %Batch{
        commit_version: Bedrock.DataPlane.Version.from_integer(200),
        last_commit_version: Bedrock.DataPlane.Version.from_integer(199),
        n_transactions: 1,
        # Single transaction, no reversal needed
        buffer: [{0, fn _ -> :ok end, binary_transaction}]
      }

      layout = %{
        storage_teams: [],
        logs: %{}
      }

      result = Finalization.create_finalization_plan(batch, layout)

      assert map_size(result.transactions) == 1
      {_idx, _reply_fn, binary} = Map.fetch!(result.transactions, 0)
      conflict_binary = Transaction.extract_sections!(binary, [:read_conflicts, :write_conflicts])
      assert is_binary(conflict_binary)

      # Write conflicts must maintain transaction ordering for consistency
      assert {:ok, write_conflicts} = Transaction.extract_write_conflicts(conflict_binary)

      expected_conflicts = [
        {<<"z_key">>, <<"z_key\0">>},
        {<<"a_key">>, <<"a_key\0">>},
        {<<"m_key">>, <<"m_key\0">>}
      ]

      assert write_conflicts == expected_conflicts
    end
  end

  describe "transaction ordering" do
    test "maintains transaction order in resolver data" do
      # Create transactions with identifiable keys to verify ordering
      transactions = [
        {fn _ -> :ok end,
         Transaction.encode(%{
           mutations: [{:set, <<"key_1">>, <<"val_1">>}],
           write_conflicts: [{<<"key_1">>, <<"key_1\0">>}],
           read_conflicts: {nil, []}
         })},
        {fn _ -> :ok end,
         Transaction.encode(%{
           mutations: [{:set, <<"key_2">>, <<"val_2">>}],
           write_conflicts: [{<<"key_2">>, <<"key_2\0">>}],
           read_conflicts: {nil, []}
         })},
        {fn _ -> :ok end,
         Transaction.encode(%{
           mutations: [{:set, <<"key_3">>, <<"val_3">>}],
           write_conflicts: [{<<"key_3">>, <<"key_3\0">>}],
           read_conflicts: {nil, []}
         })},
        {fn _ -> :ok end,
         Transaction.encode(%{
           mutations: [{:set, <<"key_4">>, <<"val_4">>}],
           write_conflicts: [{<<"key_4">>, <<"key_4\0">>}],
           read_conflicts: {nil, []}
         })}
      ]

      # Create batch and finalization plan (simulating normal flow)
      # Buffer needs to be in reverse order because transactions_in_order expects it
      # (transactions are prepended during normal operation)
      batch = %Batch{
        commit_version: Bedrock.DataPlane.Version.from_integer(100),
        last_commit_version: Bedrock.DataPlane.Version.from_integer(99),
        n_transactions: 4,
        buffer:
          transactions
          |> Enum.with_index()
          |> Enum.reverse()
          |> Enum.map(fn {{reply_fn, tx}, idx} -> {idx, reply_fn, tx} end)
      }

      layout = %{
        storage_teams: [],
        logs: %{}
      }

      plan = Finalization.create_finalization_plan(batch, layout)

      # Verify transactions map contains all transactions
      assert map_size(plan.transactions) == 4

      # Extract write conflicts from each transaction in order and verify order
      write_conflicts_by_position =
        for idx <- 0..3 do
          {_idx, _reply_fn, binary} = Map.fetch!(plan.transactions, idx)
          conflict_binary = Transaction.extract_sections!(binary, [:read_conflicts, :write_conflicts])
          {:ok, write_conflicts} = Transaction.extract_write_conflicts(conflict_binary)
          {idx, write_conflicts}
        end

      # Verify the entire list maintains correct order
      expected_order = [
        {0, [{<<"key_1">>, <<"key_1\0">>}]},
        {1, [{<<"key_2">>, <<"key_2\0">>}]},
        {2, [{<<"key_3">>, <<"key_3\0">>}]},
        {3, [{<<"key_4">>, <<"key_4\0">>}]}
      ]

      assert write_conflicts_by_position == expected_order
    end

    test "transaction indices match resolver data positions" do
      # Verify that transaction index N corresponds to resolver_data[N]
      transactions = [
        {fn _ -> :ok end,
         Transaction.encode(%{
           mutations: [{:set, <<"tx_0">>, <<"value">>}],
           write_conflicts: [{<<"tx_0">>, <<"tx_0\0">>}],
           read_conflicts: {nil, []}
         })},
        {fn _ -> :ok end,
         Transaction.encode(%{
           mutations: [{:set, <<"tx_1">>, <<"value">>}],
           write_conflicts: [{<<"tx_1">>, <<"tx_1\0">>}],
           read_conflicts: {nil, []}
         })},
        {fn _ -> :ok end,
         Transaction.encode(%{
           mutations: [{:set, <<"tx_2">>, <<"value">>}],
           write_conflicts: [{<<"tx_2">>, <<"tx_2\0">>}],
           read_conflicts: {nil, []}
         })}
      ]

      batch = %Batch{
        commit_version: Bedrock.DataPlane.Version.from_integer(100),
        last_commit_version: Bedrock.DataPlane.Version.from_integer(99),
        n_transactions: 3,
        # Buffer expects reverse order - add indices
        buffer:
          transactions
          |> Enum.with_index()
          |> Enum.reverse()
          |> Enum.map(fn {{reply_fn, tx}, idx} -> {idx, reply_fn, tx} end)
      }

      layout = %{
        storage_teams: [],
        logs: %{}
      }

      plan = Finalization.create_finalization_plan(batch, layout)

      # For each transaction index, verify the corresponding transaction data
      Enum.each(0..2, fn idx ->
        {_idx, _reply_fn, binary} = Map.fetch!(plan.transactions, idx)
        conflict_binary = Transaction.extract_sections!(binary, [:read_conflicts, :write_conflicts])
        {:ok, write_conflicts} = Transaction.extract_write_conflicts(conflict_binary)
        expected_key = "tx_#{idx}"
        assert [{^expected_key, _}] = write_conflicts, "Transaction #{idx} should be at position #{idx}"
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
      storage_teams = [
        %{tag: 0, key_range: {<<"a">>, <<"m">>}, storage_ids: ["storage_1"]},
        %{tag: 1, key_range: {<<"h">>, <<"z">>}, storage_ids: ["storage_2"]}
      ]

      assert {:ok, tags} = Finalization.key_or_range_to_tags(<<"hello">>, storage_teams)
      assert Enum.sort(tags) == [0, 1]

      assert {:ok, tags} = Finalization.key_or_range_to_tags(<<"india">>, storage_teams)
      assert Enum.sort(tags) == [0, 1]

      assert {:ok, tags} = Finalization.key_or_range_to_tags(<<"apple">>, storage_teams)
      assert tags == [0]
    end
  end
end
