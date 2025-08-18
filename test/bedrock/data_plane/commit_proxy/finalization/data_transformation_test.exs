defmodule Bedrock.DataPlane.CommitProxy.FinalizationDataTransformationTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.CommitProxy.Finalization
  alias Bedrock.DataPlane.Transaction
  alias FinalizationTestSupport, as: Support

  describe "key_to_tags/2" do
    setup do
      storage_teams = [
        %{tag: 0, key_range: {<<>>, <<0xFF>>}, storage_ids: ["storage_1", "storage_2"]},
        %{tag: 1, key_range: {<<0x80>>, :end}, storage_ids: ["storage_3", "storage_4"]},
        %{tag: 2, key_range: {<<0x40>>, <<0xC0>>}, storage_ids: ["storage_5"]}
      ]

      %{storage_teams: storage_teams}
    end

    test "maps key to single tag when no overlap", %{storage_teams: storage_teams} do
      assert {:ok, [0]} = Finalization.key_to_tags(<<0x01>>, storage_teams)
      assert {:ok, [0]} = Finalization.key_to_tags(<<0x3F>>, storage_teams)
    end

    test "maps key to multiple tags when overlapping", %{storage_teams: storage_teams} do
      assert {:ok, tags} = Finalization.key_to_tags(<<0x90>>, storage_teams)
      assert Enum.sort(tags) == [0, 1, 2]

      assert {:ok, tags} = Finalization.key_to_tags(<<0x50>>, storage_teams)
      assert Enum.sort(tags) == [0, 2]

      assert {:ok, tags} = Finalization.key_to_tags(<<0x85>>, storage_teams)
      assert Enum.sort(tags) == [0, 1, 2]
    end

    test "returns empty list for key with no matching teams" do
      storage_teams = [
        %{tag: 0, key_range: {<<0x10>>, <<0x20>>}, storage_ids: ["storage_1"]}
      ]

      assert {:ok, []} = Finalization.key_to_tags(<<0x05>>, storage_teams)
      assert {:ok, []} = Finalization.key_to_tags(<<0x25>>, storage_teams)
    end

    test "handles boundary conditions correctly", %{storage_teams: storage_teams} do
      # Boundary inclusive behavior at key range edges
      assert {:ok, tags} = Finalization.key_to_tags(<<0x80>>, storage_teams)
      assert Enum.sort(tags) == [0, 1, 2]

      assert {:ok, tags} = Finalization.key_to_tags(<<0x40>>, storage_teams)
      assert Enum.sort(tags) == [0, 2]
    end
  end

  describe "key_to_tag/2 (legacy)" do
    setup do
      storage_teams = [
        %{tag: 0, key_range: {<<>>, <<0xFF>>}, storage_ids: ["storage_1", "storage_2"]},
        %{tag: 1, key_range: {<<0xFF>>, :end}, storage_ids: ["storage_3", "storage_4"]}
      ]

      %{storage_teams: storage_teams}
    end

    test "maps key to correct tag for first range", %{storage_teams: storage_teams} do
      assert {:ok, 0} = Finalization.key_to_tag(<<0x01>>, storage_teams)
      assert {:ok, 0} = Finalization.key_to_tag(<<0x80>>, storage_teams)
      assert {:ok, 0} = Finalization.key_to_tag(<<0xFE>>, storage_teams)
    end

    test "maps key to correct tag for second range", %{storage_teams: storage_teams} do
      assert {:ok, 1} = Finalization.key_to_tag(<<0xFF>>, storage_teams)
      assert {:ok, 1} = Finalization.key_to_tag(<<0xFF, 0x01>>, storage_teams)
    end

    test "returns error for empty storage teams" do
      assert {:error, :no_matching_team} = Finalization.key_to_tag(<<"any_key">>, [])
    end

    test "handles boundary conditions correctly", %{storage_teams: storage_teams} do
      assert {:ok, 1} = Finalization.key_to_tag(<<0xFF>>, storage_teams)
      assert {:ok, 0} = Finalization.key_to_tag(<<0xFE, 0xFF>>, storage_teams)
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

  describe "transform_transactions_for_resolution/1" do
    test "transforms transaction list to resolver format" do
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

      transactions = [
        {fn _ -> :ok end, binary_transaction1},
        {fn _ -> :ok end, binary_transaction2}
      ]

      result = Finalization.transform_transactions_for_resolution(transactions)

      expected_version = Bedrock.DataPlane.Version.from_integer(100)

      expected = [
        {{expected_version, [{<<"read_key">>, <<"read_key\0">>}]}, [{<<"write_key1">>, <<"write_key1\0">>}]},
        {nil, [{<<"write_key2">>, <<"write_key2\0">>}]}
      ]

      assert result == expected
    end

    test "handles empty transaction list" do
      result = Finalization.transform_transactions_for_resolution([])
      assert result == []
    end

    test "handles transactions with no reads" do
      transaction_map = %{
        mutations: [{:set, <<"key">>, <<"value">>}],
        write_conflicts: [{<<"key">>, <<"key\0">>}],
        read_conflicts: {nil, []}
      }

      binary_transaction = Transaction.encode(transaction_map)

      transactions = [
        {fn _ -> :ok end, binary_transaction}
      ]

      result = Finalization.transform_transactions_for_resolution(transactions)
      expected = [{nil, [{<<"key">>, <<"key\0">>}]}]

      assert result == expected
    end

    test "handles transactions with no writes" do
      transaction_map = %{
        mutations: [],
        write_conflicts: [],
        read_conflicts: {Bedrock.DataPlane.Version.from_integer(100), [{<<"read_key">>, <<"read_key\0">>}]}
      }

      binary_transaction = Transaction.encode(transaction_map)

      transactions = [
        {fn _ -> :ok end, binary_transaction}
      ]

      result = Finalization.transform_transactions_for_resolution(transactions)
      expected_version = Bedrock.DataPlane.Version.from_integer(100)
      expected = [{{expected_version, [{<<"read_key">>, <<"read_key\0">>}]}, []}]

      assert result == expected
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

      transactions = [
        {fn _ -> :ok end, binary_transaction}
      ]

      result = Finalization.transform_transactions_for_resolution(transactions)

      [{nil, write_conflicts}] = result

      # Write conflicts must maintain transaction ordering for consistency
      expected_conflicts = [
        {<<"z_key">>, <<"z_key\0">>},
        {<<"a_key">>, <<"a_key\0">>},
        {<<"m_key">>, <<"m_key\0">>}
      ]

      assert write_conflicts == expected_conflicts
    end
  end

  describe "edge cases and error handling" do
    test "key_to_tag handles keys at exact boundaries" do
      storage_teams = Support.sample_storage_teams()

      assert {:ok, 1} = Finalization.key_to_tag(<<"m">>, storage_teams)
      assert {:ok, 2} = Finalization.key_to_tag(<<"z">>, storage_teams)
      assert {:ok, 0} = Finalization.key_to_tag(<<"l">>, storage_teams)
      assert {:ok, 1} = Finalization.key_to_tag(<<"y">>, storage_teams)
    end

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
