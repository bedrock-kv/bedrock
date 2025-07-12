defmodule Bedrock.DataPlane.CommitProxy.FinalizationDataTransformationTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.CommitProxy.Finalization
  alias FinalizationTestSupport, as: Support

  describe "key_to_tag/2" do
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
      # Key exactly at range boundary should belong to second range
      assert {:ok, 1} = Finalization.key_to_tag(<<0xFF>>, storage_teams)

      # Key just before boundary should belong to first range
      assert {:ok, 0} = Finalization.key_to_tag(<<0xFE, 0xFF>>, storage_teams)
    end
  end

  describe "group_writes_by_tag/2" do
    setup do
      storage_teams = [
        %{tag: 0, key_range: {<<>>, <<"m">>}, storage_ids: ["storage_1"]},
        %{tag: 1, key_range: {<<"m">>, :end}, storage_ids: ["storage_2"]}
      ]

      %{storage_teams: storage_teams}
    end

    test "groups writes by their target storage team tags", %{storage_teams: storage_teams} do
      writes = %{
        <<"apple">> => <<"fruit">>,
        <<"banana">> => <<"yellow">>,
        <<"orange">> => <<"citrus">>,
        <<"zebra">> => <<"animal">>
      }

      result = Finalization.group_writes_by_tag(writes, storage_teams)

      expected =
        {:ok,
         %{
           0 => %{
             <<"apple">> => <<"fruit">>,
             <<"banana">> => <<"yellow">>
           },
           1 => %{
             <<"orange">> => <<"citrus">>,
             <<"zebra">> => <<"animal">>
           }
         }}

      assert result == expected
    end

    test "handles empty writes map", %{storage_teams: storage_teams} do
      result = Finalization.group_writes_by_tag(%{}, storage_teams)
      assert result == {:ok, %{}}
    end

    test "handles writes that all belong to same tag", %{storage_teams: storage_teams} do
      writes = %{
        <<"apple">> => <<"fruit">>,
        <<"banana">> => <<"yellow">>
      }

      result = Finalization.group_writes_by_tag(writes, storage_teams)

      expected =
        {:ok,
         %{
           0 => %{
             <<"apple">> => <<"fruit">>,
             <<"banana">> => <<"yellow">>
           }
         }}

      assert result == expected
    end
  end

  describe "merge_writes_by_tag/2" do
    test "merges write maps for same tags" do
      acc = %{
        0 => %{<<"key1">> => <<"value1">>},
        1 => %{<<"key2">> => <<"value2">>}
      }

      new_writes = %{
        0 => %{<<"key3">> => <<"value3">>},
        2 => %{<<"key4">> => <<"value4">>}
      }

      result = Finalization.merge_writes_by_tag(acc, new_writes)

      expected = %{
        0 => %{<<"key1">> => <<"value1">>, <<"key3">> => <<"value3">>},
        1 => %{<<"key2">> => <<"value2">>},
        2 => %{<<"key4">> => <<"value4">>}
      }

      assert result == expected
    end

    test "handles empty maps" do
      assert Finalization.merge_writes_by_tag(%{}, %{}) == %{}

      acc = %{0 => %{<<"key">> => <<"value">>}}
      assert Finalization.merge_writes_by_tag(acc, %{}) == acc
      assert Finalization.merge_writes_by_tag(%{}, acc) == acc
    end

    test "overwrites values for same keys" do
      acc = %{0 => %{<<"key">> => <<"old_value">>}}
      new_writes = %{0 => %{<<"key">> => <<"new_value">>}}

      result = Finalization.merge_writes_by_tag(acc, new_writes)
      expected = %{0 => %{<<"key">> => <<"new_value">>}}

      assert result == expected
    end
  end

  describe "transform_transactions_for_resolution/1" do
    test "transforms transaction list to resolver format" do
      transactions = [
        {fn _ -> :ok end, {{100, [<<"read_key">>]}, %{<<"write_key1">> => <<"write_value1">>}}},
        {fn _ -> :ok end, {nil, %{<<"write_key2">> => <<"write_value2">>}}}
      ]

      result = Finalization.transform_transactions_for_resolution(transactions)

      expected = [
        {{100, [<<"read_key">>]}, [<<"write_key1">>]},
        {nil, [<<"write_key2">>]}
      ]

      assert result == expected
    end

    test "handles empty transaction list" do
      result = Finalization.transform_transactions_for_resolution([])
      assert result == []
    end

    test "handles transactions with no reads" do
      transactions = [
        {fn _ -> :ok end, {nil, %{<<"key">> => <<"value">>}}}
      ]

      result = Finalization.transform_transactions_for_resolution(transactions)
      expected = [{nil, [<<"key">>]}]

      assert result == expected
    end

    test "handles transactions with no writes" do
      transactions = [
        {fn _ -> :ok end, {{100, [<<"read_key">>]}, %{}}}
      ]

      result = Finalization.transform_transactions_for_resolution(transactions)
      expected = [{{100, [<<"read_key">>]}, []}]

      assert result == expected
    end

    test "extracts write keys in consistent order" do
      # Test that write keys are extracted in a deterministic order
      writes = %{
        <<"z_key">> => <<"value1">>,
        <<"a_key">> => <<"value2">>,
        <<"m_key">> => <<"value3">>
      }

      transactions = [{fn _ -> :ok end, {nil, writes}}]
      result = Finalization.transform_transactions_for_resolution(transactions)

      [{nil, write_keys}] = result

      # Keys should be sorted for consistency
      expected_keys = [<<"a_key">>, <<"m_key">>, <<"z_key">>]
      assert Enum.sort(write_keys) == expected_keys
    end
  end

  describe "edge cases and error handling" do
    test "key_to_tag handles keys at exact boundaries" do
      storage_teams = Support.sample_storage_teams()

      # Key exactly at boundary should belong to the second range
      assert {:ok, 1} = Finalization.key_to_tag(<<"m">>, storage_teams)
      assert {:ok, 2} = Finalization.key_to_tag(<<"z">>, storage_teams)

      # Keys just before boundaries
      assert {:ok, 0} = Finalization.key_to_tag(<<"l">>, storage_teams)
      assert {:ok, 1} = Finalization.key_to_tag(<<"y">>, storage_teams)
    end

    test "group_writes_by_tag handles unknown keys by returning error" do
      storage_teams = [
        %{tag: 0, key_range: {<<"a">>, <<"m">>}, storage_ids: ["storage_1"]}
      ]

      # This key doesn't match any range
      writes = %{<<"z_unknown">> => <<"value">>}

      # Should return error with storage team coverage error
      assert Finalization.group_writes_by_tag(writes, storage_teams) ==
               {:error, {:storage_team_coverage_error, "z_unknown"}}
    end
  end
end
