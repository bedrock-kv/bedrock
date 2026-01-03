defmodule Bedrock.Internal.TransactionBuilder.TxClearTombstoneTest do
  @moduledoc """
  Unit test for the bug where range queries return :clear tombstone values
  instead of filtering them out.

  Related issue: bedrock-xma
  """
  use ExUnit.Case, async: true

  alias Bedrock.Internal.TransactionBuilder.Tx

  describe "merge_storage_range_with_writes filters :clear tombstones" do
    test "cleared key is excluded from results when storage is empty" do
      tx =
        Tx.new()
        |> Tx.set("a", "value_a")
        |> Tx.set("b", "value_b")
        |> Tx.clear("b")
        |> Tx.set("c", "value_c")

      # "b" was cleared, so results should NOT include it
      # Bug: currently returns [{"a", "value_a"}, {"b", :clear}, {"c", "value_c"}]
      expected = [{"a", "value_a"}, {"c", "value_c"}]

      {_tx, results} =
        Tx.merge_storage_range_with_writes(
          tx,
          [],
          false,
          {"a", "z"},
          {"", "\xFF"}
        )

      assert results == expected,
             "Range query should not include cleared keys, got: #{inspect(results)}"
    end

    test "cleared key is excluded when merging with storage results" do
      tx =
        Tx.new()
        |> Tx.set("c", "pending_c")
        |> Tx.clear("c")

      # Storage has "b" and "d", pending write "c" was cleared
      # Results should NOT include "c"
      expected = [{"b", "stored_b"}, {"d", "stored_d"}]

      {_tx, results} =
        Tx.merge_storage_range_with_writes(
          tx,
          [{"b", "stored_b"}, {"d", "stored_d"}],
          false,
          {"a", "z"},
          {"", "\xFF"}
        )

      assert results == expected,
             "Range query should not include cleared keys, got: #{inspect(results)}"
    end

    test "cleared key overwrites storage value" do
      tx = Tx.clear(Tx.new(), "b")
      # Clear "b" which exists in storage

      # Storage has "a", "b", "c" but "b" was cleared
      # Results should NOT include "b"
      expected = [{"a", "stored_a"}, {"c", "stored_c"}]

      {_tx, results} =
        Tx.merge_storage_range_with_writes(
          tx,
          [{"a", "stored_a"}, {"b", "stored_b"}, {"c", "stored_c"}],
          false,
          {"a", "z"},
          {"", "\xFF"}
        )

      assert results == expected,
             "Range query should not include cleared storage keys, got: #{inspect(results)}"
    end

    test "multiple cleared keys are all excluded" do
      tx =
        Tx.new()
        |> Tx.set("a", "value_a")
        |> Tx.set("b", "value_b")
        |> Tx.set("c", "value_c")
        |> Tx.set("d", "value_d")
        |> Tx.clear("b")
        |> Tx.clear("d")

      # "b" and "d" were cleared
      expected = [{"a", "value_a"}, {"c", "value_c"}]

      {_tx, results} =
        Tx.merge_storage_range_with_writes(
          tx,
          [],
          false,
          {"a", "z"},
          {"", "\xFF"}
        )

      assert results == expected,
             "Range query should not include any cleared keys, got: #{inspect(results)}"
    end

    test "cleared then re-set key is included with new value" do
      tx =
        Tx.new()
        |> Tx.set("a", "original")
        |> Tx.clear("a")
        |> Tx.set("a", "new_value")

      # "a" was cleared then re-set, should have new value
      expected = [{"a", "new_value"}]

      {_tx, results} =
        Tx.merge_storage_range_with_writes(
          tx,
          [],
          false,
          {"a", "z"},
          {"", "\xFF"}
        )

      assert results == expected
    end

    test "scan_pending_writes also filters :clear tombstones" do
      # This tests the scan_pending_writes path (empty storage, has_more=false)
      tx =
        Tx.new()
        |> Tx.set("x", "value_x")
        |> Tx.set("y", "value_y")
        |> Tx.clear("y")
        |> Tx.set("z", "value_z")

      expected = [{"x", "value_x"}, {"z", "value_z"}]

      {_tx, results} =
        Tx.merge_storage_range_with_writes(
          tx,
          [],
          false,
          {"w", "\xFF"},
          {"", "\xFF"}
        )

      assert results == expected,
             "scan_pending_writes should not include cleared keys, got: #{inspect(results)}"
    end
  end
end
