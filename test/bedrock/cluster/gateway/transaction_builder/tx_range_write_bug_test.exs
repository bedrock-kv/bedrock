defmodule Bedrock.Cluster.Gateway.TransactionBuilder.TxRangeWriteBugTest do
  @moduledoc """
  Unit test demonstrating the bug where pending writes are not included
  when storage server indicates it has no more data in its range.
  """
  use ExUnit.Case, async: true

  alias Bedrock.Cluster.Gateway.TransactionBuilder.Tx

  describe "fixed function signature" do
    test "new 5-argument signature correctly handles empty storage with has_more=false" do
      tx =
        Tx.new()
        |> Tx.set("c", "value_c")
        |> Tx.set("f", "value_f")
        # Outside query range
        |> Tx.set("z", "value_z")

      # Test the new 5-argument function
      {_updated_tx, results} =
        Tx.merge_storage_range_with_writes(
          tx,
          # empty storage
          [],
          # has_more = false (no more data in shard)
          false,
          # query range
          {"a", "j"},
          # shard range
          {"", "m"}
        )

      # Should include pending writes within query range
      expected = [{"c", "value_c"}, {"f", "value_f"}]
      assert results == expected

      # Should not include "z" which is outside query range
      refute {"z", "value_z"} in results
    end

    test "new signature correctly handles partial storage with has_more=false" do
      tx =
        Tx.new()
        |> Tx.set("a", "pending_a")
        # Between storage
        |> Tx.set("c", "pending_c")
        # After storage, within query range
        |> Tx.set("f", "pending_f")
        # Outside query range
        |> Tx.set("z", "pending_z")

      {_updated_tx, results} =
        Tx.merge_storage_range_with_writes(
          tx,
          # storage data
          [{"b", "stored_b"}, {"d", "stored_d"}],
          # has_more = false (no more data in shard)
          false,
          # query range
          {"a", "j"},
          # shard range
          {"", "m"}
        )

      # Should merge storage with overlapping writes + additional writes in range
      expected = [
        # from storage
        {"b", "stored_b"},
        # pending write between storage
        {"c", "pending_c"},
        # from storage
        {"d", "stored_d"},
        # pending write after storage, within query range
        {"f", "pending_f"}
      ]

      assert results == expected

      # Should not include "a" (before storage) or "z" (outside query range)
      refute {"a", "pending_a"} in results
      refute {"z", "pending_z"} in results
    end

    test "new signature respects has_more=true by not scanning beyond storage" do
      tx =
        Tx.new()
        |> Tx.set("f", "pending_f")
        |> Tx.set("h", "pending_h")

      {_updated_tx, results} =
        Tx.merge_storage_range_with_writes(
          tx,
          # storage data
          [{"b", "stored_b"}, {"d", "stored_d"}],
          # has_more = true (more data available in shard)
          true,
          # query range
          {"a", "j"},
          # shard range
          {"", "m"}
        )

      # Should only merge storage with overlapping writes, no additional scanning
      expected = [{"b", "stored_b"}, {"d", "stored_d"}]
      assert results == expected

      # Should not include pending writes beyond storage when has_more=true
      refute {"f", "pending_f"} in results
      refute {"h", "pending_h"} in results
    end
  end
end
