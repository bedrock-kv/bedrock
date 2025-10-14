defmodule Bedrock.Internal.TransactionBuilder.NoWriteConflictTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Transaction
  alias Bedrock.Internal.TransactionBuilder.Tx

  # Helper function to build and decode transaction
  defp build_and_decode_transaction(operations_fn) do
    {:ok, result} =
      Tx.new()
      |> operations_fn.()
      |> Tx.commit(nil)
      |> Transaction.decode()

    result
  end

  # Helper to extract single keys from write conflict ranges
  defp extract_conflict_keys(write_conflicts) do
    Enum.flat_map(write_conflicts, fn {start_key, end_key} ->
      if end_key == start_key <> <<0>>, do: [start_key], else: []
    end)
  end

  describe "no_write_conflict option" do
    test "no_write_conflict prevents write conflicts from being created" do
      # Use pattern matching to extract and assert fields in one step
      assert %{
               mutations: [
                 {:set, "test1", "value1"},
                 {:set, "test2", "value2"},
                 {:atomic, :add, "counter1", "\x01"},
                 {:atomic, :add, "counter2", "\x01"}
               ],
               write_conflicts: write_conflicts
             } =
               build_and_decode_transaction(fn tx ->
                 tx
                 |> Tx.set("test1", "value1")
                 |> Tx.set("test2", "value2", no_write_conflict: true)
                 |> Tx.atomic_operation("counter1", :add, <<1>>)
                 |> Tx.atomic_operation("counter2", :add, <<1>>)
               end)

      # Only operations without no_write_conflict should create write conflicts
      assert ["test1"] = extract_conflict_keys(write_conflicts)
    end

    test "operations always remove conflicting operations regardless of no_write_conflict" do
      # Pattern match to assert mutations directly
      assert %{mutations: [{:set, "key", "value2"}]} =
               build_and_decode_transaction(fn tx ->
                 tx
                 |> Tx.set("key", "value1")
                 |> Tx.set("key", "value2", no_write_conflict: true)
               end)
    end

    test "regular operations remove conflicting operations" do
      # Pattern match to assert mutations directly
      assert %{mutations: [{:set, "key", "value2"}]} =
               build_and_decode_transaction(fn tx ->
                 tx
                 |> Tx.set("key", "value1")
                 |> Tx.set("key", "value2")
               end)
    end
  end
end
