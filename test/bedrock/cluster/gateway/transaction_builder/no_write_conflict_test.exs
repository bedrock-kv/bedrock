defmodule Bedrock.Cluster.Gateway.TransactionBuilder.NoWriteConflictTest do
  use ExUnit.Case, async: true

  alias Bedrock.Cluster.Gateway.TransactionBuilder.Tx
  alias Bedrock.DataPlane.Transaction

  describe "no_write_conflict option" do
    test "no_write_conflict prevents write conflicts from being created" do
      {:ok, %{mutations: mutations, write_conflicts: write_conflicts}} =
        Tx.new()
        |> Tx.set("test1", "value1")
        |> Tx.set("test2", "value2", no_write_conflict: true)
        |> Tx.atomic_operation("counter1", :add, <<1>>)
        |> Tx.atomic_operation("counter2", :add, <<1>>)
        |> Tx.commit(nil)
        |> Transaction.decode()

      assert mutations == [
               {:set, "test1", "value1"},
               {:set, "test2", "value2"},
               {:atomic, :add, "counter1", "\x01"},
               {:atomic, :add, "counter2", "\x01"}
             ]

      # Only operations without no_write_conflict should create write conflicts
      conflict_keys =
        Enum.flat_map(write_conflicts, fn {start_key, end_key} ->
          # Extract the key from the range (assuming single-key ranges)
          if end_key == start_key <> <<0>> do
            [start_key]
          else
            []
          end
        end)

      assert conflict_keys == ["test1"]
    end

    test "operations always remove conflicting operations regardless of no_write_conflict" do
      {:ok, %{mutations: mutations}} =
        Tx.new()
        |> Tx.set("key", "value1")
        |> Tx.set("key", "value2", no_write_conflict: true)
        |> Tx.commit(nil)
        |> Transaction.decode()

      # Should only have the second mutation since conflicting operations are always removed
      assert mutations == [{:set, "key", "value2"}]
    end

    test "regular operations remove conflicting operations" do
      {:ok, %{mutations: mutations}} =
        Tx.new()
        |> Tx.set("key", "value1")
        |> Tx.set("key", "value2")
        |> Tx.commit(nil)
        |> Transaction.decode()

      # Should only have the second mutation since regular set removes conflicts
      assert mutations == [{:set, "key", "value2"}]
    end
  end
end
