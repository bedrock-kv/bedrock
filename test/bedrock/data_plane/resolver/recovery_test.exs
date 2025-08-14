defmodule Bedrock.DataPlane.Resolver.RecoveryTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.EncodedTransaction
  alias Bedrock.DataPlane.Resolver.Recovery
  alias Bedrock.DataPlane.Resolver.State
  alias Bedrock.DataPlane.Resolver.Tree
  alias Bedrock.DataPlane.Version

  describe "apply_transaction/2" do
    test "applies a decoded transaction to a tree" do
      tree = %Tree{}
      version = Version.from_integer(1)
      transaction = {version, %{"key1" => "value1", "key2" => "value2"}}

      {updated_tree, returned_version} = Recovery.apply_transaction(tree, transaction)

      assert returned_version == version
      assert %Tree{} = updated_tree
    end

    test "handles empty writes" do
      tree = %Tree{}
      version = Version.from_integer(1)
      transaction = {version, %{}}

      {updated_tree, returned_version} = Recovery.apply_transaction(tree, transaction)

      assert returned_version == version
      assert updated_tree == tree
    end
  end

  describe "apply_batch_of_transactions/2" do
    test "applies encoded binary transactions" do
      tree = %Tree{}

      # Create encoded transactions (as returned by Log.pull)
      version1 = Version.from_integer(1)
      version2 = Version.from_integer(2)
      transaction1 = {version1, %{"key1" => "value1"}}
      transaction2 = {version2, %{"key2" => "value2"}}
      encoded1 = EncodedTransaction.encode(transaction1)
      encoded2 = EncodedTransaction.encode(transaction2)

      transactions = [encoded1, encoded2]

      {updated_tree, last_version} = Recovery.apply_batch_of_transactions(tree, transactions)

      assert last_version == version2
      assert %Tree{} = updated_tree
    end

    test "handles empty transaction list" do
      tree = %Tree{}
      transactions = []

      {updated_tree, last_version} = Recovery.apply_batch_of_transactions(tree, transactions)

      assert last_version == nil
      assert updated_tree == tree
    end

    test "raises on invalid encoded transaction with clear error" do
      tree = %Tree{}
      # Invalid binary format
      invalid_binary = <<1, 2, 3, 4>>

      assert_raise RuntimeError, ~r/Transaction decode failed: invalid binary format/, fn ->
        Recovery.apply_batch_of_transactions(tree, [invalid_binary])
      end
    end

    test "raises on corrupted transaction with clear error" do
      tree = %Tree{}

      # Create a transaction and corrupt the CRC
      valid_transaction = {Version.from_integer(1), %{"key1" => "value1"}}
      encoded = EncodedTransaction.encode(valid_transaction)

      # Corrupt the last 4 bytes (CRC32)
      corrupted = binary_part(encoded, 0, byte_size(encoded) - 4) <> <<0, 0, 0, 0>>

      assert_raise RuntimeError, ~r/Transaction decode failed: CRC32 checksum mismatch/, fn ->
        Recovery.apply_batch_of_transactions(tree, [corrupted])
      end
    end

    test "handles nil tree" do
      version = Version.from_integer(1)
      transaction = {version, %{"key1" => "value1"}}
      encoded = EncodedTransaction.encode(transaction)

      {updated_tree, last_version} = Recovery.apply_batch_of_transactions(nil, [encoded])

      assert last_version == version
      assert %Tree{} = updated_tree
    end
  end

  describe "pull_transactions/4" do
    test "returns early when first_version equals last_version" do
      tree = %Tree{}
      log_ref = :some_log_ref
      version = Version.from_integer(5)

      result = Recovery.pull_transactions(tree, log_ref, version, version)

      assert {:ok, tree} == result
    end

    test "returns early for nil log with versions 0" do
      tree = %Tree{}
      zero_version = Version.zero()

      result = Recovery.pull_transactions(tree, nil, zero_version, zero_version)

      assert {:ok, tree} == result
    end
  end

  describe "recover_from/4" do
    setup do
      zero_version = Version.zero()

      state = %State{
        mode: :locked,
        tree: %Tree{},
        last_version: zero_version,
        oldest_version: zero_version
      }

      %{state: state}
    end

    test "requires locked mode", %{state: state} do
      unlocked_state = %{state | mode: :running}

      result = Recovery.recover_from(unlocked_state, [], Version.zero(), Version.from_integer(1))

      assert {:error, :lock_required} == result
    end

    test "returns state with updated mode and versions on success", %{state: state} do
      # Mock empty log source
      source_logs = []
      target_version = Version.from_integer(5)

      result = Recovery.recover_from(state, source_logs, Version.zero(), target_version)

      assert {:ok,
              %State{
                mode: :running,
                last_version: ^target_version,
                oldest_version: oldest_version
              }} = result

      assert oldest_version == Version.zero()
    end
  end
end
