defmodule Bedrock.DataPlane.Log.TransactionLogIntegrationTest do
  @moduledoc """
  Integration test for the complete transaction log pipeline that would have caught
  the wrapper/payload bug where TransactionStreams was returning the log wrapper
  instead of just the Transaction payload.

  This test exercises the full flow:
  Transaction.encode -> Writer.append -> TransactionStreams.from_file! -> Transaction.mutations
  """
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Log.Shale.TransactionStreams
  alias Bedrock.DataPlane.Log.Shale.Writer
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version

  @test_file "test_transaction_log_integration.log"

  setup do
    File.write!(@test_file, :binary.copy(<<0>>, 10_000))
    on_exit(fn -> File.rm(@test_file) end)
    :ok
  end

  # Helper to read all valid transactions from the log file
  defp read_transactions_from_log do
    @test_file
    |> TransactionStreams.from_file!()
    |> Enum.to_list()
    |> Enum.reject(fn
      {:error, _} -> true
      :eof -> true
      _ -> false
    end)
  end

  test "full transaction log pipeline: encode -> write -> read -> mutations" do
    transaction_map = %{
      mutations: [
        {:set, "key1", "value1"},
        {:set, "key2", "value2"},
        {:clear, "key3"},
        {:clear_range, "range_start", "range_end"}
      ],
      write_conflicts: [{"key1", "key1\0"}, {"key2", "key2\0"}],
      read_conflicts: {Version.from_integer(100), [{"read_key", "read_key\0"}]}
    }

    encoded_transaction = Transaction.encode(transaction_map)

    # Add commit version to simulate what commit proxy does
    commit_version = Version.from_integer(42)

    {:ok, transaction_with_version} =
      Transaction.add_commit_version(encoded_transaction, commit_version)

    {:ok, writer} = Writer.open(@test_file)
    {:ok, _updated_writer} = Writer.append(writer, transaction_with_version, commit_version)
    Writer.close(writer)

    assert [read_transaction] = read_transactions_from_log()

    # CRITICAL: Verify that what we read back is a valid Transaction
    # This would fail with the old bug where wrapper was returned instead of payload
    assert {:ok, _decoded} = Transaction.decode(read_transaction)

    # CRITICAL: Verify mutations works (this was failing with the bug)
    assert {:ok, mutations_stream} = Transaction.mutations(read_transaction)
    mutations_list = Enum.to_list(mutations_stream)
    assert length(mutations_list) == 4

    assert {:set, "key1", "value1"} in mutations_list
    assert {:set, "key2", "value2"} in mutations_list
    assert {:clear, "key3"} in mutations_list
    assert {:clear_range, "range_start", "range_end"} in mutations_list

    assert {:ok, ^commit_version} = Transaction.commit_version(read_transaction)

    expected_read_version = Version.from_integer(100)

    assert {:ok, {^expected_read_version, [{"read_key", "read_key\0"}]}} =
             Transaction.read_conflicts(read_transaction)

    assert {:ok, [{"key1", "key1\0"}, {"key2", "key2\0"}]} =
             Transaction.write_conflicts(read_transaction)
  end

  test "multiple transactions in log preserve order and individual integrity" do
    transactions = [
      %{mutations: [{:set, "key1", "value1"}]},
      %{mutations: [{:set, "key2", "value2"}]},
      %{mutations: [{:clear, "key3"}]}
    ]

    {:ok, writer} = Writer.open(@test_file)

    {final_writer, _} =
      Enum.reduce(transactions, {writer, 1}, fn tx, {w, version_num} ->
        encoded = Transaction.encode(tx)
        version = Version.from_integer(version_num)
        {:ok, tx_with_version} = Transaction.add_commit_version(encoded, version)
        {:ok, updated_w} = Writer.append(w, tx_with_version, version)
        {updated_w, version_num + 1}
      end)

    Writer.close(final_writer)

    assert [tx1, tx2, tx3] = read_transactions_from_log()

    # Verify each transaction can be processed individually
    for {tx, expected_version} <- [{tx1, 1}, {tx2, 2}, {tx3, 3}] do
      assert {:ok, _decoded} = Transaction.decode(tx)
      assert {:ok, mutations_stream} = Transaction.mutations(tx)
      assert [_single_mutation] = Enum.to_list(mutations_stream)
      expected_commit_version = Version.from_integer(expected_version)
      assert {:ok, ^expected_commit_version} = Transaction.commit_version(tx)
    end
  end

  test "empty transactions (no mutations) still roundtrip correctly" do
    # Edge case: transaction with no mutations but other sections
    transaction_map = %{
      mutations: [],
      write_conflicts: [{"key1", "key1\0"}],
      read_conflicts: {Version.from_integer(50), [{"read_key", "read_key\0"}]}
    }

    encoded = Transaction.encode(transaction_map)
    version = Version.from_integer(123)
    {:ok, tx_with_version} = Transaction.add_commit_version(encoded, version)

    {:ok, writer} = Writer.open(@test_file)
    {:ok, _} = Writer.append(writer, tx_with_version, version)
    Writer.close(writer)

    assert [read_transaction] = read_transactions_from_log()

    # Should decode and stream successfully even with empty mutations
    assert {:ok, _decoded} = Transaction.decode(read_transaction)
    assert {:ok, mutations_stream} = Transaction.mutations(read_transaction)
    assert Enum.to_list(mutations_stream) == []

    assert {:ok, [{"key1", "key1\0"}]} = Transaction.write_conflicts(read_transaction)
  end

  test "log format preserves transaction integrity across write/read cycle" do
    # Test that demonstrates the key issue we fixed:
    # TransactionStreams must return Transaction payload, not wrapper

    transaction_map = %{mutations: [{:set, "key", "value"}]}
    encoded = Transaction.encode(transaction_map)
    version = Version.from_integer(1)
    {:ok, tx_with_version} = Transaction.add_commit_version(encoded, version)

    {:ok, writer} = Writer.open(@test_file)
    {:ok, _} = Writer.append(writer, tx_with_version, version)
    Writer.close(writer)

    assert [read_transaction] = read_transactions_from_log()

    # CRITICAL TEST: This is what was failing with the bug
    # The read transaction should be a valid Transaction, not a wrapper

    # Should start with Transaction magic number, not version bytes
    <<"BRDT", _rest::binary>> = read_transaction

    assert {:ok, %{mutations: [{:set, "key", "value"}]}} = Transaction.decode(read_transaction)

    # Should stream mutations successfully (this was the failing operation)
    assert {:ok, stream} = Transaction.mutations(read_transaction)
    assert Enum.to_list(stream) == [{:set, "key", "value"}]

    assert {:ok, ^version} = Transaction.commit_version(read_transaction)
  end
end
