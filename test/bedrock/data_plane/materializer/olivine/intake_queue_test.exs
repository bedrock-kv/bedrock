defmodule Bedrock.DataPlane.Materializer.Olivine.IntakeQueueTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Bedrock.DataPlane.Materializer.Olivine.IntakeQueue
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.Test.DataPlane.TransactionTestSupport

  describe "new/0" do
    test "creates an empty intake queue" do
      queue = IntakeQueue.new()
      assert IntakeQueue.empty?(queue)
      assert IntakeQueue.size(queue) == 0
    end
  end

  describe "add_transactions/2" do
    test "adds a single encoded transaction to the queue" do
      queue = IntakeQueue.new()
      encoded_tx = create_encoded_transaction(100)

      updated_queue = IntakeQueue.add_transactions(queue, [encoded_tx])

      refute IntakeQueue.empty?(updated_queue)
      assert IntakeQueue.size(updated_queue) == 1
    end

    test "adds multiple encoded transactions to the queue" do
      queue = IntakeQueue.new()

      encoded_txs = [
        create_encoded_transaction(100),
        create_encoded_transaction(200),
        create_encoded_transaction(300)
      ]

      updated_queue = IntakeQueue.add_transactions(queue, encoded_txs)

      assert IntakeQueue.size(updated_queue) == 3
    end

    test "maintains FIFO order for transactions" do
      queue = IntakeQueue.new()

      encoded_txs = [
        create_encoded_transaction(100),
        create_encoded_transaction(200),
        create_encoded_transaction(300)
      ]

      updated_queue = IntakeQueue.add_transactions(queue, encoded_txs)
      {batch, _last_version, _remaining} = IntakeQueue.take_batch_by_count(updated_queue, 3)

      assert batch == encoded_txs
    end

    test "adds to existing queue preserving order" do
      queue = IntakeQueue.new()
      first_batch = [create_encoded_transaction(100)]
      second_batch = [create_encoded_transaction(200), create_encoded_transaction(300)]

      queue = IntakeQueue.add_transactions(queue, first_batch)
      queue = IntakeQueue.add_transactions(queue, second_batch)

      {batch, _last_version, _remaining} = IntakeQueue.take_batch_by_count(queue, 3)
      assert batch == first_batch ++ second_batch
    end
  end

  describe "take_batch_by_size/2" do
    test "returns empty batch from empty queue" do
      queue = IntakeQueue.new()

      {batch, last_version, updated_queue} = IntakeQueue.take_batch_by_size(queue, 1000)

      assert batch == []
      assert last_version == nil
      assert IntakeQueue.empty?(updated_queue)
    end

    test "takes at least one transaction even if it exceeds size limit" do
      queue = IntakeQueue.new()
      large_tx = create_encoded_transaction(100, size: 1000)
      queue = IntakeQueue.add_transactions(queue, [large_tx])

      # Request batch with size limit smaller than transaction
      {batch, last_version, updated_queue} = IntakeQueue.take_batch_by_size(queue, 100)

      assert length(batch) == 1
      assert batch == [large_tx]
      assert last_version == <<100::64>>
      assert IntakeQueue.empty?(updated_queue)
    end

    test "takes multiple transactions up to size limit" do
      queue = IntakeQueue.new()
      # Create transactions of known sizes
      tx1 = create_encoded_transaction(100, size: 100)
      tx2 = create_encoded_transaction(200, size: 100)
      tx3 = create_encoded_transaction(300, size: 100)
      tx4 = create_encoded_transaction(400, size: 100)

      queue = IntakeQueue.add_transactions(queue, [tx1, tx2, tx3, tx4])

      # Take up to 250 bytes (should get first 3 transactions: 100+100+100=300)
      {batch, last_version, updated_queue} = IntakeQueue.take_batch_by_size(queue, 250)

      assert length(batch) == 3
      assert batch == [tx1, tx2, tx3]
      assert last_version == <<300::64>>
      assert IntakeQueue.size(updated_queue) == 1
    end

    test "returns last version from the batch" do
      queue = IntakeQueue.new()

      txs = [
        create_encoded_transaction(100),
        create_encoded_transaction(200),
        create_encoded_transaction(300)
      ]

      queue = IntakeQueue.add_transactions(queue, txs)

      {_batch, last_version, _updated_queue} = IntakeQueue.take_batch_by_size(queue, 10_000)

      assert last_version == <<300::64>>
    end

    test "leaves remaining transactions in queue" do
      queue = IntakeQueue.new()

      txs = [
        create_encoded_transaction(100, size: 100),
        create_encoded_transaction(200, size: 100),
        create_encoded_transaction(300, size: 100)
      ]

      queue = IntakeQueue.add_transactions(queue, txs)

      {_batch, _last_version, updated_queue} = IntakeQueue.take_batch_by_size(queue, 150)

      assert IntakeQueue.size(updated_queue) == 1
    end
  end

  describe "take_batch_by_count/2" do
    test "returns empty batch from empty queue" do
      queue = IntakeQueue.new()

      {batch, last_version, updated_queue} = IntakeQueue.take_batch_by_count(queue, 10)

      assert batch == []
      assert last_version == nil
      assert IntakeQueue.empty?(updated_queue)
    end

    test "takes exact number of transactions requested" do
      queue = IntakeQueue.new()

      txs = [
        create_encoded_transaction(100),
        create_encoded_transaction(200),
        create_encoded_transaction(300),
        create_encoded_transaction(400)
      ]

      queue = IntakeQueue.add_transactions(queue, txs)

      {batch, last_version, updated_queue} = IntakeQueue.take_batch_by_count(queue, 2)

      assert length(batch) == 2
      assert batch == Enum.take(txs, 2)
      assert last_version == <<200::64>>
      assert IntakeQueue.size(updated_queue) == 2
    end

    test "takes all transactions if count exceeds queue size" do
      queue = IntakeQueue.new()

      txs = [
        create_encoded_transaction(100),
        create_encoded_transaction(200)
      ]

      queue = IntakeQueue.add_transactions(queue, txs)

      {batch, last_version, updated_queue} = IntakeQueue.take_batch_by_count(queue, 10)

      assert length(batch) == 2
      assert batch == txs
      assert last_version == <<200::64>>
      assert IntakeQueue.empty?(updated_queue)
    end

    test "returns last version from the batch" do
      queue = IntakeQueue.new()

      txs = [
        create_encoded_transaction(100),
        create_encoded_transaction(200),
        create_encoded_transaction(300)
      ]

      queue = IntakeQueue.add_transactions(queue, txs)

      {_batch, last_version, _updated_queue} = IntakeQueue.take_batch_by_count(queue, 2)

      assert last_version == <<200::64>>
    end

    test "handles zero count" do
      queue = IntakeQueue.new()
      txs = [create_encoded_transaction(100)]
      queue = IntakeQueue.add_transactions(queue, txs)

      {batch, last_version, updated_queue} = IntakeQueue.take_batch_by_count(queue, 0)

      assert batch == []
      assert last_version == nil
      assert IntakeQueue.size(updated_queue) == 1
    end
  end

  describe "empty?/1" do
    test "returns true for new queue" do
      queue = IntakeQueue.new()
      assert IntakeQueue.empty?(queue)
    end

    test "returns false after adding transactions" do
      queue = IntakeQueue.new()
      queue = IntakeQueue.add_transactions(queue, [create_encoded_transaction(100)])
      refute IntakeQueue.empty?(queue)
    end

    test "returns true after taking all transactions" do
      queue = IntakeQueue.new()
      queue = IntakeQueue.add_transactions(queue, [create_encoded_transaction(100)])
      {_batch, _last_version, queue} = IntakeQueue.take_batch_by_count(queue, 1)
      assert IntakeQueue.empty?(queue)
    end
  end

  describe "size/1" do
    test "returns 0 for new queue" do
      queue = IntakeQueue.new()
      assert IntakeQueue.size(queue) == 0
    end

    test "returns correct count after adding transactions" do
      queue = IntakeQueue.new()

      queue =
        IntakeQueue.add_transactions(queue, [
          create_encoded_transaction(100),
          create_encoded_transaction(200),
          create_encoded_transaction(300)
        ])

      assert IntakeQueue.size(queue) == 3
    end

    test "returns correct count after taking some transactions" do
      queue = IntakeQueue.new()

      queue =
        IntakeQueue.add_transactions(queue, [
          create_encoded_transaction(100),
          create_encoded_transaction(200),
          create_encoded_transaction(300)
        ])

      {_batch, _last_version, queue} = IntakeQueue.take_batch_by_count(queue, 2)
      assert IntakeQueue.size(queue) == 1
    end
  end

  # Property-based tests
  property "add and take operations maintain FIFO order" do
    check all(versions <- list_of(positive_integer(), min_length: 1, max_length: 50)) do
      queue = IntakeQueue.new()
      encoded_txs = Enum.map(versions, &create_encoded_transaction/1)

      queue = IntakeQueue.add_transactions(queue, encoded_txs)
      {batch, _last_version, _remaining} = IntakeQueue.take_batch_by_count(queue, length(versions))

      assert batch == encoded_txs
    end
  end

  property "size is always non-negative and accurate" do
    check all(
            operations <-
              list_of(
                one_of([
                  {:add, positive_integer()},
                  {:take_count, positive_integer()},
                  {:take_size, positive_integer()}
                ]),
                max_length: 20
              )
          ) do
      queue =
        Enum.reduce(operations, IntakeQueue.new(), fn
          {:add, version}, queue ->
            IntakeQueue.add_transactions(queue, [create_encoded_transaction(version)])

          {:take_count, count}, queue ->
            {_batch, _version, new_queue} = IntakeQueue.take_batch_by_count(queue, count)
            new_queue

          {:take_size, size}, queue ->
            {_batch, _version, new_queue} = IntakeQueue.take_batch_by_size(queue, size)
            new_queue
        end)

      size = IntakeQueue.size(queue)
      assert size >= 0
      assert IntakeQueue.empty?(queue) == (size == 0)
    end
  end

  property "take_batch_by_size always respects minimum of one transaction" do
    check all(
            version <- positive_integer(),
            max_size <- positive_integer()
          ) do
      queue = IntakeQueue.new()
      encoded_tx = create_encoded_transaction(version)
      queue = IntakeQueue.add_transactions(queue, [encoded_tx])

      {batch, last_version, updated_queue} = IntakeQueue.take_batch_by_size(queue, max_size)

      # Should always take at least one transaction
      assert length(batch) >= 1 or IntakeQueue.empty?(queue)

      if length(batch) > 0 do
        assert last_version == <<version::64>>
        assert IntakeQueue.empty?(updated_queue)
      end
    end
  end

  property "take_batch_by_count takes at most the requested count" do
    check all(
            versions <- list_of(positive_integer(), min_length: 1, max_length: 50),
            count <- positive_integer()
          ) do
      queue = IntakeQueue.new()
      encoded_txs = Enum.map(versions, &create_encoded_transaction/1)
      queue = IntakeQueue.add_transactions(queue, encoded_txs)

      {batch, _last_version, _remaining} = IntakeQueue.take_batch_by_count(queue, count)

      assert length(batch) <= count
      assert length(batch) <= length(versions)
    end
  end

  property "last_version is always from the last transaction in batch" do
    check all(
            versions <- list_of(positive_integer(), min_length: 1, max_length: 20),
            count <- positive_integer()
          ) do
      queue = IntakeQueue.new()
      encoded_txs = Enum.map(versions, &create_encoded_transaction/1)
      queue = IntakeQueue.add_transactions(queue, encoded_txs)

      {batch, last_version, _remaining} = IntakeQueue.take_batch_by_count(queue, count)

      if batch == [] do
        assert last_version == nil
      else
        last_tx = List.last(batch)
        expected_version = Transaction.commit_version!(last_tx)
        assert last_version == expected_version
      end
    end
  end

  # Helper functions
  defp create_encoded_transaction(version, opts \\ []) do
    size = Keyword.get(opts, :size, 100)

    # Create a simple transaction with a dummy key-value pair
    # The size of the resulting transaction will vary, but we'll pad if needed
    base_tx = TransactionTestSupport.new_log_transaction(version, %{"test_key" => "test_value"})

    # If a specific size is requested and it's larger than the base transaction,
    # create a transaction with a larger value
    if size > byte_size(base_tx) do
      # Create padding to reach desired size
      # Account for overhead
      padding_size = size - 50
      large_value = String.duplicate("x", max(1, padding_size))
      TransactionTestSupport.new_log_transaction(version, %{"key" => large_value})
    else
      base_tx
    end
  end
end
