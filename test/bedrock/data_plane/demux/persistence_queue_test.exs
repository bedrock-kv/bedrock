defmodule Bedrock.DataPlane.Demux.PersistenceQueueTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Demux.PersistenceQueue

  describe "enqueue/dequeue" do
    test "enforces bounded capacity and reports backpressure" do
      test_pid = self()

      handler_id = "queue-backpressure-#{System.unique_integer([:positive])}"

      :ok =
        :telemetry.attach_many(
          handler_id,
          [[:bedrock, :demux, :persistence_queue, :backpressure]],
          &__MODULE__.handle_telemetry_event/4,
          test_pid
        )

      on_exit(fn -> :telemetry.detach(handler_id) end)

      queue = PersistenceQueue.new(capacity: 1)

      assert {:ok, queue} = PersistenceQueue.enqueue(queue, :first, now: 100)
      assert {:error, :full, queue} = PersistenceQueue.enqueue(queue, :second, now: 101)
      assert PersistenceQueue.lag(queue) == 1

      assert_receive {:telemetry, [:bedrock, :demux, :persistence_queue, :backpressure], measurements, %{}}
      assert measurements.lag == 1
      assert measurements.capacity == 1
    end

    test "dequeues in FIFO order" do
      queue = PersistenceQueue.new(capacity: 10)

      {:ok, queue} = PersistenceQueue.enqueue(queue, :a, now: 0)
      {:ok, queue} = PersistenceQueue.enqueue(queue, :b, now: 1)
      {:ok, queue} = PersistenceQueue.enqueue(queue, :c, now: 2)

      assert {:ok, token_a, :a, queue} = PersistenceQueue.dequeue(queue, now: 3)
      assert {:ok, queue} = PersistenceQueue.ack(queue, token_a)

      assert {:ok, token_b, :b, queue} = PersistenceQueue.dequeue(queue, now: 4)
      assert {:ok, queue} = PersistenceQueue.ack(queue, token_b)

      assert {:ok, token_c, :c, queue} = PersistenceQueue.dequeue(queue, now: 5)
      assert {:ok, queue} = PersistenceQueue.ack(queue, token_c)

      assert :empty = PersistenceQueue.dequeue(queue, now: 6)
      assert PersistenceQueue.counts(queue) == %{pending: 0, scheduled: 0, in_flight: 0, lag: 0}
    end
  end

  describe "retry scheduling" do
    test "nack schedules retry for a future time" do
      queue = PersistenceQueue.new(capacity: 10, max_retries: 3, retry_base_backoff_ms: 10)

      {:ok, queue} = PersistenceQueue.enqueue(queue, :retry_payload, now: 1_000)
      {:ok, token, :retry_payload, queue} = PersistenceQueue.dequeue(queue, now: 1_001)

      assert {:rescheduled, queue} =
               PersistenceQueue.nack(queue, token, :temporary_failure, now: 1_002, backoff_ms: 50)

      assert :empty = PersistenceQueue.dequeue(queue, now: 1_051)
      assert {:ok, token2, :retry_payload, queue} = PersistenceQueue.dequeue(queue, now: 1_052)
      assert {:ok, queue} = PersistenceQueue.ack(queue, token2)
      assert PersistenceQueue.counts(queue) == %{pending: 0, scheduled: 0, in_flight: 0, lag: 0}
    end

    test "drops entry after max retries" do
      queue = PersistenceQueue.new(capacity: 10, max_retries: 1, retry_base_backoff_ms: 1)

      {:ok, queue} = PersistenceQueue.enqueue(queue, :drop_me, now: 0)
      {:ok, token1, :drop_me, queue} = PersistenceQueue.dequeue(queue, now: 0)
      {:rescheduled, queue} = PersistenceQueue.nack(queue, token1, :err1, now: 0, backoff_ms: 0)

      {:ok, token2, :drop_me, queue} = PersistenceQueue.dequeue(queue, now: 0)
      {:dropped, queue} = PersistenceQueue.nack(queue, token2, :err2, now: 0, backoff_ms: 0)

      assert PersistenceQueue.counts(queue) == %{pending: 0, scheduled: 0, in_flight: 0, lag: 0}
      assert :empty = PersistenceQueue.dequeue(queue, now: 0)
    end
  end

  def handle_telemetry_event(event, measurements, metadata, pid) do
    send(pid, {:telemetry, event, measurements, metadata})
  end
end
