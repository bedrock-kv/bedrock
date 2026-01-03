defmodule Bedrock.DataPlane.Resolver.TelemetryTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Resolver.Telemetry
  alias Bedrock.DataPlane.Version

  # Named handler function to avoid telemetry warning
  def handle_event(event, measurements, metadata, config) do
    send(config.test_pid, {:telemetry_event, event, measurements, metadata})
  end

  setup do
    # Attach a test handler to capture telemetry events
    test_pid = self()

    handler_id = {:test_handler, make_ref()}

    :telemetry.attach_many(
      handler_id,
      [
        [:bedrock, :resolver, :resolve_transactions, :received],
        [:bedrock, :resolver, :resolve_transactions, :processing],
        [:bedrock, :resolver, :resolve_transactions, :completed],
        [:bedrock, :resolver, :resolve_transactions, :reply_sent],
        [:bedrock, :resolver, :resolve_transactions, :waiting_list],
        [:bedrock, :resolver, :resolve_transactions, :waiting_list_inserted],
        [:bedrock, :resolver, :resolve_transactions, :waiting_resolved],
        [:bedrock, :resolver, :resolve_transactions, :validation_error],
        [:bedrock, :resolver, :resolve_transactions, :waiting_list_validation_error]
      ],
      &__MODULE__.handle_event/4,
      %{test_pid: test_pid}
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    :ok
  end

  describe "emit_received/2" do
    test "emits telemetry event with correct data" do
      transactions = ["tx1", "tx2"]
      version = Version.from_integer(100)

      assert :ok = Telemetry.emit_received(transactions, version)

      assert_received {:telemetry_event, [:bedrock, :resolver, :resolve_transactions, :received],
                       %{transactions: ^transactions}, %{next_version: ^version}}
    end
  end

  describe "emit_processing/2" do
    test "emits telemetry event with correct data" do
      transactions = ["tx1", "tx2", "tx3"]
      version = Version.from_integer(42)

      assert :ok = Telemetry.emit_processing(transactions, version)

      assert_received {:telemetry_event, [:bedrock, :resolver, :resolve_transactions, :processing],
                       %{transactions: ^transactions}, %{next_version: ^version}}
    end
  end

  describe "emit_completed/3" do
    test "emits telemetry event with transactions and aborted list" do
      transactions = ["tx1", "tx2"]
      aborted = ["tx3"]
      version = Version.from_integer(200)

      assert :ok = Telemetry.emit_completed(transactions, aborted, version)

      assert_received {:telemetry_event, [:bedrock, :resolver, :resolve_transactions, :completed],
                       %{transactions: ^transactions, aborted: ^aborted}, %{next_version: ^version}}
    end

    test "handles empty aborted list" do
      transactions = ["tx1"]
      aborted = []
      version = Version.zero()

      assert :ok = Telemetry.emit_completed(transactions, aborted, version)

      assert_received {:telemetry_event, _, %{aborted: []}, _}
    end
  end

  describe "emit_reply_sent/3" do
    test "emits telemetry event when reply is sent" do
      transactions = ["tx1"]
      aborted = ["tx2"]
      version = Version.from_integer(5)

      assert :ok = Telemetry.emit_reply_sent(transactions, aborted, version)

      assert_received {:telemetry_event, [:bedrock, :resolver, :resolve_transactions, :reply_sent],
                       %{transactions: ^transactions, aborted: ^aborted}, %{next_version: ^version}}
    end
  end

  describe "emit_waiting_list/2" do
    test "emits telemetry event for waiting list operations" do
      transactions = ["tx1", "tx2"]
      version = Version.from_integer(10)

      assert :ok = Telemetry.emit_waiting_list(transactions, version)

      assert_received {:telemetry_event, [:bedrock, :resolver, :resolve_transactions, :waiting_list],
                       %{transactions: ^transactions}, %{next_version: ^version}}
    end
  end

  describe "emit_waiting_list_inserted/3" do
    test "emits telemetry event with waiting list state" do
      transactions = ["tx1"]
      waiting_list = %{key1: "value1", key2: "value2"}
      version = Version.from_integer(15)

      assert :ok = Telemetry.emit_waiting_list_inserted(transactions, waiting_list, version)

      assert_received {:telemetry_event, [:bedrock, :resolver, :resolve_transactions, :waiting_list_inserted],
                       %{transactions: ^transactions, waiting_list: ^waiting_list}, %{next_version: ^version}}
    end
  end

  describe "emit_waiting_resolved/3" do
    test "emits telemetry event when waiting transactions are resolved" do
      transactions = ["tx1", "tx2"]
      aborted = ["tx3"]
      version = Version.from_integer(50)

      assert :ok = Telemetry.emit_waiting_resolved(transactions, aborted, version)

      assert_received {:telemetry_event, [:bedrock, :resolver, :resolve_transactions, :waiting_resolved],
                       %{transactions: ^transactions, aborted: ^aborted}, %{next_version: ^version}}
    end
  end

  describe "emit_validation_error/2" do
    test "emits telemetry event for validation errors" do
      transactions = ["bad_tx1"]
      reason = :invalid_format

      assert :ok = Telemetry.emit_validation_error(transactions, reason)

      assert_received {:telemetry_event, [:bedrock, :resolver, :resolve_transactions, :validation_error],
                       %{transactions: ^transactions}, %{reason: ^reason}}
    end

    test "handles complex error reasons" do
      transactions = ["tx1"]
      reason = {:error, {:conflict, "key1", "key2"}}

      assert :ok = Telemetry.emit_validation_error(transactions, reason)

      assert_received {:telemetry_event, _, _, %{reason: ^reason}}
    end
  end

  describe "emit_waiting_list_validation_error/2" do
    test "emits telemetry event for waiting list validation errors" do
      transactions = ["bad_tx1", "bad_tx2"]
      reason = :timeout

      assert :ok = Telemetry.emit_waiting_list_validation_error(transactions, reason)

      assert_received {:telemetry_event, [:bedrock, :resolver, :resolve_transactions, :waiting_list_validation_error],
                       %{transactions: ^transactions}, %{reason: ^reason}}
    end

    test "handles atom error reasons" do
      transactions = ["tx"]
      reason = :stale_version

      assert :ok = Telemetry.emit_waiting_list_validation_error(transactions, reason)

      assert_received {:telemetry_event, _, %{transactions: ^transactions}, %{reason: :stale_version}}
    end
  end

  describe "integration" do
    test "all emit functions return :ok" do
      version = Version.from_integer(1)
      transactions = ["tx"]
      aborted = []
      waiting_list = %{}
      reason = :test

      assert :ok = Telemetry.emit_received(transactions, version)
      assert :ok = Telemetry.emit_processing(transactions, version)
      assert :ok = Telemetry.emit_completed(transactions, aborted, version)
      assert :ok = Telemetry.emit_reply_sent(transactions, aborted, version)
      assert :ok = Telemetry.emit_waiting_list(transactions, version)
      assert :ok = Telemetry.emit_waiting_list_inserted(transactions, waiting_list, version)
      assert :ok = Telemetry.emit_waiting_resolved(transactions, aborted, version)
      assert :ok = Telemetry.emit_validation_error(transactions, reason)
      assert :ok = Telemetry.emit_waiting_list_validation_error(transactions, reason)
    end
  end
end
