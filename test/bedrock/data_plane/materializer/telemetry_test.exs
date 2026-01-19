defmodule Bedrock.DataPlane.Materializer.TelemetryTest do
  use ExUnit.Case, async: false

  alias Bedrock.DataPlane.Materializer.Telemetry

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
        [:bedrock, :materializer, :shutdown_waiting],
        [:bedrock, :materializer, :shutdown_timeout],
        [:bedrock, :materializer, :transaction_timeout_scheduled]
      ],
      &__MODULE__.handle_event/4,
      %{test_pid: test_pid}
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    :ok
  end

  describe "trace_shutdown_waiting/1" do
    test "emits telemetry event with task count" do
      assert :ok = Telemetry.trace_shutdown_waiting(5)

      assert_received {:telemetry_event, [:bedrock, :materializer, :shutdown_waiting], %{task_count: 5}, _metadata}
    end

    test "handles zero tasks" do
      assert :ok = Telemetry.trace_shutdown_waiting(0)

      assert_received {:telemetry_event, [:bedrock, :materializer, :shutdown_waiting], %{task_count: 0}, _metadata}
    end
  end

  describe "trace_shutdown_timeout/1" do
    test "emits telemetry event with task count" do
      assert :ok = Telemetry.trace_shutdown_timeout(10)

      assert_received {:telemetry_event, [:bedrock, :materializer, :shutdown_timeout], %{task_count: 10}, _metadata}
    end

    test "handles large task counts" do
      assert :ok = Telemetry.trace_shutdown_timeout(1000)

      assert_received {:telemetry_event, [:bedrock, :materializer, :shutdown_timeout], %{task_count: 1000}, _metadata}
    end
  end

  describe "trace_transaction_timeout_scheduled/0" do
    test "emits telemetry event with no parameters" do
      assert :ok = Telemetry.trace_transaction_timeout_scheduled()

      assert_received {:telemetry_event, [:bedrock, :materializer, :transaction_timeout_scheduled], %{}, _metadata}
    end
  end

  describe "integration" do
    test "all trace functions return :ok" do
      assert :ok = Telemetry.trace_shutdown_waiting(5)
      assert :ok = Telemetry.trace_shutdown_timeout(10)
      assert :ok = Telemetry.trace_transaction_timeout_scheduled()
    end
  end
end
