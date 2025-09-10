defmodule TestTelemetryHandlerTest do
  use ExUnit.Case, async: true

  alias Bedrock.Test.Common.TestTelemetryHandler

  # Common setup helper
  defp setup_handler(events) do
    handler = TestTelemetryHandler.attach_many(events)
    on_exit(fn -> TestTelemetryHandler.detach(handler) end)
    handler
  end

  # Helper to emit events and wait for processing
  defp emit_and_wait(events_to_emit) do
    Enum.each(events_to_emit, fn {event, measurements, metadata} ->
      :telemetry.execute(event, measurements, metadata)
    end)

    Process.sleep(10)
  end

  describe "TestTelemetryHandler" do
    test "captures telemetry events with attach_many/1" do
      handler = setup_handler([[:bedrock, :test, :started], [:bedrock, :test, :completed]])

      emit_and_wait([
        {[:bedrock, :test, :started], %{count: 1}, %{user: "alice"}},
        {[:bedrock, :test, :completed], %{duration: 100}, %{user: "alice"}}
      ])

      # Events are returned in chronological order (oldest first)
      assert [
               %{name: [:bedrock, :test, :started], measurements: %{count: 1}, metadata: %{user: "alice"}},
               %{name: [:bedrock, :test, :completed], measurements: %{duration: 100}, metadata: %{user: "alice"}}
             ] = TestTelemetryHandler.get_events(handler)
    end

    test "captures specific events with attach_many/1" do
      handler = setup_handler([[:bedrock, :test, :started], [:bedrock, :test, :completed]])

      emit_and_wait([
        {[:bedrock, :test, :started], %{}, %{}},
        {[:bedrock, :test, :completed], %{}, %{}},
        # This should NOT be captured
        {[:bedrock, :test, :failed], %{}, %{}}
      ])

      events = TestTelemetryHandler.get_events(handler)
      event_names = Enum.map(events, & &1.name)

      # Verify exactly 2 events captured with specific names
      assert length(events) == 2
      assert [:bedrock, :test, :started] in event_names
      assert [:bedrock, :test, :completed] in event_names
      refute [:bedrock, :test, :failed] in event_names
    end

    test "clears events" do
      handler = setup_handler([[:bedrock, :test, :event1], [:bedrock, :test, :event2]])

      emit_and_wait([
        {[:bedrock, :test, :event1], %{}, %{}},
        {[:bedrock, :test, :event2], %{}, %{}}
      ])

      # Verify events were captured, then clear and verify empty
      assert [_, _] = TestTelemetryHandler.get_events(handler)
      TestTelemetryHandler.clear_events(handler)
      assert [] = TestTelemetryHandler.get_events(handler)
    end

    test "waits for events with timeout" do
      handler = setup_handler([[:bedrock, :test, :event1], [:bedrock, :test, :event2]])

      # Emit events in a background task
      task =
        Task.async(fn ->
          Process.sleep(50)
          :telemetry.execute([:bedrock, :test, :event1], %{}, %{})
          Process.sleep(50)
          :telemetry.execute([:bedrock, :test, :event2], %{}, %{})
        end)

      # Wait for exactly 2 events to be received
      assert {:ok, [%{name: [:bedrock, :test, :event1]}, %{name: [:bedrock, :test, :event2]}]} =
               TestTelemetryHandler.wait_for_events(handler, 2, 200)

      Task.await(task)
    end

    test "times out when waiting for events" do
      handler = setup_handler([[:bedrock, :test, :event1]])

      # Don't emit any events - should timeout
      assert {:timeout, []} = TestTelemetryHandler.wait_for_events(handler, 1, 50)
    end
  end
end
