defmodule TestTelemetryHandlerTest do
  use ExUnit.Case, async: true

  describe "TestTelemetryHandler" do
    test "captures telemetry events with attach_many/1" do
      handler =
        TestTelemetryHandler.attach_many([
          [:bedrock, :test, :started],
          [:bedrock, :test, :completed]
        ])

      on_exit(fn -> TestTelemetryHandler.detach(handler) end)

      # Emit some test events
      :telemetry.execute([:bedrock, :test, :started], %{count: 1}, %{user: "alice"})
      :telemetry.execute([:bedrock, :test, :completed], %{duration: 100}, %{user: "alice"})

      # Give GenServer time to process events
      Process.sleep(10)

      events = TestTelemetryHandler.get_events(handler)

      assert length(events) == 2

      # Events are returned in chronological order (oldest first)
      [started_event, completed_event] = events

      assert started_event.name == [:bedrock, :test, :started]
      assert started_event.measurements.count == 1
      assert started_event.metadata.user == "alice"

      assert completed_event.name == [:bedrock, :test, :completed]
      assert completed_event.measurements.duration == 100
      assert completed_event.metadata.user == "alice"
    end

    test "captures specific events with attach_many/1" do
      handler =
        TestTelemetryHandler.attach_many([
          [:bedrock, :test, :started],
          [:bedrock, :test, :completed]
        ])

      on_exit(fn -> TestTelemetryHandler.detach(handler) end)

      # These should be captured
      :telemetry.execute([:bedrock, :test, :started], %{}, %{})
      :telemetry.execute([:bedrock, :test, :completed], %{}, %{})

      # This should NOT be captured (not in the list)
      :telemetry.execute([:bedrock, :test, :failed], %{}, %{})

      # Give GenServer time to process events
      Process.sleep(10)

      events = TestTelemetryHandler.get_events(handler)

      assert length(events) == 2

      event_names = Enum.map(events, & &1.name)
      assert [:bedrock, :test, :started] in event_names
      assert [:bedrock, :test, :completed] in event_names
      refute [:bedrock, :test, :failed] in event_names
    end

    test "clears events" do
      handler =
        TestTelemetryHandler.attach_many([
          [:bedrock, :test, :event1],
          [:bedrock, :test, :event2]
        ])

      on_exit(fn -> TestTelemetryHandler.detach(handler) end)

      :telemetry.execute([:bedrock, :test, :event1], %{}, %{})
      :telemetry.execute([:bedrock, :test, :event2], %{}, %{})

      # Give GenServer time to process events
      Process.sleep(10)

      events = TestTelemetryHandler.get_events(handler)
      assert length(events) == 2

      TestTelemetryHandler.clear_events(handler)

      events = TestTelemetryHandler.get_events(handler)
      assert Enum.empty?(events)
    end

    test "waits for events with timeout" do
      handler =
        TestTelemetryHandler.attach_many([
          [:bedrock, :test, :event1],
          [:bedrock, :test, :event2]
        ])

      on_exit(fn -> TestTelemetryHandler.detach(handler) end)

      # Emit events in a background task
      task =
        Task.async(fn ->
          Process.sleep(50)
          :telemetry.execute([:bedrock, :test, :event1], %{}, %{})
          Process.sleep(50)
          :telemetry.execute([:bedrock, :test, :event2], %{}, %{})
        end)

      # Wait for 2 events
      {:ok, events} = TestTelemetryHandler.wait_for_events(handler, 2, 200)

      assert length(events) == 2

      Task.await(task)
    end

    test "times out when waiting for events" do
      handler =
        TestTelemetryHandler.attach_many([
          [:bedrock, :test, :event1]
        ])

      on_exit(fn -> TestTelemetryHandler.detach(handler) end)

      # Don't emit any events

      # Should timeout
      {:timeout, events} = TestTelemetryHandler.wait_for_events(handler, 1, 50)

      assert Enum.empty?(events)
    end
  end
end
