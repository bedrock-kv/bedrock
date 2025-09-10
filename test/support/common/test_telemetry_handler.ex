defmodule Bedrock.Test.Common.TestTelemetryHandler do
  @moduledoc """
  A test telemetry handler that captures events for assertion in tests.

  Usage:

      # In your test
      setup do
        handler = TestTelemetryHandler.attach([:bedrock, :recovery])
        on_exit(fn -> TestTelemetryHandler.detach(handler) end)
        %{telemetry_handler: handler}
      end

      test "emits recovery started event", %{telemetry_handler: handler} do
        # Code that emits telemetry
        :telemetry.execute([:bedrock, :recovery, :started], %{}, %{attempt: 1})

        # Assert on captured events
        events = TestTelemetryHandler.get_events(handler)
        assert length(events) == 1

        [event] = events
        assert event.name == [:bedrock, :recovery, :started]
        assert event.metadata.attempt == 1
      end
  """

  use GenServer

  defstruct events: []

  @doc """
  Attaches a test telemetry handler for the given event prefix.
  Returns a handler reference that can be used to retrieve events and detach.

  Example:
      handler = TestTelemetryHandler.attach([:bedrock, :recovery])
      # ... emit some telemetry events ...
      events = TestTelemetryHandler.get_events(handler)
      TestTelemetryHandler.detach(handler)
  """
  def attach(event_prefix) when is_list(event_prefix) do
    handler_id = "test_telemetry_#{System.unique_integer([:positive])}"
    {:ok, pid} = GenServer.start_link(__MODULE__, [])

    :telemetry.attach(
      handler_id,
      event_prefix,
      &__MODULE__.handle_event/4,
      pid
    )

    %{
      id: handler_id,
      pid: pid,
      prefix: event_prefix
    }
  end

  @doc """
  Attaches a test telemetry handler for multiple specific events.
  Returns a handler reference that can be used to retrieve events and detach.

  Example:
      handler = TestTelemetryHandler.attach_many([
        [:bedrock, :recovery, :started],
        [:bedrock, :recovery, :completed]
      ])
  """
  def attach_many(event_names) when is_list(event_names) do
    handler_id = "test_telemetry_many_#{System.unique_integer([:positive])}"
    {:ok, pid} = GenServer.start_link(__MODULE__, [])

    :telemetry.attach_many(
      handler_id,
      event_names,
      &__MODULE__.handle_event/4,
      pid
    )

    %{
      id: handler_id,
      pid: pid,
      events: event_names
    }
  end

  @doc """
  Retrieves all captured telemetry events from the handler.
  Returns a list of maps with keys: :name, :measurements, :metadata
  """
  def get_events(handler) do
    GenServer.call(handler.pid, :get_events)
  end

  @doc """
  Clears all captured events from the handler.
  """
  def clear_events(handler) do
    GenServer.call(handler.pid, :clear_events)
  end

  @doc """
  Detaches the telemetry handler and stops the process.
  """
  def detach(handler) do
    :telemetry.detach(handler.id)

    if Process.alive?(handler.pid) do
      GenServer.stop(handler.pid)
    end
  end

  @doc """
  Waits for a specific number of events to be captured.
  Returns the events once the expected count is reached, or times out.
  """
  def wait_for_events(handler, expected_count, timeout \\ 1000) do
    end_time = System.monotonic_time(:millisecond) + timeout
    wait_for_events_loop(handler, expected_count, end_time)
  end

  defp wait_for_events_loop(handler, expected_count, end_time) do
    events = get_events(handler)

    cond do
      length(events) >= expected_count ->
        {:ok, events}

      System.monotonic_time(:millisecond) >= end_time ->
        {:timeout, events}

      true ->
        Process.sleep(10)
        wait_for_events_loop(handler, expected_count, end_time)
    end
  end

  # GenServer callbacks

  @impl true
  def init([]) do
    {:ok, %__MODULE__{}}
  end

  @impl true
  def handle_call(:get_events, _from, state) do
    {:reply, Enum.reverse(state.events), state}
  end

  @impl true
  def handle_call(:clear_events, _from, state) do
    {:reply, :ok, %{state | events: []}}
  end

  @impl true
  def handle_cast({:event, name, measurements, metadata}, state) do
    event = %{
      name: name,
      measurements: measurements,
      metadata: metadata,
      timestamp: System.monotonic_time(:millisecond)
    }

    {:noreply, %{state | events: [event | state.events]}}
  end

  # Telemetry handler function
  def handle_event(name, measurements, metadata, pid) when is_pid(pid) do
    GenServer.cast(pid, {:event, name, measurements, metadata})
  end
end
