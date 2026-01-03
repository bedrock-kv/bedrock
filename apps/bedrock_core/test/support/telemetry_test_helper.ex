defmodule Bedrock.Test.TelemetryTestHelper do
  @moduledoc """
  Test helpers for working with telemetry events in a clean, deterministic way.

  This module provides utilities for attaching telemetry handlers that automatically
  clean up after themselves and send events to test processes in a consistent format.
  """

  import ExUnit.Callbacks, only: [on_exit: 1]

  @doc """
  Attaches telemetry handlers that send events to a target process and automatically
  clean up when the test exits.

  ## Parameters

  - `target_pid` - The process to send telemetry events to
  - `events` - A single event tuple or list of event tuples to listen for
  - `event_name` - A unique name for this telemetry attachment

  ## Examples

      # Listen for a single event
      attach_telemetry_reflector(
        self(),
        [:bedrock, :resolver, :resolve_transactions, :completed],
        "my-test-events"
      )

      # Listen for multiple events
      attach_telemetry_reflector(
        self(),
        [
          [:bedrock, :resolver, :resolve_transactions, :processing],
          [:bedrock, :resolver, :resolve_transactions, :completed]
        ],
        "my-test-events"
      )

  Events will be sent to the target process in the format:
  `{:telemetry_event, event, measurements, metadata}`
  """
  def attach_telemetry_reflector(target_pid, events, event_name) when is_list(events) do
    :telemetry.attach_many(
      event_name,
      events,
      &__MODULE__.telemetry_reflector/4,
      target_pid
    )

    on_exit(fn ->
      :telemetry.detach(event_name)
    end)
  end

  def attach_telemetry_reflector(target_pid, event, event_name),
    do: attach_telemetry_reflector(target_pid, [event], event_name)

  @doc """
  Expects a telemetry event to be received and returns the event data.

  ## Parameters

  - `expected_event` - The event tuple to expect (e.g., `[:bedrock, :resolver, :completed]`)
  - `timeout` - Optional timeout in milliseconds (defaults to 100ms)

  ## Examples

      # Basic usage with default timeout
      {measurements, metadata} = expect_telemetry([:bedrock, :resolver, :completed])

      # With custom timeout
      {measurements, metadata} = expect_telemetry([:bedrock, :resolver, :completed], 5000)

      # Pattern match on specific metadata
      {_measurements, metadata} = expect_telemetry([:bedrock, :resolver, :completed])
      assert metadata.next_version == expected_version
  """
  def expect_telemetry(expected_event, timeout \\ 100) do
    receive do
      {:telemetry_event, ^expected_event, measurements, metadata} ->
        {measurements, metadata}
    after
      timeout ->
        ExUnit.Assertions.flunk(
          "Expected telemetry event #{inspect(expected_event)} but none was received within #{timeout}ms"
        )
    end
  end

  @doc false
  def telemetry_reflector(event, measurements, metadata, target_pid),
    do: send(target_pid, {:telemetry_event, event, measurements, metadata})
end
