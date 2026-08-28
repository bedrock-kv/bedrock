defmodule Bedrock.DataPlane.Resolver.TracingTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Bedrock.DataPlane.Resolver.Telemetry
  alias Bedrock.DataPlane.Resolver.Tracing
  alias Bedrock.DataPlane.Version

  setup do
    Tracing.start()
    on_exit(&Tracing.stop/0)
    :ok
  end

  # Our handler specifically, not merely "something is attached here" — a
  # detached tracer is the failure mode under test, and another test's
  # handler on the same prefix must not be able to mask it.
  @handler_id "bedrock_trace_data_plane_resolver"

  defp our_handlers, do: [:bedrock, :resolver] |> :telemetry.list_handlers() |> Enum.filter(&(&1.id == @handler_id))

  defp attached?, do: our_handlers() != []

  # The defect this pins: the handler read `measurements.transaction_count`
  # and `metadata.last_version`, which no emitter ever sent. :telemetry
  # detaches a handler that raises, so enabling resolver tracing silently
  # tore itself down on the first resolved batch. Driving the REAL emitters
  # through the REAL handler is the only shape of test that catches it —
  # the telemetry test attaches its own handler and so proves nothing about
  # this one.
  describe "the handler survives every event its emitters produce" do
    test "received" do
      log = capture_log(fn -> Telemetry.emit_received(["tx1", "tx2"], Version.from_integer(100)) end)

      assert log =~ "Received 2 transactions"
      assert log =~ "next_version=<0,0,0,0,0,0,0,100>"
      assert attached?()
    end

    test "completed" do
      log = capture_log(fn -> Telemetry.emit_completed(["tx1", "tx2", "tx3"], [1], Version.from_integer(200)) end)

      assert log =~ "Completed 3 transactions (1 aborted)"
      assert attached?()
    end

    test "waiting_list_inserted" do
      log =
        capture_log(fn ->
          Telemetry.emit_waiting_list_inserted(["tx1"], %{a: 1, b: 2}, Version.from_integer(15))
        end)

      assert log =~ "Inserted 1 transactions into waiting list (size: 2)"
      assert attached?()
    end

    test "waiting_resolved" do
      log = capture_log(fn -> Telemetry.emit_waiting_resolved(["tx1", "tx2"], [], Version.from_integer(50)) end)

      assert log =~ "Resolved waiting transaction: 2 transactions"
      assert attached?()
    end

    test "a full batch's worth of events leaves the handler attached" do
      version = Version.from_integer(1)

      capture_log(fn ->
        Telemetry.emit_received(["tx"], version)
        Telemetry.emit_waiting_list_inserted(["tx"], %{}, version)
        Telemetry.emit_completed(["tx"], [], version)
        Telemetry.emit_waiting_resolved(["tx"], [], version)
      end)

      assert attached?()
    end
  end

  describe "subscriptions" do
    test "subscribes to exactly the events Telemetry emits" do
      emitted =
        :exports
        |> Telemetry.module_info()
        |> Enum.filter(fn {name, _arity} -> name |> Atom.to_string() |> String.starts_with?("emit_") end)
        |> MapSet.new(fn {name, _arity} ->
          name |> Atom.to_string() |> String.replace_prefix("emit_", "") |> String.to_atom()
        end)

      subscribed =
        MapSet.new(our_handlers(), fn %{event_name: [:bedrock, :resolver, :resolve_transactions, event]} -> event end)

      assert MapSet.equal?(subscribed, emitted),
             "tracing subscribes to #{inspect(MapSet.difference(subscribed, emitted))} with no emitter, " <>
               "and misses #{inspect(MapSet.difference(emitted, subscribed))}"
    end
  end
end
