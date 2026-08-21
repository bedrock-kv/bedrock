defmodule Bedrock.DataPlane.Log.Shale.DisplacementTest do
  @moduledoc """
  A log worker decides its own retirement (FDB's TLog isDisplaced /
  'DBInfoDoesNotContain'): the foreman relays each newly durable layout,
  and the worker checks its own membership. Log topology is
  epoch-constant, so the pushed log set IS the membership authority —
  guarded by epoch progression, so absence during an in-flight recovery
  is never a death sentence.
  """
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Bedrock.DataPlane.Log.Shale.Server
  alias Bedrock.DataPlane.Log.Shale.State

  defp state(overrides), do: struct!(%State{id: "log-1", epoch: 5, foreman: self()}, overrides)

  defp tsl(epoch, logs), do: %{epoch: epoch, logs: logs, sequencer: nil, proxies: [], resolvers: []}

  test "a progressed epoch whose log set omits this worker retires it" do
    capture_log(fn ->
      assert {:stop, {:shutdown, :displaced}, _t} =
               Server.handle_info({:tsl_updated, tsl(6, %{"log-2" => []})}, state([]))

      assert_received {:"$gen_cast", {:worker_retired, "log-1"}}
    end)
  end

  test "membership in the pushed epoch keeps the worker" do
    assert {:noreply, _t} = Server.handle_info({:tsl_updated, tsl(6, %{"log-1" => []})}, state([]))
    refute_received {:"$gen_cast", {:worker_retired, _}}
  end

  test "absence in a layout that has not progressed past our epoch is not displacement" do
    # An in-flight recovery relocks us into its epoch before its layout
    # becomes durable; a replayed push of our own (or an older) epoch
    # must never retire us.
    assert {:noreply, _t} = Server.handle_info({:tsl_updated, tsl(5, %{"log-2" => []})}, state([]))
    assert {:noreply, _t} = Server.handle_info({:tsl_updated, tsl(4, %{})}, state([]))
    refute_received {:"$gen_cast", {:worker_retired, _}}
  end

  test "a never-locked resurrection is judged by any completed layout" do
    # Cold boot: the foreman rehydrated us from a manifest, no recovery
    # has locked us, and the replayed push's layout doesn't name us —
    # we are a stale generation and self-dispose.
    capture_log(fn ->
      assert {:stop, {:shutdown, :displaced}, _t} =
               Server.handle_info({:tsl_updated, tsl(2, %{"log-2" => []})}, state(epoch: nil))

      assert_received {:"$gen_cast", {:worker_retired, "log-1"}}
    end)
  end

  test "a push without wiring shape is ignored" do
    assert {:noreply, _t} = Server.handle_info({:tsl_updated, %{}}, state([]))
  end
end
