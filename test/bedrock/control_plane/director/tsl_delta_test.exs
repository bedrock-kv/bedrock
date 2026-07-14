defmodule Bedrock.ControlPlane.Director.TSLDeltaTest do
  use ExUnit.Case, async: true

  import Bedrock.Test.TelemetryTestHelper

  alias Bedrock.ControlPlane.Director.Server
  alias Bedrock.ControlPlane.Director.State

  defp base_tsl(shard_materializers) do
    %{
      id: 1,
      epoch: 5,
      director: self(),
      sequencer: nil,
      rate_keeper: nil,
      proxies: [],
      resolvers: [],
      logs: %{},
      services: %{},
      shard_layout: %{},
      shard_materializers: shard_materializers
    }
  end

  defp director_state(opts \\ []) do
    %State{
      state: :running,
      epoch: Keyword.get(opts, :epoch, 5),
      cluster: TestCluster,
      coordinator: Keyword.get(opts, :coordinator, self()),
      transaction_system_layout: Keyword.get(opts, :tsl, base_tsl(%{}))
    }
  end

  describe "handle_call({:apply_tsl_delta, delta, epoch}, ...)" do
    test "applies a put delta to shard_materializers and notifies the coordinator" do
      materializer = spawn(fn -> Process.sleep(:infinity) end)
      state = director_state()

      assert {:reply, :ok, updated_state} =
               Server.handle_call({:apply_tsl_delta, %{1 => materializer}, 5}, {self(), make_ref()}, state)

      assert updated_state.transaction_system_layout.shard_materializers == %{1 => materializer}

      assert_receive {:"$gen_cast", {:notify_transaction_system_layout, broadcast_tsl}}
      assert broadcast_tsl == updated_state.transaction_system_layout
    end

    test "applies a :remove delta by deleting the shard entry" do
      old_materializer = spawn(fn -> Process.sleep(:infinity) end)
      state = director_state(tsl: base_tsl(%{1 => old_materializer, 2 => old_materializer}))

      assert {:reply, :ok, updated_state} =
               Server.handle_call({:apply_tsl_delta, %{1 => :remove}, 5}, {self(), make_ref()}, state)

      assert updated_state.transaction_system_layout.shard_materializers == %{2 => old_materializer}
    end

    test "mixed delta puts and removes in one application" do
      old_materializer = spawn(fn -> Process.sleep(:infinity) end)
      new_materializer = spawn(fn -> Process.sleep(:infinity) end)
      state = director_state(tsl: base_tsl(%{1 => old_materializer, 2 => old_materializer}))

      delta = %{1 => new_materializer, 2 => :remove, 3 => new_materializer}

      assert {:reply, :ok, updated_state} =
               Server.handle_call({:apply_tsl_delta, delta, 5}, {self(), make_ref()}, state)

      assert updated_state.transaction_system_layout.shard_materializers ==
               %{1 => new_materializer, 3 => new_materializer}
    end

    test "creates the shard_materializers map when the TSL lacks one" do
      materializer = spawn(fn -> Process.sleep(:infinity) end)
      tsl = Map.delete(base_tsl(%{}), :shard_materializers)
      state = director_state(tsl: tsl)

      assert {:reply, :ok, updated_state} =
               Server.handle_call({:apply_tsl_delta, %{7 => materializer}, 5}, {self(), make_ref()}, state)

      assert updated_state.transaction_system_layout.shard_materializers == %{7 => materializer}
    end

    test "rejects a delta carrying a stale epoch with :newer_epoch_exists" do
      state = director_state(epoch: 5)
      materializer = spawn(fn -> Process.sleep(:infinity) end)

      assert {:reply, {:error, :newer_epoch_exists}, unchanged_state} =
               Server.handle_call({:apply_tsl_delta, %{1 => materializer}, 4}, {self(), make_ref()}, state)

      assert unchanged_state.transaction_system_layout == state.transaction_system_layout
      refute_receive {:"$gen_cast", {:notify_transaction_system_layout, _}}
    end

    test "rejects a delta carrying an unknown (newer) epoch with :newer_epoch_exists" do
      state = director_state(epoch: 5)
      materializer = spawn(fn -> Process.sleep(:infinity) end)

      assert {:reply, {:error, :newer_epoch_exists}, _} =
               Server.handle_call({:apply_tsl_delta, %{1 => materializer}, 6}, {self(), make_ref()}, state)

      refute_receive {:"$gen_cast", {:notify_transaction_system_layout, _}}
    end

    test "rejects a delta before recovery has produced a TSL with :unavailable" do
      state = director_state(tsl: nil)
      materializer = spawn(fn -> Process.sleep(:infinity) end)

      assert {:reply, {:error, :unavailable}, _} =
               Server.handle_call({:apply_tsl_delta, %{1 => materializer}, 5}, {self(), make_ref()}, state)

      refute_receive {:"$gen_cast", {:notify_transaction_system_layout, _}}
    end

    test "emits a tsl_delta_applied telemetry event" do
      attach_telemetry_reflector(self(), [[:bedrock, :director, :tsl_delta_applied]], "tsl-delta-telemetry")

      materializer = spawn(fn -> Process.sleep(:infinity) end)
      delta = %{1 => materializer, 2 => :remove}
      state = director_state()

      assert {:reply, :ok, _} = Server.handle_call({:apply_tsl_delta, delta, 5}, {self(), make_ref()}, state)

      assert_receive {:telemetry_event, [:bedrock, :director, :tsl_delta_applied], measurements, metadata}
      assert measurements == %{shard_count: 2}
      assert metadata.epoch == 5
      assert metadata.cluster == TestCluster
      assert metadata.delta == delta
    end
  end
end
