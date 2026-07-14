defmodule Bedrock.ControlPlane.Coordinator.StateTest do
  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.Coordinator.State
  alias Bedrock.ControlPlane.Coordinator.State.Changes

  describe "service directory state changes" do
    test "put_service_directory replaces entire service directory" do
      initial_state = %State{
        service_directory: %{"old" => {:materializer, {:old_worker, :old_node@host}}}
      }

      new_directory = %{"new" => {:log, {:new_worker, :new_node@host}}}

      result = Changes.put_service_directory(initial_state, new_directory)

      assert result.service_directory == new_directory
    end

    test "update_service_directory applies updater function" do
      initial_state = %State{
        service_directory: %{"service_1" => {:materializer, {:worker1, :node1@host}}}
      }

      updater = fn directory ->
        directory
        |> Map.put("service_2", {:log, {:worker2, :node2@host}})
        |> Map.delete("service_1")
      end

      result = Changes.update_service_directory(initial_state, updater)

      expected_directory = %{"service_2" => {:log, {:worker2, :node2@host}}}
      assert result.service_directory == expected_directory
    end

    test "state initializes with empty service directory" do
      state = %State{}
      assert state.service_directory == %{}
    end
  end

  describe "TSL subscriber state changes" do
    test "add_tsl_subscriber is idempotent" do
      state =
        %State{}
        |> Changes.add_tsl_subscriber(self())
        |> Changes.add_tsl_subscriber(self())

      assert state.tsl_subscribers == MapSet.new([self()])
    end

    test "remove_tsl_subscriber removes only the given subscriber" do
      other = spawn(fn -> Process.sleep(:infinity) end)

      state =
        %State{}
        |> Changes.add_tsl_subscriber(self())
        |> Changes.add_tsl_subscriber(other)
        |> Changes.remove_tsl_subscriber(other)

      assert state.tsl_subscribers == MapSet.new([self()])
    end

    test "broadcast_tsl_update sends {:tsl_updated, tsl} to every subscriber" do
      tsl = %{epoch: 1, shard_materializers: %{}}

      state = Changes.add_tsl_subscriber(%State{}, self())

      assert ^state = Changes.broadcast_tsl_update(state, tsl)
      assert_receive {:tsl_updated, ^tsl}
    end

    test "put_transaction_system_layout marks the TSL live; mark_tsl_stale clears it" do
      tsl = %{epoch: 1, shard_materializers: %{}}

      state = Changes.put_transaction_system_layout(%State{}, tsl)
      assert state.tsl_live?

      state = Changes.mark_tsl_stale(state)
      refute state.tsl_live?
      # The TSL itself is retained as recovery input.
      assert state.transaction_system_layout == tsl
    end

    test "broadcast_tsl_update tolerates dead subscribers" do
      dead = spawn(fn -> :ok end)
      ref = Process.monitor(dead)
      assert_receive {:DOWN, ^ref, :process, ^dead, _}

      tsl = %{epoch: 1, shard_materializers: %{}}

      state =
        %State{}
        |> Changes.add_tsl_subscriber(dead)
        |> Changes.add_tsl_subscriber(self())

      assert ^state = Changes.broadcast_tsl_update(state, tsl)
      assert_receive {:tsl_updated, ^tsl}
    end
  end
end
