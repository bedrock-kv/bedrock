defmodule Bedrock.ControlPlane.Coordinator.StateTest do
  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.Coordinator.State
  alias Bedrock.ControlPlane.Coordinator.State.Changes

  describe "transaction system layout lifecycle" do
    test "clearing the runnable layout preserves the recovery source" do
      recovery_source = %{logs: %{"old-log" => []}}
      runnable_layout = %{id: "layout-1", epoch: 7, logs: %{}, services: %{}}

      state =
        struct!(State,
          prior_core_state: recovery_source,
          transaction_system_layout: runnable_layout,
          tsl_subscribers: MapSet.new([self()])
        )

      result = Changes.clear_transaction_system_layout(state)

      assert result.transaction_system_layout == nil
      assert result.prior_core_state == recovery_source
      assert_receive {:tsl_updated, nil}
    end

    test "publishing a runnable layout refreshes the recovery source with its DURABLE half only" do
      # The broadcast goes out whole - subscribers need the pids. What is
      # kept as the next recovery's prior state is the core state
      # projected from it: the epoch's pids die with the epoch, and this
      # slot exists to outlive it.
      runnable_layout = %{
        id: "layout-2",
        epoch: 8,
        sequencer: self(),
        proxies: [self()],
        logs: %{"new-log" => [0]},
        services: %{}
      }

      state =
        struct!(State,
          prior_core_state: %{logs: %{"old-log" => []}},
          transaction_system_layout: nil,
          tsl_subscribers: MapSet.new([self()])
        )

      result = Changes.put_transaction_system_layout(state, runnable_layout)

      assert result.transaction_system_layout == runnable_layout
      assert result.prior_core_state == %{logs: %{"new-log" => [0]}}
      refute Map.has_key?(result.prior_core_state, :sequencer)
      assert_receive {:tsl_updated, ^runnable_layout}
    end
  end

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
end
