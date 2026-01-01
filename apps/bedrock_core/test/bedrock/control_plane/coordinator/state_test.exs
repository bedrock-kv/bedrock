defmodule Bedrock.ControlPlane.Coordinator.StateTest do
  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.Coordinator.State
  alias Bedrock.ControlPlane.Coordinator.State.Changes

  describe "service directory state changes" do
    test "put_service_directory replaces entire service directory" do
      initial_state = %State{
        service_directory: %{"old" => {:storage, {:old_worker, :old_node@host}}}
      }

      new_directory = %{"new" => {:log, {:new_worker, :new_node@host}}}

      result = Changes.put_service_directory(initial_state, new_directory)

      assert result.service_directory == new_directory
    end

    test "update_service_directory applies updater function" do
      initial_state = %State{
        service_directory: %{"service_1" => {:storage, {:worker1, :node1@host}}}
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
