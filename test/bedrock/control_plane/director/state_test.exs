defmodule Bedrock.ControlPlane.Director.StateTest do
  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.Director.State

  describe "Changes.put_state/2" do
    test "updates the state field" do
      state = %State{state: :starting}

      updated = State.Changes.put_state(state, :running)

      assert updated.state == :running
    end

    test "preserves other fields when updating state" do
      state = %State{
        state: :starting,
        epoch: 5,
        cluster: SomeCluster,
        lock_token: "test_token"
      }

      updated = State.Changes.put_state(state, :recovery)

      assert updated.state == :recovery
      assert updated.epoch == 5
      assert updated.cluster == SomeCluster
      assert updated.lock_token == "test_token"
    end
  end

  describe "Changes.update_config/2" do
    test "updates config using an updater function" do
      initial_config = %{
        coordinators: [node()],
        parameters: nil,
        policies: nil,
        version: 1
      }

      state = %State{config: initial_config}

      updated =
        State.Changes.update_config(state, fn config ->
          %{config | version: 2}
        end)

      assert updated.config.version == 2
    end

    test "preserves other fields when updating config" do
      initial_config = %{
        coordinators: [node()],
        parameters: nil,
        policies: nil,
        version: 1
      }

      state = %State{
        config: initial_config,
        state: :running,
        epoch: 3
      }

      updated =
        State.Changes.update_config(state, fn config ->
          %{config | version: 5}
        end)

      assert updated.config.version == 5
      assert updated.state == :running
      assert updated.epoch == 3
    end
  end
end
