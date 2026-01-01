defmodule Bedrock.ControlPlane.ConfigTest do
  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.Config
  alias Bedrock.ControlPlane.Config.Parameters
  alias Bedrock.ControlPlane.Config.RecoveryAttempt

  describe "key_range/2" do
    test "creates a key range when min_key is less than max_key" do
      assert {"a", "z"} = Config.key_range("a", "z")
      assert {<<0>>, <<255>>} = Config.key_range(<<0>>, <<255>>)
    end
  end

  describe "new/1" do
    test "creates a new config with coordinators" do
      coordinators = [node(), :node1@localhost, :node2@localhost]
      config = Config.new(coordinators)

      assert config.coordinators == coordinators
      assert config.parameters
      assert config.policies
    end
  end

  describe "allow_volunteer_nodes_to_join?/1" do
    test "returns true by default from a real config" do
      config = Config.new([node()])
      # Should return true by default
      assert Config.allow_volunteer_nodes_to_join?(config) == true
    end
  end

  describe "coordinators/1" do
    test "returns the coordinators list" do
      coordinators = [node(), :node1@localhost]
      config = Config.new(coordinators)
      assert Config.coordinators(config) == coordinators
    end
  end

  describe "ping_rate_in_ms/1" do
    test "converts ping rate from Hz to milliseconds" do
      config = Config.new([node()])
      # Default ping rate should give us a valid millisecond value
      assert is_integer(Config.ping_rate_in_ms(config))
      assert Config.ping_rate_in_ms(config) > 0
    end
  end

  describe "Changes.put_recovery_attempt/2" do
    test "adds a recovery attempt to the config" do
      config = Config.new([node()])
      recovery_attempt = %RecoveryAttempt{}

      updated = Config.Changes.put_recovery_attempt(config, recovery_attempt)

      assert updated.recovery_attempt == recovery_attempt
    end

    test "replaces existing recovery attempt" do
      old_attempt = %RecoveryAttempt{}
      new_attempt = %RecoveryAttempt{}
      config = Map.put(Config.new([node()]), :recovery_attempt, old_attempt)

      updated = Config.Changes.put_recovery_attempt(config, new_attempt)

      assert updated.recovery_attempt == new_attempt
    end

    test "can set recovery attempt to nil" do
      config = Map.put(Config.new([node()]), :recovery_attempt, %RecoveryAttempt{})

      updated = Config.Changes.put_recovery_attempt(config, nil)

      assert updated.recovery_attempt == nil
    end
  end

  describe "Changes.put_parameters/2" do
    test "updates the parameters" do
      config = Config.new([node()])
      new_params = Parameters.new([node()])

      updated = Config.Changes.put_parameters(config, new_params)

      assert updated.parameters == new_params
    end
  end

  describe "Changes.update_recovery_attempt!/2" do
    test "updates recovery attempt using a function" do
      recovery_attempt = %RecoveryAttempt{epoch: 1}
      config = Map.put(Config.new([node()]), :recovery_attempt, recovery_attempt)

      updated =
        Config.Changes.update_recovery_attempt!(config, fn attempt ->
          %{attempt | epoch: 2}
        end)

      assert updated.recovery_attempt.epoch == 2
    end
  end

  describe "Changes.update_parameters/2" do
    test "updates parameters using an updater function" do
      config = Config.new([node()])
      original_params = config.parameters

      updated =
        Config.Changes.update_parameters(config, fn params ->
          %{params | ping_rate_in_hz: 20}
        end)

      assert updated.parameters.ping_rate_in_hz == 20
      refute updated.parameters == original_params
    end
  end
end
