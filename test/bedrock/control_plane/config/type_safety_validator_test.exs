defmodule Bedrock.ControlPlane.Config.TypeSafetyValidatorTest do
  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.Config.TypeSafetyValidator

  describe "validate_layout_type_safety/1" do
    test "accepts a well-typed transaction system layout" do
      layout = %{
        logs: %{"log_1" => [0, 100]},
        resolvers: [%{start_key: "", resolver: {:vacancy, 1}}]
      }

      assert :ok = TypeSafetyValidator.validate_layout_type_safety(layout)
    end

    test "rejects a log entry with binary versions instead of integer ranges" do
      layout = %{logs: %{"log_1" => [<<1, 2, 3>>, <<4, 5, 6>>]}, resolvers: []}

      assert {:error, {:invalid_logs, "log_1", _}} = TypeSafetyValidator.validate_layout_type_safety(layout)
    end

    test "rejects a malformed resolver entry" do
      layout = %{logs: %{}, resolvers: [:not_a_resolver]}

      assert {:error, {:invalid_resolvers, _}} = TypeSafetyValidator.validate_layout_type_safety(layout)
    end
  end

  describe "validate_core_state_type_safety/1" do
    test "accepts a fresh core state naming no logs" do
      assert :ok = TypeSafetyValidator.validate_core_state_type_safety(%{logs: %{}})
    end

    test "accepts a core state naming prior logs, with no :resolvers key at all" do
      assert :ok = TypeSafetyValidator.validate_core_state_type_safety(%{logs: %{"log_1" => [0, 5]}})
    end

    test "rejects a log entry with binary versions instead of integer ranges" do
      core_state = %{logs: %{"log_1" => [<<1, 2, 3>>, <<4, 5, 6>>]}}

      assert {:error, {:invalid_logs, "log_1", _}} = TypeSafetyValidator.validate_core_state_type_safety(core_state)
    end
  end

  describe "assert_type_safety!/1" do
    test "returns the layout unchanged when well-typed" do
      layout = %{logs: %{}, resolvers: []}

      assert ^layout = TypeSafetyValidator.assert_type_safety!(layout)
    end

    test "raises when the layout fails type validation" do
      layout = %{logs: %{"log_1" => [<<1, 2, 3>>, <<4, 5, 6>>]}, resolvers: []}

      assert_raise ArgumentError, ~r/Type safety assertion failed/, fn ->
        TypeSafetyValidator.assert_type_safety!(layout)
      end
    end
  end
end
