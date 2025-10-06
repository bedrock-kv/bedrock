defmodule Bedrock.Cluster.Gateway.CallsTest do
  use ExUnit.Case, async: true

  alias Bedrock.Cluster.Gateway.Calls
  alias Bedrock.Cluster.Gateway.State
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout

  # Shared setup functions
  defp build_base_state(overrides \\ %{}) do
    defaults = %{
      node: :test_node,
      cluster: TestCluster,
      known_coordinator: :test_coordinator,
      transaction_system_layout: nil
    }

    struct(State, Map.merge(defaults, overrides))
  end

  defp assert_valid_transaction_result(result) do
    case result do
      {:ok, _pid} -> :ok
      {:error, reason} when reason != :unavailable -> :ok
      other -> flunk("Unexpected result: #{inspect(other)}")
    end
  end

  describe "begin_transaction/2" do
    setup do
      tsl = TransactionSystemLayout.default()
      base_state = build_base_state()
      %{tsl: tsl, base_state: base_state}
    end

    test "returns error when coordinator is unavailable", %{base_state: base_state} do
      state = %{base_state | known_coordinator: :unavailable}

      assert {^state, {:error, :unavailable}} = Calls.begin_transaction(state, [])
    end

    test "works with various option formats when TSL is cached", %{tsl: tsl, base_state: base_state} do
      state = %{base_state | transaction_system_layout: tsl}

      test_cases = [
        [],
        [some_option: :value],
        [multiple: :options, ignored: true]
      ]

      for opts <- test_cases do
        assert {^state, result} = Calls.begin_transaction(state, opts)
        assert_valid_transaction_result(result)
      end
    end

    test "returns error when TSL is missing and coordinator unavailable", %{base_state: base_state} do
      state = %{base_state | known_coordinator: :unavailable, transaction_system_layout: nil}

      assert {^state, {:error, :unavailable}} = Calls.begin_transaction(state, [])
    end

    # This test verifies the TSL caching behavior
    test "preserves cached TSL in state when TSL is available", %{
      tsl: tsl,
      base_state: base_state
    } do
      state = %{base_state | transaction_system_layout: tsl}
      opts = []

      {updated_state, _result} = Calls.begin_transaction(state, opts)

      # TSL should remain cached in the updated state
      assert updated_state.transaction_system_layout == tsl
    end
  end
end
