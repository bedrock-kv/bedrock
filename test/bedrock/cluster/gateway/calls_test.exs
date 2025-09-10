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
      transaction_system_layout: nil,
      deadline_by_version: %{},
      minimum_read_version: nil,
      lease_renewal_interval_in_ms: 5_000
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

  defp calculate_expected_deadline(current_time, renewal_interval) do
    current_time + 10 + renewal_interval
  end

  defp assert_deadline_calculation(updated_state, read_version, current_time, renewal_interval) do
    new_deadline = Map.get(updated_state.deadline_by_version, read_version)
    expected_deadline = calculate_expected_deadline(current_time, renewal_interval)
    assert new_deadline == expected_deadline
    new_deadline
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

  describe "renew_read_version_lease/2" do
    setup do
      %{base_state: build_base_state()}
    end

    test "returns lease_expired when read_version is below minimum_read_version", %{
      base_state: base_state
    } do
      state = %{base_state | minimum_read_version: 100}

      assert {^state, {:error, :lease_expired}} = Calls.renew_read_version_lease(state, 50)
    end

    test "allows renewal when read_version equals minimum_read_version", %{
      base_state: base_state
    } do
      state = %{base_state | minimum_read_version: 100}
      read_version = 100

      assert {updated_state, {:ok, 5_000}} = Calls.renew_read_version_lease(state, read_version)

      # Should add the version to deadline_by_version
      assert Map.has_key?(updated_state.deadline_by_version, read_version)
    end

    test "returns lease_expired when deadline has passed", %{base_state: base_state} do
      now = :erlang.monotonic_time(:millisecond)
      expired_deadline = now - 1000
      read_version = 100

      state = %{
        base_state
        | deadline_by_version: %{read_version => expired_deadline}
      }

      assert {%State{deadline_by_version: %{}}, {:error, :lease_expired}} =
               Calls.renew_read_version_lease(state, read_version)
    end

    test "successfully renews lease when deadline is valid", %{base_state: base_state} do
      # Fixed time for deterministic test
      now = 1_000_000
      future_deadline = now + 10_000
      read_version = 100
      time_fn = fn -> now end

      state = %{
        base_state
        | deadline_by_version: %{read_version => future_deadline}
      }

      assert {updated_state, {:ok, 5_000}} = Calls.renew_read_version_lease(state, read_version, time_fn)

      # Should update the deadline with exact calculation
      assert_deadline_calculation(updated_state, read_version, now, 5_000)
    end

    test "handles version not in deadline_by_version map", %{base_state: base_state} do
      read_version = 100
      now = :erlang.monotonic_time(:millisecond)

      # State with empty deadline_by_version
      state = %{base_state | deadline_by_version: %{}}

      assert {updated_state, {:ok, 5_000}} = Calls.renew_read_version_lease(state, read_version)

      # Should add new entry to deadline_by_version
      new_deadline = Map.get(updated_state.deadline_by_version, read_version)
      assert new_deadline > now
    end

    test "preserves other versions in deadline_by_version when renewing", %{
      base_state: base_state
    } do
      now = :erlang.monotonic_time(:millisecond)
      future_deadline_1 = now + 10_000
      future_deadline_2 = now + 15_000
      read_version_1 = 100
      read_version_2 = 200

      state = %{
        base_state
        | deadline_by_version: %{
            read_version_1 => future_deadline_1,
            read_version_2 => future_deadline_2
          }
      }

      assert {updated_state, {:ok, 5_000}} = Calls.renew_read_version_lease(state, read_version_1)

      # Should preserve read_version_2 deadline
      assert Map.get(updated_state.deadline_by_version, read_version_2) == future_deadline_2
      # Should update read_version_1 deadline
      new_deadline_1 = Map.get(updated_state.deadline_by_version, read_version_1)
      assert new_deadline_1 != future_deadline_1
      assert new_deadline_1 > now
    end

    test "calculates correct new lease deadline with custom renewal interval", %{
      base_state: base_state
    } do
      # Test the specific calculation: now + 10 + renewal_deadline_in_ms
      custom_renewal_interval = 3_000
      # Use deterministic time for testing
      current_time = 1_000_000
      future_deadline = current_time + 20_000
      read_version = 100

      state = %{
        base_state
        | deadline_by_version: %{read_version => future_deadline},
          lease_renewal_interval_in_ms: custom_renewal_interval
      }

      time_fn = fn -> current_time end

      assert {updated_state, {:ok, ^custom_renewal_interval}} =
               Calls.renew_read_version_lease(state, read_version, time_fn)

      # Verify the calculation: new_lease_deadline_in_ms = now + 10 + renewal_deadline_in_ms
      assert_deadline_calculation(updated_state, read_version, current_time, custom_renewal_interval)
    end

    test "handles boundary case when deadline exactly equals current time", %{
      base_state: base_state
    } do
      now = :erlang.monotonic_time(:millisecond)
      read_version = 100

      # Set deadline to exactly current time (should be considered expired)
      state = %{
        base_state
        | deadline_by_version: %{read_version => now}
      }

      assert {%State{deadline_by_version: %{}}, {:error, :lease_expired}} =
               Calls.renew_read_version_lease(state, read_version)
    end

    test "works with minimum renewal interval", %{base_state: base_state} do
      now = :erlang.monotonic_time(:millisecond)
      future_deadline = now + 100
      read_version = 150

      state = %{
        base_state
        | deadline_by_version: %{read_version => future_deadline},
          lease_renewal_interval_in_ms: 1
      }

      assert {updated_state, {:ok, 1}} = Calls.renew_read_version_lease(state, read_version)

      # With minimum renewal interval, new deadline should be now + 10 + 1
      new_deadline = Map.get(updated_state.deadline_by_version, read_version)

      # Calculate the actual interval and verify it's within expected range
      actual_interval = new_deadline - now
      # 10ms + renewal_interval_in_ms
      expected_interval = 10 + 1

      # Allow for timing variance (minimum should be expected, max should be expected + 5ms)
      assert actual_interval >= expected_interval
      assert actual_interval <= expected_interval + 5
    end

    test "handles large renewal intervals", %{base_state: base_state} do
      # Mock time to avoid timing-sensitive test failures
      # Fixed time point
      mock_now = 1_000_000
      future_deadline = mock_now + 100_000
      read_version = 500
      # 1 minute
      large_interval = 60_000

      state = %{
        base_state
        | deadline_by_version: %{read_version => future_deadline},
          lease_renewal_interval_in_ms: large_interval
      }

      # Use mocked time function
      mock_time_fn = fn -> mock_now end

      assert {updated_state, {:ok, ^large_interval}} = Calls.renew_read_version_lease(state, read_version, mock_time_fn)

      # With mocked time, this should be exact
      assert_deadline_calculation(updated_state, read_version, mock_now, large_interval)
    end
  end
end
