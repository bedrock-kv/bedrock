defmodule Bedrock.Cluster.Gateway.CallsTest do
  use ExUnit.Case, async: true

  alias Bedrock.Cluster.Gateway.Calls
  alias Bedrock.Cluster.Gateway.State
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout

  describe "begin_transaction/2" do
    setup do
      # Create a minimal TransactionSystemLayout for testing
      tsl = TransactionSystemLayout.default()

      base_state = %State{
        node: :test_node,
        cluster: TestCluster,
        known_coordinator: :test_coordinator,
        transaction_system_layout: nil,
        deadline_by_version: %{},
        minimum_read_version: nil,
        lease_renewal_interval_in_ms: 5_000
      }

      %{tsl: tsl, base_state: base_state}
    end

    test "returns error when coordinator is unavailable", %{base_state: base_state} do
      state = %{base_state | known_coordinator: :unavailable}
      opts = [key_codec: TestKeyCodec, value_codec: TestValueCodec]

      {updated_state, result} = Calls.begin_transaction(state, opts)

      assert updated_state == state
      assert result == {:error, :unavailable}
    end

    test "raises when required key_codec option is missing", %{tsl: tsl, base_state: base_state} do
      state = %{base_state | transaction_system_layout: tsl}

      assert_raise KeyError, fn ->
        Calls.begin_transaction(state, value_codec: TestValueCodec)
      end
    end

    test "raises when required value_codec option is missing", %{tsl: tsl, base_state: base_state} do
      state = %{base_state | transaction_system_layout: tsl}

      assert_raise KeyError, fn ->
        Calls.begin_transaction(state, key_codec: TestKeyCodec)
      end
    end

    test "raises when both key_codec and value_codec options are missing", %{
      tsl: tsl,
      base_state: base_state
    } do
      state = %{base_state | transaction_system_layout: tsl}

      assert_raise KeyError, fn ->
        Calls.begin_transaction(state, [])
      end
    end

    test "attempts to start TransactionBuilder when TSL is cached", %{
      tsl: tsl,
      base_state: base_state
    } do
      state = %{base_state | transaction_system_layout: tsl}
      opts = [key_codec: TestKeyCodec, value_codec: TestValueCodec]

      # Since we can't easily mock TransactionBuilder.start_link in this environment,
      # we test that the function reaches the point where it would call start_link
      # by verifying it doesn't return :unavailable (which would happen if TSL lookup failed)
      {updated_state, result} = Calls.begin_transaction(state, opts)

      # State should remain unchanged when TSL is already cached
      assert updated_state == state

      # Result should be either {:ok, pid} or {:error, reason} from TransactionBuilder.start_link
      # We can't predict the exact result without mocking, but it shouldn't be :unavailable
      case result do
        {:ok, _pid} -> :ok
        {:error, reason} when reason != :unavailable -> :ok
        other -> flunk("Unexpected result: #{inspect(other)}")
      end
    end

    # Test the ensure_current_tsl logic through the public interface
    test "returns error when coordinator fetch would be needed but coordinator unavailable", %{
      base_state: base_state
    } do
      # No cached TSL and unavailable coordinator
      state = %{base_state | known_coordinator: :unavailable, transaction_system_layout: nil}
      opts = [key_codec: TestKeyCodec, value_codec: TestValueCodec]

      {updated_state, result} = Calls.begin_transaction(state, opts)

      assert updated_state == state
      assert result == {:error, :unavailable}
    end

    # This test verifies the TSL caching behavior
    test "preserves cached TSL in state when TSL is available", %{
      tsl: tsl,
      base_state: base_state
    } do
      state = %{base_state | transaction_system_layout: tsl}
      opts = [key_codec: TestKeyCodec, value_codec: TestValueCodec]

      {updated_state, _result} = Calls.begin_transaction(state, opts)

      # TSL should remain cached in the updated state
      assert updated_state.transaction_system_layout == tsl
    end
  end

  describe "renew_read_version_lease/2" do
    setup do
      base_state = %State{
        node: :test_node,
        cluster: TestCluster,
        known_coordinator: :test_coordinator,
        deadline_by_version: %{},
        minimum_read_version: nil,
        lease_renewal_interval_in_ms: 5_000
      }

      %{base_state: base_state}
    end

    test "returns lease_expired when read_version is below minimum_read_version", %{
      base_state: base_state
    } do
      state = %{base_state | minimum_read_version: 100}
      read_version = 50

      result = Calls.renew_read_version_lease(state, read_version)

      assert result == {:error, :lease_expired}
    end

    test "allows renewal when read_version equals minimum_read_version", %{
      base_state: base_state
    } do
      state = %{base_state | minimum_read_version: 100}
      read_version = 100

      {updated_state, result} = Calls.renew_read_version_lease(state, read_version)

      assert {:ok, renewal_interval} = result
      assert renewal_interval == 5_000

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

      {updated_state, result} = Calls.renew_read_version_lease(state, read_version)

      assert result == {:error, :lease_expired}
      # Should remove the expired entry from deadline_by_version
      assert updated_state.deadline_by_version == %{}
    end

    test "successfully renews lease when deadline is valid", %{base_state: base_state} do
      now = :erlang.monotonic_time(:millisecond)
      future_deadline = now + 10_000
      read_version = 100

      state = %{
        base_state
        | deadline_by_version: %{read_version => future_deadline}
      }

      {updated_state, result} = Calls.renew_read_version_lease(state, read_version)

      assert {:ok, renewal_interval} = result
      assert renewal_interval == 5_000

      # Should update the deadline
      new_deadline = Map.get(updated_state.deadline_by_version, read_version)
      assert new_deadline > now
      # New deadline should be roughly now + 10 + renewal_interval_in_ms
      expected_min = now + 5_010
      # Allow small timing variance
      expected_max = now + 5_020
      assert new_deadline >= expected_min
      assert new_deadline <= expected_max
    end

    test "handles version not in deadline_by_version map", %{base_state: base_state} do
      read_version = 100
      now = :erlang.monotonic_time(:millisecond)

      # State with empty deadline_by_version
      state = %{base_state | deadline_by_version: %{}}

      {updated_state, result} = Calls.renew_read_version_lease(state, read_version)

      assert {:ok, renewal_interval} = result
      assert renewal_interval == 5_000

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

      {updated_state, result} = Calls.renew_read_version_lease(state, read_version_1)

      assert {:ok, 5_000} = result

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
      now = :erlang.monotonic_time(:millisecond)
      future_deadline = now + 20_000
      read_version = 100

      state = %{
        base_state
        | deadline_by_version: %{read_version => future_deadline},
          lease_renewal_interval_in_ms: custom_renewal_interval
      }

      {updated_state, result} = Calls.renew_read_version_lease(state, read_version)

      assert {:ok, ^custom_renewal_interval} = result

      # Verify the calculation: new_lease_deadline_in_ms = now + 10 + renewal_deadline_in_ms
      new_deadline = Map.get(updated_state.deadline_by_version, read_version)
      # Allow for small timing differences since we can't control exact timing
      expected_min = now + 10 + custom_renewal_interval - 5
      expected_max = now + 10 + custom_renewal_interval + 5

      assert new_deadline >= expected_min
      assert new_deadline <= expected_max
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

      {updated_state, result} = Calls.renew_read_version_lease(state, read_version)

      assert result == {:error, :lease_expired}
      assert updated_state.deadline_by_version == %{}
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

      {updated_state, result} = Calls.renew_read_version_lease(state, read_version)
      assert {:ok, 1} = result

      # With minimum renewal interval, new deadline should be now + 10 + 1
      new_deadline = Map.get(updated_state.deadline_by_version, read_version)
      assert new_deadline >= now + 10
      # Allow for timing variance
      assert new_deadline <= now + 15
    end

    test "handles large renewal intervals", %{base_state: base_state} do
      now = :erlang.monotonic_time(:millisecond)
      future_deadline = now + 100_000
      read_version = 500
      # 1 minute
      large_interval = 60_000

      state = %{
        base_state
        | deadline_by_version: %{read_version => future_deadline},
          lease_renewal_interval_in_ms: large_interval
      }

      {updated_state, result} = Calls.renew_read_version_lease(state, read_version)
      assert {:ok, ^large_interval} = result

      new_deadline = Map.get(updated_state.deadline_by_version, read_version)
      expected_min = now + 10 + large_interval - 10
      expected_max = now + 10 + large_interval + 10

      assert new_deadline >= expected_min
      assert new_deadline <= expected_max
    end
  end

  describe "integration scenarios" do
    test "begin_transaction and renew_read_version_lease work with same state" do
      # Test that both functions work with the same state structure
      base_state = %State{
        node: :test_node,
        cluster: TestCluster,
        known_coordinator: :test_coordinator,
        transaction_system_layout: TransactionSystemLayout.default(),
        deadline_by_version: %{100 => :erlang.monotonic_time(:millisecond) + 10_000},
        minimum_read_version: 50,
        lease_renewal_interval_in_ms: 5_000
      }

      # Test begin_transaction with this state
      opts = [key_codec: TestKeyCodec, value_codec: TestValueCodec]
      {_updated_state1, result1} = Calls.begin_transaction(base_state, opts)

      # Should not return :unavailable since coordinator and TSL are available
      case result1 do
        {:ok, _pid} -> :ok
        {:error, reason} when reason != :unavailable -> :ok
        {:error, :unavailable} -> flunk("Should not return :unavailable with valid state")
      end

      # Test renew_read_version_lease with same state
      {updated_state2, result2} = Calls.renew_read_version_lease(base_state, 100)
      assert {:ok, 5_000} = result2
      assert Map.has_key?(updated_state2.deadline_by_version, 100)
    end

    test "state transitions maintain data integrity" do
      # Start with minimal state
      initial_state = %State{
        node: :test_node,
        cluster: TestCluster,
        known_coordinator: :test_coordinator,
        transaction_system_layout: nil,
        deadline_by_version: %{},
        minimum_read_version: nil,
        lease_renewal_interval_in_ms: 2_000
      }

      # Add some lease data
      now = :erlang.monotonic_time(:millisecond)
      state_with_lease = %{initial_state | deadline_by_version: %{200 => now + 5_000}}

      # Renew lease
      {updated_state, result} = Calls.renew_read_version_lease(state_with_lease, 200)
      assert {:ok, 2_000} = result

      # Verify state integrity
      assert updated_state.node == :test_node
      assert updated_state.cluster == TestCluster
      assert updated_state.known_coordinator == :test_coordinator
      assert updated_state.transaction_system_layout == nil
      assert updated_state.minimum_read_version == nil
      assert updated_state.lease_renewal_interval_in_ms == 2_000
      assert Map.has_key?(updated_state.deadline_by_version, 200)
    end
  end
end
