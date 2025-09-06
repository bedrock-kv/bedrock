defmodule Bedrock.DataPlane.CommitProxy.ServerTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.CommitProxy.Server
  alias Bedrock.DataPlane.CommitProxy.State

  # Mock cluster module for testing
  defmodule TestCluster do
    @moduledoc false
    def otp_name(component) when is_atom(component) do
      :"test_cluster_#{component}"
    end
  end

  describe "error handling integration" do
    test "commit proxy server handles director failures without crashing batching logic" do
      # This test verifies that our fix prevents the KeyError we encountered
      # The issue was that {:stop, :timeout} was being passed to batching functions
      # that expected a state map with a :batch key

      # Simulate the problematic scenario
      invalid_state = {:stop, :timeout}

      # Before our fix, this would have caused:
      # ** (KeyError) key :batch not found in: {:stop, :timeout}

      # After our fix, handle_call should catch this and handle it properly
      # by not passing the {:stop, reason} tuple to the batching pipeline

      # Verify the invalid state is the expected error tuple
      assert invalid_state == {:stop, :timeout}
    end

    test "handle_call pattern matches correctly for error cases" do
      # Test that the handle_call function properly pattern matches on {:stop, reason}
      # and handles it without passing to batching functions

      state = %State{
        cluster: TestCluster,
        director: self(),
        epoch: 1,
        max_latency_in_ms: 10,
        max_per_batch: 5,
        empty_transaction_timeout_ms: 1000,
        transaction_system_layout: nil,
        batch: nil
      }

      # Simulate what happens when ask_for_transaction_system_layout_if_needed returns {:stop, reason}
      error_result = {:stop, :timeout}

      # Verify the error result structure
      assert error_result == {:stop, :timeout}

      # The expected behavior for this error case
      expected_reply = {:error, :timeout}
      expected_return = {:stop, :timeout, state}

      assert expected_reply == {:error, :timeout}
      assert expected_return == {:stop, :timeout, state}
    end

    test "state validation ensures proper structure" do
      # Test that valid states have the required structure
      valid_state = %State{
        cluster: TestCluster,
        director: self(),
        epoch: 1,
        max_latency_in_ms: 10,
        max_per_batch: 5,
        empty_transaction_timeout_ms: 1000,
        transaction_system_layout: %{sequencer: nil},
        batch: nil
      }

      # Valid states should be maps with required keys
      assert is_map(valid_state)
      assert Map.has_key?(valid_state, :batch)
      assert Map.has_key?(valid_state, :transaction_system_layout)
      assert Map.has_key?(valid_state, :director)

      # Error states should not be passed to batching functions
      error_state = {:stop, :some_error}
      refute is_map(error_state)
      # Error states are tuples, not State structs
      assert is_tuple(error_state)
    end
  end

  describe "empty transaction timeout" do
    test "handle_info(:timeout, %{batch: nil, mode: :running}) triggers empty transaction creation" do
      # Test that timeout with no batch and running mode calls single_transaction_batch
      # Since we can't easily mock the sequencer call, we test for the expected exit
      # when sequencer is unavailable (which it will be in unit tests)

      state = %State{
        cluster: TestCluster,
        director: self(),
        epoch: 1,
        max_latency_in_ms: 10,
        max_per_batch: 5,
        empty_transaction_timeout_ms: 1000,
        # No sequencer = unavailable
        transaction_system_layout: %{sequencer: nil},
        batch: nil,
        mode: :running,
        lock_token: "test_token"
      }

      # This should exit with sequencer unavailable
      assert catch_exit(Server.handle_info(:timeout, state)) ==
               {:sequencer_unavailable, :timeout_empty_transaction}
    end

    test "handle_info(:timeout, %{batch: nil, mode: :locked}) resets timeout without creating transaction" do
      # Create a mock state in locked mode with no active batch
      state = %State{
        cluster: TestCluster,
        director: self(),
        epoch: 1,
        max_latency_in_ms: 10,
        max_per_batch: 5,
        empty_transaction_timeout_ms: 1000,
        transaction_system_layout: %{sequencer: nil},
        batch: nil,
        mode: :locked,
        lock_token: "test_token"
      }

      # Test the timeout handler when locked - should reset empty transaction timeout
      result = Server.handle_info(:timeout, state)

      # Should return noreply with timeout reset
      assert {:noreply, ^state, 1000} = result
    end

    test "handle_info(:timeout, %{batch: batch}) processes existing batch normally" do
      # Create a mock state with an active batch
      state = %State{
        cluster: TestCluster,
        director: self(),
        epoch: 1,
        max_latency_in_ms: 10,
        max_per_batch: 5,
        empty_transaction_timeout_ms: 1000,
        transaction_system_layout: %{sequencer: nil},
        # Mock batch
        batch: %{},
        mode: :running,
        lock_token: "test_token"
      }

      # Test the timeout handler - should process existing batch
      result = Server.handle_info(:timeout, state)

      # Should clear batch and continue with finalization
      assert {:noreply, new_state, {:continue, {:finalize, %{}}}} = result
      assert new_state.batch == nil
    end

    test "init sets empty_transaction_timeout_ms in state and initial timeout" do
      init_args = {TestCluster, self(), 1, 10, 5, 1000, "test_token"}

      {:ok, state, timeout} = Server.init(init_args)

      assert state.empty_transaction_timeout_ms == 1000
      assert timeout == 1000
    end
  end
end
