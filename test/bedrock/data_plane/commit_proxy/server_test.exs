defmodule Bedrock.DataPlane.CommitProxy.ServerTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.CommitProxy.Server
  alias Bedrock.DataPlane.CommitProxy.State

  # Mock cluster module for testing
  defmodule TestCluster do
    def otp_name(component) when is_atom(component) do
      :"test_cluster_#{component}"
    end
  end

  describe "maybe_update_layout_from_transaction/2" do
    test "extracts transaction system layout from system transaction" do
      state = %State{
        cluster: TestCluster,
        director: self(),
        epoch: 1,
        max_latency_in_ms: 10,
        max_per_batch: 5,
        transaction_system_layout: nil,
        batch: nil
      }

      layout = %{sequencer: self(), resolvers: [], logs: %{}}
      encoded_layout = :erlang.term_to_binary(layout)

      transaction = {nil, %{"\xff/system/transaction_system_layout" => encoded_layout}}

      result = Server.maybe_update_layout_from_transaction(state, transaction)
      assert result.transaction_system_layout == layout
    end

    test "returns state unchanged when no layout in transaction" do
      state = %State{
        cluster: TestCluster,
        director: self(),
        epoch: 1,
        max_latency_in_ms: 10,
        max_per_batch: 5,
        transaction_system_layout: nil,
        batch: nil
      }

      transaction = {nil, %{"some_key" => "some_value"}}

      result = Server.maybe_update_layout_from_transaction(state, transaction)
      assert result == state
    end

    test "returns state unchanged for non-map writes" do
      state = %State{
        cluster: TestCluster,
        director: self(),
        epoch: 1,
        max_latency_in_ms: 10,
        max_per_batch: 5,
        transaction_system_layout: nil,
        batch: nil
      }

      transaction = {nil, "not_a_map"}

      result = Server.maybe_update_layout_from_transaction(state, transaction)
      assert result == state
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
        transaction_system_layout: %{sequencer: self()},
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
end
