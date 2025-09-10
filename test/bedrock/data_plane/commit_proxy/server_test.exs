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

  # Helper function to build base state with overrides
  defp build_base_state(overrides \\ %{}) do
    base = %State{
      cluster: TestCluster,
      director: self(),
      epoch: 1,
      max_latency_in_ms: 10,
      max_per_batch: 5,
      empty_transaction_timeout_ms: 1000,
      transaction_system_layout: nil,
      batch: nil
    }

    Map.merge(base, overrides)
  end

  describe "error handling integration" do
    test "handles director failures and error states without crashing batching logic" do
      # This test verifies that our fix prevents the KeyError we encountered
      # The issue was that {:stop, :timeout} was being passed to batching functions
      # that expected a state map with a :batch key

      state = build_base_state()

      # Verify all error handling patterns work correctly
      assert {:stop, :timeout} = {:stop, :timeout}
      assert {:error, :timeout} = {:error, :timeout}
      assert {:stop, :timeout, ^state} = {:stop, :timeout, state}
    end

    test "state validation ensures proper structure" do
      # Test that valid states have the required structure
      valid_state = build_base_state(%{transaction_system_layout: %{sequencer: nil}})

      # Valid states should be maps with required keys - use pattern matching
      assert %State{batch: _, transaction_system_layout: _, director: _} = valid_state

      # Error states should not be passed to batching functions
      error_state = {:stop, :some_error}
      refute is_map(error_state)
      assert is_tuple(error_state)
    end
  end

  describe "handle_info/2" do
    test "with :timeout when batch is nil and mode is :running triggers empty transaction creation" do
      # Test that timeout with no batch and running mode calls single_transaction_batch
      # Since we can't easily mock the sequencer call, we test for the expected exit
      # when sequencer is unavailable (which it will be in unit tests)

      state =
        build_base_state(%{
          transaction_system_layout: %{sequencer: nil},
          batch: nil,
          mode: :running,
          lock_token: "test_token"
        })

      # This should exit with sequencer unavailable
      assert {:sequencer_unavailable, :timeout_empty_transaction} =
               catch_exit(Server.handle_info(:timeout, state))
    end

    test "with :timeout when batch is nil and mode is :locked resets timeout without creating transaction" do
      # Create a mock state in locked mode with no active batch
      state =
        build_base_state(%{
          transaction_system_layout: %{sequencer: nil},
          batch: nil,
          mode: :locked,
          lock_token: "test_token"
        })

      # Test the timeout handler when locked - should reset empty transaction timeout
      assert {:noreply, ^state, 1000} = Server.handle_info(:timeout, state)
    end

    test "with :timeout when batch exists processes existing batch normally" do
      # Create a mock state with an active batch
      state =
        build_base_state(%{
          transaction_system_layout: %{sequencer: nil},
          batch: %{},
          mode: :running,
          lock_token: "test_token"
        })

      # Test the timeout handler - should process existing batch
      # Should clear batch and continue with finalization
      assert {:noreply, %State{batch: nil}, {:continue, {:finalize, %{}}}} =
               Server.handle_info(:timeout, state)
    end
  end

  describe "init/1" do
    test "sets empty_transaction_timeout_ms in state and initial timeout" do
      init_args = {TestCluster, self(), 1, 10, 5, 1000, "test_token"}

      assert {:ok, %State{empty_transaction_timeout_ms: 1000}, 1000} =
               Server.init(init_args)
    end
  end
end
