defmodule Bedrock.DataPlane.CommitProxy.FinalizationTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.CommitProxy.Finalization

  describe "resolve_transactions with injectable functions" do
    test "calls timeout function with correct attempt numbers" do
      resolvers = [{<<0>>, :test_resolver}]
      last_version = 100
      commit_version = 101
      transaction_summaries = [{nil, [<<1>>]}]
      opts = []
      
      test_pid = self()
      
      # Mock timeout function that captures attempt numbers
      timeout_fn = fn attempt ->
        send(test_pid, {:timeout_called, attempt})
        1000  # Return a reasonable timeout
      end
      
      # Mock exit function that doesn't actually exit
      exit_fn = fn reason ->
        send(test_pid, {:exit_called, reason})
        throw({:test_exit, reason})
      end

      # Test that function goes through all retry attempts
      assert catch_throw(
        Finalization.resolve_transactions_with_functions(
          resolvers,
          last_version,
          commit_version,
          transaction_summaries,
          opts,
          timeout_fn,
          exit_fn
        )
      ) == {:test_exit, :unavailable}
      
      # Verify timeout function was called for each attempt (0, 1, 2)
      assert_receive {:timeout_called, 0}
      assert_receive {:timeout_called, 1} 
      assert_receive {:timeout_called, 2}
      assert_receive {:exit_called, :unavailable}
    end

    test "default timeout function provides exponential backoff" do
      # Test the default timeout function directly
      assert Finalization.default_timeout_fn(0) == 500    # 500 * 2^0 = 500
      assert Finalization.default_timeout_fn(1) == 1000   # 500 * 2^1 = 1000  
      assert Finalization.default_timeout_fn(2) == 2000   # 500 * 2^2 = 2000
    end

    test "emits telemetry events during retries" do
      test_pid = self()
      Process.put(:test_pid, test_pid)
      
      # Attach telemetry handlers using module functions to avoid warnings
      :telemetry.attach(
        "test-retry-telemetry",
        [:bedrock, :commit_proxy, :resolver, :retry],
        &__MODULE__.handle_retry_telemetry/4,
        nil
      )
      
      :telemetry.attach(
        "test-max-retries-telemetry", 
        [:bedrock, :commit_proxy, :resolver, :max_retries_exceeded],
        &__MODULE__.handle_max_retries_telemetry/4,
        nil
      )

      resolvers = [{<<0>>, :test_resolver}]
      last_version = 100
      commit_version = 101
      transaction_summaries = [{nil, [<<1>>]}]
      opts = []
      
      timeout_fn = fn _attempt -> 100 end  # Short timeout for faster test
      exit_fn = fn reason -> throw({:test_exit, reason}) end

      catch_throw(
        Finalization.resolve_transactions_with_functions(
          resolvers,
          last_version,
          commit_version,
          transaction_summaries,
          opts,
          timeout_fn,
          exit_fn
        )
      )

      # Should receive telemetry for retry attempts and final failure
      assert_receive {:telemetry, :retry, %{attempt: 1}, %{reason: :unavailable}}
      assert_receive {:telemetry, :retry, %{attempt: 2}, %{reason: :unavailable}}
      assert_receive {:telemetry, :max_retries, %{total_attempts: 3}, %{reason: :unavailable}}

      :telemetry.detach("test-retry-telemetry")
      :telemetry.detach("test-max-retries-telemetry")
    end
  end

  # Telemetry handlers to avoid performance warnings
  def handle_retry_telemetry(_event, measurements, metadata, _config) do
    test_pid = Process.get(:test_pid)
    send(test_pid, {:telemetry, :retry, measurements, metadata})
  end

  def handle_max_retries_telemetry(_event, measurements, metadata, _config) do
    test_pid = Process.get(:test_pid)
    send(test_pid, {:telemetry, :max_retries, measurements, metadata})
  end
end