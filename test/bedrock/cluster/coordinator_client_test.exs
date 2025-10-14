defmodule Bedrock.Cluster.CoordinatorClientTest do
  use ExUnit.Case, async: true

  import Bedrock.Test.Common.GenServerTestHelpers

  alias Bedrock.Cluster.CoordinatorClient

  # Helper to reduce repetitive spawn patterns
  defp make_call(fun), do: spawn(fun)

  describe "fetch_coordinator/2" do
    test "fetches coordinator with various options" do
      test_pid = self()

      # Test with default options
      make_call(fn -> CoordinatorClient.fetch_coordinator(test_pid) end)
      assert_call_received(:get_known_coordinator)

      # Test with custom timeout
      make_call(fn -> CoordinatorClient.fetch_coordinator(test_pid, timeout_in_ms: 5000) end)
      assert_call_received(:get_known_coordinator)
    end
  end

  describe "fetch_transaction_system_layout/2" do
    test "fetches TSL with various options" do
      test_pid = self()

      # Test with default options
      make_call(fn -> CoordinatorClient.fetch_transaction_system_layout(test_pid) end)
      assert_call_received(:get_transaction_system_layout)

      # Test with custom timeout
      make_call(fn -> CoordinatorClient.fetch_transaction_system_layout(test_pid, timeout_in_ms: 5000) end)
      assert_call_received(:get_transaction_system_layout)
    end
  end

  describe "fetch_descriptor/2" do
    test "fetches descriptor with various options" do
      test_pid = self()

      # Test with default behavior (no options)
      make_call(fn -> CoordinatorClient.fetch_descriptor(test_pid) end)
      assert_call_received(:get_descriptor)

      # Test with empty options
      make_call(fn -> CoordinatorClient.fetch_descriptor(test_pid, []) end)
      assert_call_received(:get_descriptor)

      # Test with custom timeout
      make_call(fn -> CoordinatorClient.fetch_descriptor(test_pid, timeout_in_ms: 5000) end)
      assert_call_received(:get_descriptor)
    end
  end

  describe "module integration" do
    test "exports expected functions with correct arities" do
      exports = CoordinatorClient.__info__(:functions)

      # Check that the expected functions exist with at least the expected arities
      assert Keyword.has_key?(exports, :fetch_coordinator)
      assert Keyword.has_key?(exports, :fetch_transaction_system_layout)
      assert Keyword.has_key?(exports, :fetch_descriptor)

      # Verify the main arities are present
      assert 1 in Keyword.get_values(exports, :fetch_coordinator)
      assert 1 in Keyword.get_values(exports, :fetch_transaction_system_layout)
      assert 1 in Keyword.get_values(exports, :fetch_descriptor)
    end

    test "includes GenServer behavior functions" do
      Code.ensure_loaded(CoordinatorClient)

      assert function_exported?(CoordinatorClient, :child_spec, 1)
      assert function_exported?(CoordinatorClient, :start_link, 1)
    end
  end

  describe "edge cases and validation" do
    test "handles boundary values and edge cases" do
      test_pid = self()

      # Test fetch_descriptor with extreme timeout values
      make_call(fn -> CoordinatorClient.fetch_descriptor(test_pid, timeout_in_ms: 0) end)
      assert_call_received(:get_descriptor)

      make_call(fn -> CoordinatorClient.fetch_descriptor(test_pid, timeout_in_ms: 999_999) end)
      assert_call_received(:get_descriptor)
    end
  end

  describe "concurrent operations" do
    test "handles multiple concurrent calls" do
      test_pid = self()

      # Test concurrent fetch_coordinator calls
      tasks = for _i <- 1..3, do: Task.async(fn -> CoordinatorClient.fetch_coordinator(test_pid) end)

      for _i <- 1..3 do
        assert_call_received(:get_known_coordinator)
      end

      Enum.each(tasks, &Task.shutdown(&1, 100))
    end
  end
end
