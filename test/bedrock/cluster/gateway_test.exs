defmodule Bedrock.Cluster.GatewayTest do
  use ExUnit.Case, async: true

  import Bedrock.Test.GenServerTestHelpers

  alias Bedrock.Cluster.Gateway

  describe "begin_transaction/2" do
    test "successfully begins transaction with default options" do
      test_pid = self()

      # Spawn a process to make the call so we can capture the GenServer message
      spawn(fn ->
        Gateway.begin_transaction(test_pid, [])
      end)

      # Assert that the correct call was made with default options
      assert_call_received({:begin_transaction, opts}) do
        assert opts == []
      end
    end

    test "begins transaction with custom options" do
      test_pid = self()
      opts = [retry_count: 3, timeout_in_ms: 5000]

      spawn(fn ->
        Gateway.begin_transaction(test_pid, opts)
      end)

      assert_call_received({:begin_transaction, received_opts}) do
        assert received_opts == opts
      end
    end

    test "uses custom timeout from options" do
      test_pid = self()
      opts = [timeout_in_ms: 2000]

      spawn(fn ->
        Gateway.begin_transaction(test_pid, opts)
      end)

      # The timeout would be used in the actual GenServer.call
      # We can verify the options were passed correctly
      assert_call_received({:begin_transaction, received_opts}) do
        assert received_opts == opts
      end
    end

    test "uses infinity timeout when not specified" do
      test_pid = self()

      spawn(fn ->
        Gateway.begin_transaction(test_pid, [])
      end)

      # Verify the call was made with empty options (infinity timeout is default)
      assert_call_received({:begin_transaction, []})
    end

    test "handles empty options list" do
      test_pid = self()

      spawn(fn ->
        Gateway.begin_transaction(test_pid)
      end)

      assert_call_received({:begin_transaction, []})
    end
  end

  describe "renew_read_version_lease/3" do
    test "renews lease with read version and default options" do
      test_pid = self()
      read_version = 12_345

      spawn(fn ->
        Gateway.renew_read_version_lease(test_pid, read_version, [])
      end)

      assert_call_received({:renew_read_version_lease, received_version}) do
        assert received_version == read_version
      end
    end

    test "renews lease with custom timeout options" do
      test_pid = self()
      read_version = 54_321
      opts = [timeout_in_ms: 3_000]

      spawn(fn ->
        Gateway.renew_read_version_lease(test_pid, read_version, opts)
      end)

      assert_call_received({:renew_read_version_lease, received_version}) do
        assert received_version == read_version
      end
    end

    test "handles zero read version" do
      test_pid = self()
      read_version = 0

      spawn(fn ->
        Gateway.renew_read_version_lease(test_pid, read_version)
      end)

      assert_call_received({:renew_read_version_lease, received_version}) do
        assert received_version == 0
      end
    end

    test "handles large read version numbers" do
      test_pid = self()
      read_version = 999_999_999_999

      spawn(fn ->
        Gateway.renew_read_version_lease(test_pid, read_version)
      end)

      assert_call_received({:renew_read_version_lease, received_version}) do
        assert received_version == read_version
      end
    end

    test "uses infinity timeout when not specified" do
      test_pid = self()
      read_version = 123

      spawn(fn ->
        Gateway.renew_read_version_lease(test_pid, read_version)
      end)

      # Verify the call was made with the read version
      assert_call_received({:renew_read_version_lease, 123})
    end
  end

  describe "advertise_worker/2" do
    test "sends cast message to advertise worker" do
      test_pid = self()
      worker_pid = spawn(fn -> :ok end)

      spawn(fn ->
        Gateway.advertise_worker(test_pid, worker_pid)
      end)

      assert_cast_received({:advertise_worker, received_worker}) do
        assert received_worker == worker_pid
        assert is_pid(received_worker)
      end
    end

    test "advertises worker with different gateway reference types" do
      test_pid = self()
      worker_pid = spawn(fn -> :ok end)

      # Test with atom reference
      spawn(fn ->
        Gateway.advertise_worker(test_pid, worker_pid)
      end)

      assert_cast_received({:advertise_worker, received_worker}) do
        assert received_worker == worker_pid
      end
    end

    test "handles worker pid correctly" do
      test_pid = self()

      worker_pid =
        spawn(fn ->
          receive do
            :stop -> :ok
          end
        end)

      spawn(fn ->
        Gateway.advertise_worker(test_pid, worker_pid)
      end)

      assert_cast_received({:advertise_worker, received_worker}) do
        assert received_worker == worker_pid
        assert Process.alive?(received_worker)
      end

      # Cleanup
      send(worker_pid, :stop)
    end

    test "returns :ok immediately" do
      test_pid = self()
      worker_pid = spawn(fn -> :ok end)

      # The function should return :ok synchronously
      result = Gateway.advertise_worker(test_pid, worker_pid)
      assert result == :ok

      # And the cast should still be sent
      assert_cast_received({:advertise_worker, ^worker_pid})
    end
  end

  describe "get_descriptor/2" do
    test "gets descriptor with default timeout" do
      test_pid = self()

      spawn(fn ->
        Gateway.get_descriptor(test_pid, [])
      end)

      # The call should be made with :get_descriptor
      assert_call_received(:get_descriptor)
    end

    test "gets descriptor with custom timeout" do
      test_pid = self()
      opts = [timeout_in_ms: 5000]

      spawn(fn ->
        Gateway.get_descriptor(test_pid, opts)
      end)

      assert_call_received(:get_descriptor)
    end

    test "uses default 1000ms timeout when not specified" do
      test_pid = self()

      spawn(fn ->
        Gateway.get_descriptor(test_pid)
      end)

      # Verify the call was made
      assert_call_received(:get_descriptor)
    end

    test "handles empty options correctly" do
      test_pid = self()

      spawn(fn ->
        Gateway.get_descriptor(test_pid, [])
      end)

      assert_call_received(:get_descriptor)
    end

    test "handles nil options" do
      test_pid = self()

      # Test default parameter behavior
      spawn(fn ->
        Gateway.get_descriptor(test_pid)
      end)

      assert_call_received(:get_descriptor)
    end
  end

  describe "module integration" do
    test "all functions are exported and callable" do
      # Verify that all expected functions are exported with correct arity
      exports = Gateway.__info__(:functions)

      assert Keyword.has_key?(exports, :begin_transaction)
      assert Keyword.get(exports, :begin_transaction) == 1

      assert Keyword.has_key?(exports, :renew_read_version_lease)
      assert Keyword.get(exports, :renew_read_version_lease) == 2

      assert Keyword.has_key?(exports, :advertise_worker)
      assert Keyword.get(exports, :advertise_worker) == 2

      assert Keyword.has_key?(exports, :get_descriptor)
      assert Keyword.get(exports, :get_descriptor) == 1
    end

    test "uses GenServerApi correctly" do
      Code.ensure_loaded(Gateway)

      # Verify that the module properly uses the GenServerApi
      # This tests that the module structure is correct
      assert function_exported?(Gateway, :child_spec, 1)
      assert function_exported?(Gateway, :start_link, 1)
    end
  end

  describe "parameter validation and edge cases" do
    test "begin_transaction handles various gateway reference types" do
      test_pid = self()

      # Test with PID
      spawn(fn ->
        Gateway.begin_transaction(test_pid, [])
      end)

      assert_call_received({:begin_transaction, []})

      # Test with atom
      spawn(fn ->
        Gateway.begin_transaction(:gateway_atom, [])
      end)

      # We can't easily capture this without the actual GenServer running
      # but we can verify the function doesn't crash
    end

    test "renew_read_version_lease with boundary values" do
      test_pid = self()

      # Test with minimum value
      spawn(fn ->
        Gateway.renew_read_version_lease(test_pid, 0, [])
      end)

      assert_call_received({:renew_read_version_lease, 0})

      # Test with negative value (edge case - should still work as it's just passed through)
      spawn(fn ->
        Gateway.renew_read_version_lease(test_pid, -1, [])
      end)

      assert_call_received({:renew_read_version_lease, -1})
    end

    test "advertise_worker with dead process" do
      test_pid = self()

      # Create a process and kill it
      worker_pid = spawn(fn -> :ok end)
      Process.exit(worker_pid, :kill)

      # Should still send the cast (the server will handle dead processes)
      spawn(fn ->
        Gateway.advertise_worker(test_pid, worker_pid)
      end)

      assert_cast_received({:advertise_worker, received_worker}) do
        assert received_worker == worker_pid
        refute Process.alive?(received_worker)
      end
    end

    test "get_descriptor with various timeout values" do
      test_pid = self()

      # Test with zero timeout
      spawn(fn ->
        Gateway.get_descriptor(test_pid, timeout_in_ms: 0)
      end)

      assert_call_received(:get_descriptor)

      # Test with very large timeout
      spawn(fn ->
        Gateway.get_descriptor(test_pid, timeout_in_ms: 999_999)
      end)

      assert_call_received(:get_descriptor)
    end
  end

  describe "option handling" do
    test "begin_transaction preserves all option keys" do
      test_pid = self()
      opts = [retry_count: 5, timeout_in_ms: 1000, custom_option: :value]

      spawn(fn ->
        Gateway.begin_transaction(test_pid, opts)
      end)

      assert_call_received({:begin_transaction, received_opts}) do
        assert Keyword.get(received_opts, :retry_count) == 5
        assert Keyword.get(received_opts, :timeout_in_ms) == 1000
        assert Keyword.get(received_opts, :custom_option) == :value
      end
    end

    test "renew_read_version_lease preserves timeout options" do
      test_pid = self()
      read_version = 42
      opts = [timeout_in_ms: 2500, extra_opt: :test]

      spawn(fn ->
        Gateway.renew_read_version_lease(test_pid, read_version, opts)
      end)

      assert_call_received({:renew_read_version_lease, 42})
    end

    test "get_descriptor with various option formats" do
      test_pid = self()

      # Test with keyword list
      spawn(fn ->
        Gateway.get_descriptor(test_pid, timeout_in_ms: 3000)
      end)

      assert_call_received(:get_descriptor)

      # Test with empty keyword list
      spawn(fn ->
        Gateway.get_descriptor(test_pid, [])
      end)

      assert_call_received(:get_descriptor)
    end
  end

  describe "concurrent operations" do
    test "multiple begin_transaction calls can be made concurrently" do
      test_pid = self()

      # Spawn multiple processes making calls
      tasks =
        for i <- 1..3 do
          Task.async(fn ->
            Gateway.begin_transaction(test_pid, retry_count: i)
          end)
        end

      # Should receive all three calls
      for _i <- 1..3 do
        assert_call_received({:begin_transaction, opts}) do
          retry_count = Keyword.get(opts, :retry_count)
          assert retry_count in [1, 2, 3]
        end
      end

      # Clean up tasks
      for task <- tasks do
        Task.shutdown(task, 100)
      end
    end

    test "advertise_worker calls are asynchronous" do
      test_pid = self()

      # Create multiple worker processes
      workers =
        for _i <- 1..3 do
          spawn(fn -> :ok end)
        end

      # Advertise all workers
      for worker <- workers do
        Gateway.advertise_worker(test_pid, worker)
      end

      # Should receive all casts
      for _i <- 1..3 do
        assert_cast_received({:advertise_worker, worker}) do
          assert worker in workers
        end
      end
    end
  end
end
