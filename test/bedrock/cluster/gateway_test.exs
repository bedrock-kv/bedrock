defmodule Bedrock.Cluster.GatewayTest do
  use ExUnit.Case, async: true

  import Bedrock.Test.Common.GenServerTestHelpers

  alias Bedrock.Cluster.Gateway

  # Helper to reduce repetitive spawn patterns
  defp make_call(fun), do: spawn(fun)

  describe "begin_transaction/2" do
    test "begins transaction with various options" do
      test_pid = self()

      # Test with default options
      make_call(fn -> Gateway.begin_transaction(test_pid, []) end)
      assert_call_received({:begin_transaction, []})

      # Test with custom options
      opts = [retry_count: 3, timeout_in_ms: 5000]
      make_call(fn -> Gateway.begin_transaction(test_pid, opts) end)
      assert_call_received({:begin_transaction, ^opts})
    end
  end

  describe "renew_read_version_lease/3" do
    test "renews lease with read version and options" do
      test_pid = self()
      read_version = 12_345
      opts = [timeout_in_ms: 3_000]

      make_call(fn -> Gateway.renew_read_version_lease(test_pid, read_version, opts) end)
      assert_call_received({:renew_read_version_lease, ^read_version})
    end

    test "handles various version numbers" do
      test_versions = [0, 123, 999_999_999_999]
      test_pid = self()

      for version <- test_versions do
        make_call(fn -> Gateway.renew_read_version_lease(test_pid, version) end)
        assert_call_received({:renew_read_version_lease, ^version})
      end
    end
  end

  describe "advertise_worker/2" do
    test "sends cast message and returns :ok" do
      test_pid = self()
      worker_pid = spawn(fn -> :ok end)

      assert :ok = Gateway.advertise_worker(test_pid, worker_pid)
      assert_cast_received({:advertise_worker, ^worker_pid})
    end
  end

  describe "get_descriptor/2" do
    test "gets descriptor with various options" do
      test_pid = self()

      # Test with default behavior (no options)
      make_call(fn -> Gateway.get_descriptor(test_pid) end)
      assert_call_received(:get_descriptor)

      # Test with empty options
      make_call(fn -> Gateway.get_descriptor(test_pid, []) end)
      assert_call_received(:get_descriptor)

      # Test with custom timeout
      make_call(fn -> Gateway.get_descriptor(test_pid, timeout_in_ms: 5000) end)
      assert_call_received(:get_descriptor)
    end
  end

  describe "module integration" do
    test "exports expected functions with correct arities" do
      exports = Gateway.__info__(:functions)

      # Check that the expected functions exist with at least the expected arities
      assert Keyword.has_key?(exports, :begin_transaction)
      assert Keyword.has_key?(exports, :renew_read_version_lease)
      assert Keyword.has_key?(exports, :advertise_worker)
      assert Keyword.has_key?(exports, :get_descriptor)

      # Verify the main arities are present
      assert 1 in Keyword.get_values(exports, :begin_transaction)
      assert 2 in Keyword.get_values(exports, :renew_read_version_lease)
      assert 2 in Keyword.get_values(exports, :advertise_worker)
      assert 1 in Keyword.get_values(exports, :get_descriptor)
    end

    test "includes GenServer behavior functions" do
      Code.ensure_loaded(Gateway)

      assert function_exported?(Gateway, :child_spec, 1)
      assert function_exported?(Gateway, :start_link, 1)
    end
  end

  describe "edge cases and validation" do
    test "handles boundary values and edge cases" do
      test_pid = self()

      # Test renew_read_version_lease with boundary values
      make_call(fn -> Gateway.renew_read_version_lease(test_pid, 0, []) end)
      assert_call_received({:renew_read_version_lease, 0})

      make_call(fn -> Gateway.renew_read_version_lease(test_pid, -1, []) end)
      assert_call_received({:renew_read_version_lease, -1})

      # Test get_descriptor with extreme timeout values
      make_call(fn -> Gateway.get_descriptor(test_pid, timeout_in_ms: 0) end)
      assert_call_received(:get_descriptor)

      make_call(fn -> Gateway.get_descriptor(test_pid, timeout_in_ms: 999_999) end)
      assert_call_received(:get_descriptor)
    end

    test "advertise_worker handles dead processes" do
      test_pid = self()
      worker_pid = spawn(fn -> :ok end)
      Process.exit(worker_pid, :kill)

      make_call(fn -> Gateway.advertise_worker(test_pid, worker_pid) end)

      assert_cast_received({:advertise_worker, received_worker}) do
        assert received_worker == worker_pid
        refute Process.alive?(received_worker)
      end
    end
  end

  describe "option handling" do
    test "begin_transaction preserves all option keys" do
      test_pid = self()
      opts = [retry_count: 5, timeout_in_ms: 1000, custom_option: :value]

      make_call(fn -> Gateway.begin_transaction(test_pid, opts) end)

      assert_call_received({:begin_transaction, received_opts}) do
        assert %{retry_count: 5, timeout_in_ms: 1000, custom_option: :value} =
                 Map.new(received_opts)
      end
    end
  end

  describe "concurrent operations" do
    test "handles multiple concurrent calls" do
      test_pid = self()

      # Test concurrent begin_transaction calls
      tasks = for i <- 1..3, do: Task.async(fn -> Gateway.begin_transaction(test_pid, retry_count: i) end)

      for _i <- 1..3 do
        assert_call_received({:begin_transaction, opts}) do
          assert Keyword.get(opts, :retry_count) in [1, 2, 3]
        end
      end

      Enum.each(tasks, &Task.shutdown(&1, 100))

      # Test concurrent advertise_worker calls
      workers = for _i <- 1..3, do: spawn(fn -> :ok end)
      Enum.each(workers, &Gateway.advertise_worker(test_pid, &1))

      for _i <- 1..3 do
        assert_cast_received({:advertise_worker, worker}) do
          assert worker in workers
        end
      end
    end
  end
end
