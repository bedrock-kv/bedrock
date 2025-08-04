defmodule Bedrock.DataPlane.CommitProxy.TracingTest do
  use ExUnit.Case, async: true
  import ExUnit.CaptureLog

  alias Bedrock.DataPlane.CommitProxy.Tracing
  alias Bedrock.DataPlane.Version

  # Define a mock cluster for testing
  defmodule TestCluster do
    def name, do: :test_cluster
  end

  describe "trace/3" do
    setup do
      # Set up logger metadata to avoid KeyError
      Logger.metadata(cluster: TestCluster)
      :ok
    end

    test "traces start event" do
      measurements = %{n_transactions: 5}
      metadata = %{cluster: TestCluster, commit_version: Version.from_integer(123)}

      log_output =
        capture_log(fn ->
          Tracing.trace(:start, measurements, metadata)
        end)

      assert log_output =~ "Transaction Batch 123 started with 5 transactions"
    end

    test "traces stop event" do
      measurements = %{n_aborts: 2, n_oks: 8, duration_us: 1500}
      metadata = %{commit_version: Version.from_integer(124)}

      log_output =
        capture_log(fn ->
          Tracing.trace(:stop, measurements, metadata)
        end)

      assert log_output =~ "Transaction Batch 124 completed with 2 aborts and 8 oks"
      # The function rounds microseconds
      assert log_output =~ "1ms"
    end

    test "traces failed event with commit_version in measurements" do
      # This test verifies the fix for the function_clause error
      # The original bug was that :failed expected commit_version in metadata
      # but it was actually passed in measurements
      measurements = %{
        duration_us: 437,
        commit_version: Version.from_integer(1),
        n_transactions: 1
      }

      metadata = %{
        pid: self(),
        reason: {:log_failures, [{"byj7vnbf", :tx_out_of_order}]},
        cluster: Example.Cluster
      }

      # This should not raise a function_clause error
      log_output =
        capture_log(fn ->
          Tracing.trace(:failed, measurements, metadata)
        end)

      assert log_output =~ "Transaction Batch 1 failed"
      assert log_output =~ "{:log_failures, [{\"byj7vnbf\", :tx_out_of_order}]}"
      assert log_output =~ "437μs"
    end

    test "traces failed event handles various failure reasons" do
      measurements = %{duration_us: 1000, commit_version: Version.from_integer(42)}

      test_cases = [
        {:timeout, "timeout"},
        {:log_failures, "log_failures"},
        {:storage_failures, "storage_failures"},
        {:custom_error, "custom_error"}
      ]

      Enum.each(test_cases, fn {reason, expected_text} ->
        metadata = %{reason: reason, cluster: TestCluster}

        log_output =
          capture_log(fn ->
            Tracing.trace(:failed, measurements, metadata)
          end)

        assert log_output =~ "Transaction Batch 42 failed"
        assert log_output =~ expected_text
        assert log_output =~ "1ms"
      end)
    end
  end

  describe "handler/4" do
    setup do
      Logger.metadata(cluster: TestCluster)
      :ok
    end

    test "calls trace with correct arguments for failed event" do
      # This test ensures the telemetry handler correctly passes through
      # the measurements and metadata structure that caused the original bug
      event_name = [:bedrock, :data_plane, :commit_proxy, :failed]

      measurements = %{
        duration_us: 500,
        commit_version: Version.from_integer(99),
        n_transactions: 3
      }

      metadata = %{
        pid: self(),
        reason: {:storage_failures, [{"storage1", :locked}]},
        cluster: TestCluster
      }

      # Should not crash with function_clause error
      log_output =
        capture_log(fn ->
          Tracing.handler(event_name, measurements, metadata, nil)
        end)

      assert log_output =~ "Transaction Batch 99 failed"
      assert log_output =~ "storage_failures"
    end
  end

  describe "telemetry integration" do
    test "start and stop functions work correctly" do
      # Test that the telemetry handlers can be attached and detached
      assert :ok = Tracing.start()
      assert :ok = Tracing.stop()
    end
  end
end
