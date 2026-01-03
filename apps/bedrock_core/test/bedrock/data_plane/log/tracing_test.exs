defmodule Bedrock.DataPlane.Log.TracingTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Bedrock.DataPlane.Log.Tracing
  alias Bedrock.DataPlane.Version
  alias Bedrock.Test.DataPlane.TransactionTestSupport

  # Mock cluster module for testing
  defmodule MockCluster do
    @moduledoc false
    def name, do: "test_cluster"
  end

  describe "telemetry handlers" do
    test "start/0 attaches telemetry handlers" do
      # Stop any existing handler first
      Tracing.stop()

      # Start should succeed
      assert :ok = Tracing.start()

      # Starting again should return already_exists
      assert {:error, :already_exists} = Tracing.start()

      # Clean up
      Tracing.stop()
    end

    test "stop/0 detaches telemetry handlers" do
      # Start handler first
      Tracing.start()

      # Stop should succeed
      assert :ok = Tracing.stop()

      # Stopping again should return not_found
      assert {:error, :not_found} = Tracing.stop()
    end
  end

  describe "log event handling" do
    setup do
      # Set up logger metadata for tracing
      Logger.metadata(
        cluster: MockCluster,
        id: "test_log_1"
      )

      {:ok, cluster: MockCluster}
    end

    # Helper function to capture and assert log content
    defp assert_log_contains(event, measurements, metadata, expected_content) do
      log =
        capture_log(fn ->
          Tracing.handler([:bedrock, :log, event], measurements, metadata, nil)
        end)

      assert log =~ expected_content
      assert log =~ "Bedrock Log [test_cluster/test_log_1]"
    end

    test "handles :started event", %{cluster: cluster} do
      metadata = %{
        cluster: cluster,
        id: "test_log_1",
        otp_name: :test_log_server
      }

      assert_log_contains(:started, %{}, metadata, "Started log service: test_log_server")
    end

    test "handles :lock_for_recovery event" do
      assert_log_contains(:lock_for_recovery, %{}, %{epoch: 5}, "Lock for recovery in epoch 5")
    end

    test "handles :recover_from event with no source" do
      assert_log_contains(:recover_from, %{}, %{source_log: :none}, "Reset to initial version")
    end

    test "handles :recover_from event with source log" do
      metadata = %{
        source_log: :log_server_2,
        first_version: Version.from_integer(100),
        last_version: Version.from_integer(150)
      }

      assert_log_contains(
        :recover_from,
        %{},
        metadata,
        "Recover from :log_server_2 with versions <0,0,0,0,0,0,0,100> to <0,0,0,0,0,0,0,150>"
      )
    end

    test "handles :push event" do
      version = Version.from_integer(200)

      encoded_transaction =
        TransactionTestSupport.new_log_transaction(
          version,
          %{"key1" => "value1", "key2" => "value2", "key3" => "value3"}
        )

      assert_log_contains(
        :push,
        %{},
        %{transaction: encoded_transaction},
        "Push transaction (3 keys) with expected version <0,0,0,0,0,0,0,200>"
      )
    end

    test "handles :push_out_of_order event" do
      metadata = %{
        expected_version: Version.from_integer(205),
        current_version: Version.from_integer(200)
      }

      assert_log_contains(
        :push_out_of_order,
        %{},
        metadata,
        "Rejected out-of-order transaction: expected <0,0,0,0,0,0,0,205>, current <0,0,0,0,0,0,0,200>"
      )
    end

    test "handles :pull event" do
      metadata = %{
        from_version: Version.from_integer(100),
        opts: [timeout: 5000]
      }

      assert_log_contains(
        :pull,
        %{},
        metadata,
        "Pull transactions from version <0,0,0,0,0,0,0,100> with options [timeout: 5000]"
      )
    end
  end
end
