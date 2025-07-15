defmodule Bedrock.DataPlane.Log.TelemetryTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Log.Telemetry

  setup do
    # Capture telemetry events
    test_pid = self()
    
    :telemetry.attach_many(
      "test-log-telemetry",
      [
        [:bedrock, :log, :push],
        [:bedrock, :log, :push_out_of_order]
      ],
      &__MODULE__.handle_telemetry/4,
      test_pid
    )

    # Set trace metadata for the process
    Telemetry.trace_metadata(cluster: :test_cluster, id: "test_log", otp_name: :test_otp)

    on_exit(fn ->
      :telemetry.detach("test-log-telemetry")
    end)

    :ok
  end

  describe "trace_push_transaction/2" do
    test "emits push telemetry event with correct data" do
      expected_version = 42
      n_keys = 5

      Telemetry.trace_push_transaction(expected_version, n_keys)

      assert_receive {:telemetry, [:bedrock, :log, :push], 
                      %{n_keys: ^n_keys}, 
                      %{expected_version: ^expected_version, cluster: :test_cluster, id: "test_log", otp_name: :test_otp}}
    end
  end

  describe "trace_push_out_of_order/2" do
    test "emits push_out_of_order telemetry event with version information" do
      expected_version = 35
      current_version = 42

      Telemetry.trace_push_out_of_order(expected_version, current_version)

      assert_receive {:telemetry, [:bedrock, :log, :push_out_of_order], 
                      %{}, 
                      %{expected_version: ^expected_version, current_version: ^current_version, 
                        cluster: :test_cluster, id: "test_log", otp_name: :test_otp}}
    end
  end

  def handle_telemetry(event, measurements, metadata, test_pid) do
    send(test_pid, {:telemetry, event, measurements, metadata})
  end
end