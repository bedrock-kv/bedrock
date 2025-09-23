defmodule Bedrock.DataPlane.Storage.Olivine.TracingTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Storage.Olivine.Tracing

  describe "olivine tracing" do
    test "can start and stop tracing" do
      assert :ok = Tracing.start()
      assert {:error, :already_exists} = Tracing.start()
      assert :ok = Tracing.stop()
      assert :ok = Tracing.start()
      assert :ok = Tracing.stop()
    end

    test "handles olivine-specific telemetry events" do
      Tracing.start()

      # These should not crash
      Tracing.handler(
        [:bedrock, :storage, :transactions_queued],
        %{transaction_count: 5, queue_size: 10},
        %{otp_name: :test_olivine},
        nil
      )

      Tracing.handler(
        [:bedrock, :storage, :batch_processing_complete],
        %{batch_size: 3, duration_us: 1500},
        %{otp_name: :test_olivine},
        nil
      )

      Tracing.handler(
        [:bedrock, :storage, :read_request_waitlisted],
        %{},
        %{otp_name: :test_olivine, operation: :get, key: "test_key"},
        nil
      )

      Tracing.stop()
    end
  end
end
