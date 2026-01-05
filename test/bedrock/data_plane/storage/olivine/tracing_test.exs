defmodule Bedrock.DataPlane.Storage.Olivine.TracingTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

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

      log =
        capture_log(fn ->
          Tracing.handler(
            [:bedrock, :storage, :transactions_queued],
            %{transaction_count: 5, queue_size: 10},
            %{otp_name: :test_olivine},
            nil
          )

          Tracing.handler(
            [:bedrock, :storage, :transaction_processing_complete],
            %{batch_size: 3, duration_μs: 1500},
            %{otp_name: :test_olivine},
            nil
          )
        end)

      assert log =~ "Queued 5 transactions (queue size: 10)"
      assert log =~ "Completed transaction processing (batch size: 3"

      Tracing.stop()
    end
  end
end
