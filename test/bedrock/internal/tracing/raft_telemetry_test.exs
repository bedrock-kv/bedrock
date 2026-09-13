defmodule Bedrock.Internal.Tracing.RaftTelemetryTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Bedrock.Internal.Tracing.RaftTelemetry

  # A handler clause that fails to match its event's metadata raises a
  # FunctionClauseError inside :telemetry, which detaches the handler and
  # silently ends tracing. These tests pin the handler to the metadata shapes
  # bedrock_raft 0.10 actually emits.

  @start_time 0

  describe "append-entries ack events (bedrock_raft 0.10 metadata)" do
    test "handles the ack-received metadata shape" do
      metadata = %{
        term: 3,
        follower: :node_b,
        success: true,
        request_transaction_id: {3, 7},
        follower_newest_transaction_id: {3, 7}
      }

      log =
        capture_log(fn ->
          RaftTelemetry.log_event(
            [:bedrock, :raft, :append_entries_ack_received],
            %{at: 1},
            metadata,
            @start_time
          )
        end)

      assert log =~ "ack"
    end

    test "handles the ack-sent metadata shape, including a rejection" do
      metadata = %{
        term: 3,
        leader: :node_a,
        success: false,
        request_transaction_id: {3, 9},
        follower_newest_transaction_id: {3, 7}
      }

      log =
        capture_log(fn ->
          RaftTelemetry.log_event(
            [:bedrock, :raft, :append_entries_ack_sent],
            %{at: 1},
            metadata,
            @start_time
          )
        end)

      assert log =~ "ack"
    end
  end
end
