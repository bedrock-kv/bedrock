defmodule Bedrock.DataPlane.Log.Shale.PushingTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Log.Shale.Pushing
  alias Bedrock.DataPlane.Log.Shale.State
  alias Bedrock.DataPlane.Version
  alias Bedrock.Test.DataPlane.TransactionTestSupport

  describe "push/4 error conditions" do
    test "rejects transaction that is too large" do
      state = %State{
        mode: :ready,
        last_version: Version.from_integer(0)
      }

      # Create a transaction larger than 10MB limit
      large_transaction = :binary.copy(<<0>>, 10_000_001)
      ack_fn = fn _result -> :ok end

      assert {:error, :tx_too_large} =
               Pushing.push(state, Version.from_integer(1), large_transaction, ack_fn)
    end

    test "rejects out of order transaction with version less than last_version" do
      state = %State{
        mode: :ready,
        last_version: Version.from_integer(5),
        pending_pushes: %{}
      }

      transaction = TransactionTestSupport.new_log_transaction(3, %{"a" => "1"})
      ack_fn = fn _result -> :ok end

      # Trying to push version 3 when last_version is 5
      assert {:error, :tx_out_of_order} =
               Pushing.push(state, Version.from_integer(3), transaction, ack_fn)
    end

    test "waits for future transaction version" do
      state = %State{
        mode: :ready,
        last_version: Version.from_integer(5),
        pending_pushes: %{}
      }

      transaction = TransactionTestSupport.new_log_transaction(10, %{"a" => "1"})
      ack_fn = fn _result -> :ok end

      # Trying to push version 10 when last_version is 5 should wait
      assert {:wait, new_state} =
               Pushing.push(state, Version.from_integer(10), transaction, ack_fn)

      # Transaction should be in pending pushes
      assert Map.has_key?(new_state.pending_pushes, Version.from_integer(10))
    end
  end

  describe "write_encoded_transaction/2 error handling" do
    test "raises when transaction version cannot be extracted" do
      state = %State{
        mode: :ready,
        last_version: Version.from_integer(0),
        writer: nil,
        segment_recycler: nil
      }

      # Malformed transaction that will fail version extraction
      malformed_transaction = <<0, 1, 2>>

      assert_raise RuntimeError, ~r/Failed to extract version/, fn ->
        Pushing.write_encoded_transaction(state, malformed_transaction)
      end
    end
  end
end
