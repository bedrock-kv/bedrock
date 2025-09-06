defmodule Bedrock.Internal.RangeQueryTest do
  use ExUnit.Case, async: true

  alias Bedrock.Internal.RangeQuery

  describe "stream/4" do
    test "creates a lazy stream" do
      # Mock transaction PID (we won't actually call it in this test)
      txn_pid = spawn(fn -> :timer.sleep(1000) end)

      # Creating the stream should not fail
      stream = RangeQuery.stream(txn_pid, "a", "z", batch_size: 10)

      # Stream should be a proper Stream function
      assert is_function(stream)
      # Stream.resource returns a 2-arity function
      assert is_function(stream, 2)
    end

    test "stream is lazy and doesn't evaluate immediately" do
      # Create a process that will wait and then fail if called
      test_pid = self()

      failing_pid =
        spawn(fn ->
          receive do
            {:get_range, _, _, _, _} ->
              send(test_pid, :should_not_be_called)
              exit(:should_not_be_called)
          after
            500 ->
              # Process stays alive, proving stream didn't call it
              send(test_pid, :still_alive)
          end
        end)

      # Stream creation should not fail even with a monitored PID
      _stream = RangeQuery.stream(failing_pid, "a", "z")

      # The process should still be alive after stream creation (laziness test)
      receive do
        :should_not_be_called ->
          flunk("Stream was not lazy - it called the transaction process immediately")

        :still_alive ->
          # Good, the process is still alive, meaning stream didn't call it
          Process.exit(failing_pid, :kill)
      after
        600 ->
          # Process didn't respond, likely already dead for other reasons
          Process.exit(failing_pid, :kill)
      end
    end
  end
end
