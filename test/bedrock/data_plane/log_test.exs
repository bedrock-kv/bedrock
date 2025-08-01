defmodule Bedrock.DataPlane.LogTest do
  use ExUnit.Case, async: true

  import Bedrock.Test.GenServerTestHelpers

  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.Log.Transaction

  describe "recovery_info/0" do
    test "returns list of fact names for recovery" do
      result = Log.recovery_info()

      expected = [:kind, :last_version, :oldest_version, :minimum_durable_version]
      assert result == expected
    end
  end

  describe "initial_transaction/0" do
    test "creates transaction with version 0 and empty key map" do
      transaction = Log.initial_transaction()

      # Transaction is a tuple {version, key_values_map}
      expected_transaction = Transaction.new(0, %{})
      assert transaction == expected_transaction

      # Verify it's the expected structure
      assert {0, %{}} = transaction
    end
  end

  describe "recover_from/4" do
    # Example of testing GenServer calls by receiving and asserting on format
    # Instead of: receive {:"$gen_call", from, {:some_call, _}} -> ...
    # Use: receive {:"$gen_call", from, call_message} ->
    #        assert {:some_call, actual_arg} = call_message
    #        assert actual_arg == expected_value

    test "delegates to GenServerApi call with proper arguments" do
      source_log = :source_log_ref
      first_version = 100
      last_version = 200
      test_pid = self()

      # Spawn a process that will make the call and we'll capture the message
      spawn(fn ->
        Log.recover_from(test_pid, source_log, first_version, last_version)
      end)

      # Use our helper macro to assert on the exact call message format
      assert_call_received({:recover_from, actual_source, actual_first, actual_last}) do
        assert actual_source == :source_log_ref
        assert actual_first == 100
        assert actual_last == 200
      end
    end

    test "handles unavailable log" do
      test_pid = self()

      # Spawn a process that will make the call and we'll capture the message
      spawn(fn ->
        Log.recover_from(test_pid, nil, 0, 50)
      end)

      # Use our helper macro to assert on the exact call message format
      assert_call_received({:recover_from, actual_source, actual_first, actual_last}) do
        assert actual_source == nil
        assert actual_first == 0
        assert actual_last == 50
      end
    end
  end

  describe "module structure" do
    test "public functions work correctly" do
      # Test that we can call the functions that provide coverage
      assert Log.recovery_info() == [
               :kind,
               :last_version,
               :oldest_version,
               :minimum_durable_version
             ]

      assert Log.initial_transaction() == {0, %{}}

      # recover_from would need a real GenServer process to test fully
      # but we already tested it in the dedicated test above
    end
  end
end
