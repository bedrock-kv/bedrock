defmodule Bedrock.DataPlane.LogTest do
  use ExUnit.Case, async: true

  import Bedrock.Test.Common.GenServerTestHelpers

  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.Version
  alias Bedrock.Test.DataPlane.TransactionTestSupport

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
      zero_version = Version.zero()

      # Transaction should be encoded Transaction format
      expected_transaction = TransactionTestSupport.new_log_transaction(zero_version, %{})
      assert transaction == expected_transaction
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
      assert_call_received({:recover_from, :source_log_ref, 100, 200})
    end

    test "handles unavailable log" do
      test_pid = self()

      # Spawn a process that will make the call and we'll capture the message
      spawn(fn ->
        Log.recover_from(test_pid, nil, 0, 50)
      end)

      # Use our helper macro to assert on the exact call message format
      assert_call_received({:recover_from, nil, 0, 50})
    end
  end

  describe "lock_for_recovery/2" do
    test "delegates to Worker with proper arguments" do
      test_pid = self()
      epoch = 42

      # Spawn a process that will make the call and we'll capture the message
      spawn(fn ->
        Log.lock_for_recovery(test_pid, epoch)
      end)

      # Use our helper macro to assert on the exact call message format
      assert_call_received({:lock_for_recovery, 42})
    end
  end

  describe "module structure" do
    test "initial_transaction returns properly encoded Transaction with zero version and empty writes" do
      initial_tx = Log.initial_transaction()

      assert is_binary(initial_tx)
      assert Version.zero() == TransactionTestSupport.extract_log_version(initial_tx)
      assert %{} == TransactionTestSupport.extract_log_writes(initial_tx)
    end
  end
end
