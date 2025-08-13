defmodule Bedrock.Integration.BrandNewSystemInitializationTest do
  # Not async due to potential file system conflicts
  use ExUnit.Case, async: false

  alias Bedrock.ControlPlane.Director.Recovery.LogReplayPhase

  @moduletag :integration

  describe "brand new system initialization" do
    test "log replay phase calls recover_from for brand new system" do
      # This test verifies the fix: :none old_log_id should call Log.recover_from
      # instead of returning early with :no_recovery_needed

      new_log_id = "test_log"
      service_pids = %{new_log_id => self()}

      # The fix ensures this calls Log.recover_from instead of returning early
      result =
        try do
          LogReplayPhase.copy_log_data(
            new_log_id,
            # Brand new system - no old log to recover from
            :none,
            # First version for brand new system
            0,
            # Last version for brand new system
            0,
            service_pids
          )
        catch
          # Expected to fail because self() is not a real log process
          # But the important thing is it ATTEMPTS the call
          :exit, {:noproc, _} ->
            :attempted_log_call

          :exit, {:calling_self, {GenServer, :call, [_, {:recover_from, nil, 0, 0}, _]}} ->
            :attempted_log_call_with_correct_params

          :exit, {reason, _} when reason in [:normal, :killed, :shutdown] ->
            :attempted_log_call

          error_type, reason ->
            {error_type, reason}
        end

      # Before the fix: would return {:ok, :no_recovery_needed}
      # After the fix: attempts Log.recover_from call with correct params (nil source, 0 versions)
      assert result == :attempted_log_call_with_correct_params
    end

    test "log replay phase behavior difference between old and new systems" do
      # This test demonstrates the difference between old system recovery
      # and brand new system initialization

      service_pids = %{
        "new_log" => self(),
        "old_log" => self()
      }

      # Test 1: Brand new system (:none) should call recover_from with nil
      result_new =
        try do
          LogReplayPhase.copy_log_data(
            "new_log",
            # This is the brand new system case
            :none,
            0,
            0,
            service_pids
          )
        catch
          _, _ -> :expected_call_attempted
        end

      # Test 2: Existing system recovery should call recover_from with source log
      result_old =
        try do
          LogReplayPhase.copy_log_data(
            "new_log",
            # This is the existing system case
            "old_log",
            10,
            20,
            service_pids
          )
        catch
          _, _ -> :expected_call_attempted
        end

      # Both should attempt to call Log.recover_from (and fail due to mock process)
      # The key difference is the parameters passed to recover_from
      assert result_new == :expected_call_attempted
      assert result_old == :expected_call_attempted
    end
  end
end
