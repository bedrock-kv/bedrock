defmodule Bedrock.Integration.BrandNewSystemInitializationTest do
  # Not async due to potential file system conflicts
  use ExUnit.Case, async: false

  alias Bedrock.ControlPlane.Director.Recovery.LogReplayPhase

  @moduletag :integration

  describe "copy_log_data/5" do
    # Helper function to attempt log data copy and handle expected failures
    defp attempt_copy_log_data(new_log_id, old_log_id, first_version, last_version, service_pids) do
      LogReplayPhase.copy_log_data(new_log_id, old_log_id, first_version, last_version, service_pids)
    catch
      # Expected failures when using mock process (self())
      :exit, {:noproc, _} -> :attempted_log_call
      :exit, {:calling_self, {GenServer, :call, [_, {:recover_from, nil, 0, 0}, _]}} -> :new_system_call
      :exit, {:calling_self, {GenServer, :call, [_, {:recover_from, _, _, _}, _]}} -> :existing_system_call
      :exit, {reason, _} when reason in [:normal, :killed, :shutdown] -> :attempted_log_call
      error_type, reason -> {error_type, reason}
    end

    test "calls recover_from with nil source for brand new system" do
      # Verifies fix: :none old_log_id calls Log.recover_from instead of returning early
      service_pids = %{"test_log" => self()}

      # Brand new system should call recover_from with nil source and version 0
      assert :new_system_call = attempt_copy_log_data("test_log", :none, 0, 0, service_pids)
    end

    test "calls recover_from with source log for existing system recovery" do
      service_pids = %{"new_log" => self(), "old_log" => self()}

      # Existing system should call recover_from with source log reference
      assert :existing_system_call = attempt_copy_log_data("new_log", "old_log", 10, 20, service_pids)
    end
  end
end
