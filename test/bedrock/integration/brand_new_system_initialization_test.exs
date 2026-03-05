defmodule Bedrock.Integration.BrandNewSystemInitializationTest do
  # Not async due to potential file system conflicts
  use ExUnit.Case, async: false

  alias Bedrock.ControlPlane.Director.Recovery.LogReplayPhase

  @moduletag :integration

  describe "copy_log_data/5" do
    # Helper function to attempt log data copy and handle expected failures
    defp attempt_copy_log_data(new_log_id, survivor_pids, first_version, last_version, service_pids) do
      LogReplayPhase.copy_log_data(new_log_id, survivor_pids, first_version, last_version, service_pids)
    catch
      # Expected failures when using mock process (self())
      :exit, {:noproc, _} -> :attempted_log_call
      :exit, {:calling_self, {GenServer, :call, [_, {:recover_from, [], 0, 0}, _]}} -> :new_system_call
      :exit, {:calling_self, {GenServer, :call, [_, {:recover_from, [_ | _], _, _}, _]}} -> :existing_system_call
      :exit, {reason, _} when reason in [:normal, :killed, :shutdown] -> :attempted_log_call
      error_type, reason -> {error_type, reason}
    end

    test "calls recover_from with empty list for brand new system" do
      # Verifies: brand new system (no survivors) calls Log.recover_from with empty list
      service_pids = %{"test_log" => self()}

      # Brand new system should call recover_from with empty survivor list and version 0
      assert :new_system_call = attempt_copy_log_data("test_log", [], 0, 0, service_pids)
    end

    test "calls recover_from with survivor list for existing system recovery" do
      survivor_pid = spawn(fn -> :timer.sleep(1000) end)
      service_pids = %{"new_log" => self()}

      # Existing system should call recover_from with list of survivor PIDs
      assert :existing_system_call = attempt_copy_log_data("new_log", [survivor_pid], 10, 20, service_pids)
    end
  end
end
