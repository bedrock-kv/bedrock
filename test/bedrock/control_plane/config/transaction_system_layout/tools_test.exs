defmodule Bedrock.ControlPlane.Config.TransactionSystemLayout.ChangesTest do
  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.ControlPlane.Config.LogDescriptor
  alias Bedrock.ControlPlane.Config.StorageTeamDescriptor

  import Bedrock.ControlPlane.Config.TransactionSystemLayout.Changes

  describe "put_controller/2" do
    test "sets the controller pid" do
      layout = %TransactionSystemLayout{}
      controller_pid = self()
      updated_layout = put_controller(layout, controller_pid)

      refute updated_layout.id == layout.id
      assert updated_layout.controller == controller_pid
    end
  end

  describe "upsert_log_descriptor/2" do
    test "inserts a log descriptor" do
      expected_id = "log1"
      layout = %TransactionSystemLayout{}
      log_descriptor = LogDescriptor.log_descriptor(expected_id, [1, 2, 3])
      updated_layout = upsert_log_descriptor(layout, log_descriptor)

      refute updated_layout.id == layout.id
      assert Enum.any?(updated_layout.logs, fn log -> log.log_id == expected_id end)
    end
  end

  describe "find_log_by_id/2" do
    test "finds a log descriptor by id" do
      log_descriptor = LogDescriptor.log_descriptor("log1", [1, 2, 3])
      layout = %TransactionSystemLayout{logs: [log_descriptor]}
      found_log = find_log_by_id(layout, "log1")

      assert found_log == log_descriptor
    end

    test "returns nil if log descriptor not found" do
      layout = %TransactionSystemLayout{}
      found_log = find_log_by_id(layout, 1)

      assert found_log == nil
    end
  end

  describe "remove_log_with_id/2" do
    test "removes a log descriptor by id" do
      log_descriptor = LogDescriptor.log_descriptor("log1", [1, 2, 3])
      layout = %TransactionSystemLayout{logs: [log_descriptor]}
      updated_layout = remove_log_with_id(layout, "log1")

      refute updated_layout.id == layout.id
      assert Enum.empty?(updated_layout.logs)
    end
  end

  describe "upsert_storage_team_descriptor/2" do
    test "inserts a storage team descriptor" do
      layout = %TransactionSystemLayout{}

      storage_team_descriptor =
        StorageTeamDescriptor.storage_team_descriptor(:team1, {<<0x00>>, <<0xFF>>}, [
          "storage1",
          "storage2"
        ])

      updated_layout =
        upsert_storage_team_descriptor(layout, storage_team_descriptor)

      refute updated_layout.id == layout.id
      assert Enum.any?(updated_layout.storage_teams, fn team -> team.tag == :team1 end)
    end
  end

  describe "find_storage_team_by_tag/2" do
    test "finds a storage team descriptor by tag" do
      storage_team_descriptor =
        StorageTeamDescriptor.storage_team_descriptor(:team1, {<<0x00>>, <<0xFF>>}, [
          "storage1",
          "storage2"
        ])

      layout = %TransactionSystemLayout{storage_teams: [storage_team_descriptor]}
      found_team = find_storage_team_by_tag(layout, :team1)

      assert found_team == storage_team_descriptor
    end

    test "returns nil if storage team descriptor not found" do
      layout = %TransactionSystemLayout{}
      found_team = find_storage_team_by_tag(layout, :team1)

      assert found_team == nil
    end
  end

  describe "remove_storage_team_with_tag/2" do
    test "removes a storage team descriptor by tag" do
      storage_team_descriptor =
        StorageTeamDescriptor.storage_team_descriptor(:team1, {<<0x00>>, <<0xFF>>}, [
          "storage1",
          "storage2"
        ])

      layout = %TransactionSystemLayout{storage_teams: [storage_team_descriptor]}
      updated_layout = remove_storage_team_with_tag(layout, :team1)

      refute updated_layout.id == layout.id
      assert Enum.empty?(updated_layout.storage_teams)
    end
  end
end
