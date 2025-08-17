defmodule Bedrock.ControlPlane.Config.StorageTeamDescriptorTest do
  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.Config.StorageTeamDescriptor

  describe "new/3" do
    test "creates a new storage team descriptor" do
      key_range = {1, 100}
      tag = 1
      storage_ids = ["1", "2"]

      result = StorageTeamDescriptor.storage_team_descriptor(tag, key_range, storage_ids)

      assert %{
               tag: ^tag,
               key_range: ^key_range,
               storage_ids: ^storage_ids
             } = result
    end
  end

  describe "upsert/2" do
    test "inserts a new storage team descriptor" do
      key_range = {1, 100}
      tag = 1
      storage_ids = ["1", "2"]
      new_descriptor = StorageTeamDescriptor.storage_team_descriptor(tag, key_range, storage_ids)

      result = StorageTeamDescriptor.upsert([], new_descriptor)

      assert [new_descriptor] == result
    end

    test "replaces an existing storage team descriptor with the same tag" do
      key_range1 = {1, 100}
      tag = 1
      storage_ids1 = ["1", "2"]

      existing_descriptor =
        StorageTeamDescriptor.storage_team_descriptor(tag, key_range1, storage_ids1)

      key_range2 = {101, 200}
      storage_ids2 = ["1", "2"]

      new_descriptor =
        StorageTeamDescriptor.storage_team_descriptor(tag, key_range2, storage_ids2)

      result = StorageTeamDescriptor.upsert([existing_descriptor], new_descriptor)

      assert [new_descriptor] == result
    end
  end

  describe "find_by_tag/2" do
    test "finds a storage team descriptor by tag" do
      key_range = {1, 100}
      tag = 1
      storage_ids = ["1", "2"]
      descriptor = StorageTeamDescriptor.storage_team_descriptor(tag, key_range, storage_ids)

      result = StorageTeamDescriptor.find_by_tag([descriptor], tag)

      assert descriptor == result
    end

    test "returns nil if no storage team descriptor with the given tag is found" do
      key_range = {1, 100}
      tag = 1
      storage_ids = ["1", "2"]
      descriptor = StorageTeamDescriptor.storage_team_descriptor(tag, key_range, storage_ids)

      result = StorageTeamDescriptor.find_by_tag([descriptor], 2)

      assert result == nil
    end
  end

  describe "remove_by_tag/2" do
    test "removes a storage team descriptor by tag" do
      key_range = {1, 100}
      tag = 1
      storage_ids = ["1", "2"]
      descriptor = StorageTeamDescriptor.storage_team_descriptor(tag, key_range, storage_ids)

      result = StorageTeamDescriptor.remove_by_tag([descriptor], tag)

      assert result == []
    end

    test "does not remove any storage team descriptor if the tag is not found" do
      key_range = {1, 100}
      tag = 1
      storage_ids = ["1", "2"]
      descriptor = StorageTeamDescriptor.storage_team_descriptor(tag, key_range, storage_ids)

      result = StorageTeamDescriptor.remove_by_tag([descriptor], 2)

      assert [descriptor] == result
    end
  end
end
