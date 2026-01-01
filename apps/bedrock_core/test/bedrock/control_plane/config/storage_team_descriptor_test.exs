defmodule Bedrock.ControlPlane.Config.StorageTeamDescriptorTest do
  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.Config.StorageTeamDescriptor

  # Test data helpers
  defp sample_descriptor(tag \\ 1, key_range \\ {1, 100}, storage_ids \\ ["1", "2"]) do
    StorageTeamDescriptor.storage_team_descriptor(tag, key_range, storage_ids)
  end

  describe "storage_team_descriptor/3" do
    test "creates a new storage team descriptor" do
      tag = 1
      key_range = {1, 100}
      storage_ids = ["1", "2"]

      assert %{
               tag: ^tag,
               key_range: ^key_range,
               storage_ids: ^storage_ids
             } = sample_descriptor(tag, key_range, storage_ids)
    end
  end

  describe "upsert/2" do
    test "inserts a new storage team descriptor" do
      new_descriptor = sample_descriptor()

      assert [^new_descriptor] = StorageTeamDescriptor.upsert([], new_descriptor)
    end

    test "replaces an existing storage team descriptor with the same tag" do
      tag = 1
      existing_descriptor = sample_descriptor(tag, {1, 100}, ["1", "2"])
      new_descriptor = sample_descriptor(tag, {101, 200}, ["3", "4"])

      assert [^new_descriptor] = StorageTeamDescriptor.upsert([existing_descriptor], new_descriptor)
    end

    test "preserves other descriptors when replacing" do
      descriptor_1 = sample_descriptor(1, {1, 50}, ["1"])
      descriptor_2 = sample_descriptor(2, {51, 100}, ["2"])
      new_descriptor_1 = sample_descriptor(1, {1, 75}, ["1", "3"])

      assert [^new_descriptor_1, ^descriptor_2] =
               StorageTeamDescriptor.upsert([descriptor_1, descriptor_2], new_descriptor_1)
    end

    test "recursively searches nested storage teams for matching tag" do
      # Test the recursive case where tag doesn't match at head
      descriptor_1 = sample_descriptor(1, {1, 50}, ["1"])
      descriptor_2 = sample_descriptor(2, {51, 100}, ["2"])
      descriptor_3 = sample_descriptor(3, {101, 150}, ["3"])
      new_descriptor_2 = sample_descriptor(2, {51, 125}, ["2", "4"])

      # Should skip descriptor_1, find and replace descriptor_2
      assert [^descriptor_1, ^new_descriptor_2, ^descriptor_3] =
               StorageTeamDescriptor.upsert([descriptor_1, descriptor_2, descriptor_3], new_descriptor_2)
    end
  end

  describe "find_by_tag/2" do
    test "finds a storage team descriptor by tag" do
      descriptor = sample_descriptor()

      assert ^descriptor = StorageTeamDescriptor.find_by_tag([descriptor], 1)
    end

    test "finds correct descriptor among multiple" do
      descriptor_1 = sample_descriptor(1, {1, 50}, ["1"])
      descriptor_2 = sample_descriptor(2, {51, 100}, ["2"])

      assert ^descriptor_2 = StorageTeamDescriptor.find_by_tag([descriptor_1, descriptor_2], 2)
    end

    test "returns nil if no storage team descriptor with the given tag is found" do
      descriptor = sample_descriptor(1)

      assert nil == StorageTeamDescriptor.find_by_tag([descriptor], 2)
    end

    test "returns nil for empty list" do
      assert nil == StorageTeamDescriptor.find_by_tag([], 1)
    end
  end

  describe "remove_by_tag/2" do
    test "removes a storage team descriptor by tag" do
      descriptor = sample_descriptor()

      assert [] == StorageTeamDescriptor.remove_by_tag([descriptor], 1)
    end

    test "removes only matching descriptor among multiple" do
      descriptor_1 = sample_descriptor(1, {1, 50}, ["1"])
      descriptor_2 = sample_descriptor(2, {51, 100}, ["2"])

      assert [^descriptor_2] = StorageTeamDescriptor.remove_by_tag([descriptor_1, descriptor_2], 1)
    end

    test "does not remove any storage team descriptor if the tag is not found" do
      descriptor = sample_descriptor(1)

      assert [^descriptor] = StorageTeamDescriptor.remove_by_tag([descriptor], 2)
    end

    test "returns empty list when removing from empty list" do
      assert [] == StorageTeamDescriptor.remove_by_tag([], 1)
    end
  end
end
