defmodule Bedrock.ControlPlane.Config.LogDescriptorTest do
  use ExUnit.Case, async: true
  import Bedrock.ControlPlane.Config.LogDescriptor

  describe "new/2" do
    test "creates a new LogDescriptor struct" do
      log_id = 1
      tags = [1, 2, 3]
      log_descriptor = log_descriptor(log_id, tags)

      assert %{log_id: ^log_id, tags: ^tags} = log_descriptor
    end
  end

  describe "upsert/2" do
    test "inserts a new log descriptor into an empty list" do
      log_descriptor = %{log_id: 1, tags: [1, 2, 3]}
      assert [log_descriptor] == upsert([], log_descriptor)
    end

    test "replaces an existing log descriptor with the same id" do
      existing = %{log_id: 1, tags: [1, 2, 3]}
      new = %{log_id: 1, tags: [4, 5, 6]}
      assert [new] == upsert([existing], new)
    end

    test "adds a new log descriptor to the list if id does not match" do
      existing = %{log_id: 1, tags: [1, 2, 3]}
      new = %{log_id: 2, tags: [4, 5, 6]}
      assert [existing, new] == upsert([existing], new)
    end
  end

  describe "find_by_id/2" do
    test "finds a log descriptor by id" do
      log_descriptor = %{log_id: 1, tags: [1, 2, 3]}
      assert log_descriptor == find_by_id([log_descriptor], 1)
    end

    test "returns nil if no log descriptor with the given id is found" do
      log_descriptor = %{log_id: 1, tags: [1, 2, 3]}
      assert nil == find_by_id([log_descriptor], 2)
    end
  end

  describe "remove_by_id/2" do
    test "removes a log descriptor by id" do
      log_descriptor = %{log_id: 1, tags: [1, 2, 3]}
      assert [] == remove_by_id([log_descriptor], 1)
    end

    test "does not remove any log descriptor if id does not match" do
      log_descriptor = %{log_id: 1, tags: [1, 2, 3]}
      assert [log_descriptor] == remove_by_id([log_descriptor], 2)
    end
  end
end
