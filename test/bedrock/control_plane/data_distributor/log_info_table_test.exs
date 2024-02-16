defmodule Bedrock.ControlPlane.DataDistributor.LogInfoTableTest do
  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.DataDistributor.LogInfo
  alias Bedrock.ControlPlane.DataDistributor.LogInfoTable

  describe "DataDistributor.LogInfoTable.add_log_info/2" do
    test "works correctly when ids do not conflict" do
      t = LogInfoTable.new()
      log_info = LogInfo.new("id", 1, "endpoint")
      assert :ok = LogInfoTable.add_log_info(t, log_info)

      assert [
               {{:tag, 1}, "id", 1, "endpoint"},
               {{:id, "id"}, "id", 1, "endpoint"}
             ] = :ets.tab2list(t)
    end

    test "works correctly when ids do not conflict, but tags are duplicated" do
      t = LogInfoTable.new()
      log_info1 = LogInfo.new("id1", 1, "endpoint1")
      log_info2 = LogInfo.new("id2", 1, "endpoint2")
      assert :ok = LogInfoTable.add_log_info(t, log_info1)
      assert :ok = LogInfoTable.add_log_info(t, log_info2)

      assert [
               {{:id, "id1"}, "id1", 1, "endpoint1"},
               {{:tag, 1}, "id1", 1, "endpoint1"},
               {{:tag, 1}, "id2", 1, "endpoint2"},
               {{:id, "id2"}, "id2", 1, "endpoint2"}
             ] = :ets.tab2list(t)
    end

    test "returns the correct error when given a log_info that already exists" do
      t = LogInfoTable.new()
      log_info = LogInfo.new("id", 1, "endpoint")
      assert :ok = LogInfoTable.add_log_info(t, log_info)
      assert {:error, :already_exists} = LogInfoTable.add_log_info(t, log_info)

      assert [
               {{:tag, 1}, "id", 1, "endpoint"},
               {{:id, "id"}, "id", 1, "endpoint"}
             ] = :ets.tab2list(t)
    end
  end

  describe "DataDistributor.LogInfoTable.log_info_for_id/2" do
    test "returns the correct log_info when given an id that exists" do
      t = LogInfoTable.new()
      log_info = LogInfo.new("id", 1, "endpoint")
      assert :ok = LogInfoTable.add_log_info(t, log_info)
      assert {:ok, ^log_info} = LogInfoTable.log_info_for_id(t, "id")
    end

    test "returns the correct error when given an id that does not exist" do
      t = LogInfoTable.new()
      log_info = LogInfo.new("id", 1, "endpoint")
      assert :ok = LogInfoTable.add_log_info(t, log_info)
      assert {:error, :not_found} = LogInfoTable.log_info_for_id(t, "id2")
    end
  end

  describe "DataDistributor.LogInfoTable.log_infos_for_tag/2" do
    test "returns the correct log_infos when given a tag that exists" do
      t = LogInfoTable.new()
      log_info1 = LogInfo.new("id1", 1, "endpoint1")
      log_info2 = LogInfo.new("id2", 1, "endpoint2")
      assert :ok = LogInfoTable.add_log_info(t, log_info1)
      assert :ok = LogInfoTable.add_log_info(t, log_info2)
      assert {:ok, [^log_info1, ^log_info2]} = LogInfoTable.log_infos_for_tag(t, 1)
    end
  end
end
