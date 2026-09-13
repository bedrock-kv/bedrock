defmodule Bedrock.DataPlane.Materializer.Olivine.SnapshotRetentionTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Materializer.Olivine.SnapshotRetention

  describe "from_params/1" do
    test "reads the keep-last knob" do
      assert %SnapshotRetention{keep_last: 3} = SnapshotRetention.from_params(%{"snapshot_keep_last" => 3})
    end

    test "leaves retention off for a missing or malformed knob" do
      for params <- [
            %{},
            %{"snapshot_keep_last" => 0},
            %{"snapshot_keep_last" => -1},
            %{"snapshot_keep_last" => "3"},
            %{"snapshot_keep_last" => nil}
          ] do
        assert %SnapshotRetention{keep_last: nil} = SnapshotRetention.from_params(params)
      end
    end
  end

  describe "configured?/1" do
    test "an unset policy would never delete anything" do
      refute SnapshotRetention.configured?(%SnapshotRetention{})
    end

    test "a keep-last policy would" do
      assert SnapshotRetention.configured?(%SnapshotRetention{keep_last: 1})
    end
  end

  describe "oldest_to_keep/2" do
    test "an unset policy keeps everything" do
      assert :keep_all = SnapshotRetention.oldest_to_keep(%SnapshotRetention{}, [40, 30, 20, 10])
    end

    test "keeps everything until the shard has more snapshots than it keeps" do
      policy = %SnapshotRetention{keep_last: 3}

      assert :keep_all = SnapshotRetention.oldest_to_keep(policy, [])
      assert :keep_all = SnapshotRetention.oldest_to_keep(policy, [30, 20])
      assert :keep_all = SnapshotRetention.oldest_to_keep(policy, [30, 20, 10])
    end

    test "floors at the Kth newest once there are more" do
      assert {:ok, 20} = SnapshotRetention.oldest_to_keep(%SnapshotRetention{keep_last: 3}, [40, 30, 20, 10])
      assert {:ok, 30} = SnapshotRetention.oldest_to_keep(%SnapshotRetention{keep_last: 2}, [40, 30, 20, 10])
    end

    test "keep_last: 1 floors at the newest, which is never strictly below itself" do
      assert {:ok, 40} = SnapshotRetention.oldest_to_keep(%SnapshotRetention{keep_last: 1}, [40, 30, 20, 10])
    end
  end
end
