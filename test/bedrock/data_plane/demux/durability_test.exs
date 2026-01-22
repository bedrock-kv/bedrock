defmodule Bedrock.DataPlane.Demux.DurabilityTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Demux.Durability

  describe "new/0" do
    test "creates empty durability tracker" do
      durability = Durability.new()
      assert Durability.active_shard_count(durability) == 0
      assert Durability.min_durable_version(durability) == nil
    end
  end

  describe "activate_shard/3" do
    test "activates a new shard" do
      durability = Durability.new()
      assert {:ok, durability} = Durability.activate_shard(durability, 0, 1000)
      assert Durability.active?(durability, 0)
      assert Durability.shard_version(durability, 0) == 1000
    end

    test "returns error for already active shard" do
      durability = Durability.new()
      {:ok, durability} = Durability.activate_shard(durability, 0, 1000)
      assert {:error, :already_active} = Durability.activate_shard(durability, 0, 2000)
    end

    test "can activate multiple shards" do
      durability = Durability.new()
      {:ok, durability} = Durability.activate_shard(durability, 0, 1000)
      {:ok, durability} = Durability.activate_shard(durability, 5, 500)
      {:ok, durability} = Durability.activate_shard(durability, 10, 2000)

      assert Durability.active_shard_count(durability) == 3
      assert Durability.shard_version(durability, 0) == 1000
      assert Durability.shard_version(durability, 5) == 500
      assert Durability.shard_version(durability, 10) == 2000
    end
  end

  describe "update_shard/3" do
    test "updates shard version" do
      durability = Durability.new()
      {:ok, durability} = Durability.activate_shard(durability, 0, 1000)
      {:ok, durability} = Durability.update_shard(durability, 0, 2000)

      assert Durability.shard_version(durability, 0) == 2000
    end

    test "returns error for inactive shard" do
      durability = Durability.new()
      assert {:error, :not_active} = Durability.update_shard(durability, 0, 1000)
    end

    test "returns error for version going backwards" do
      durability = Durability.new()
      {:ok, durability} = Durability.activate_shard(durability, 0, 2000)
      assert {:error, :version_going_backwards} = Durability.update_shard(durability, 0, 1000)
    end

    test "allows same version (no-op)" do
      durability = Durability.new()
      {:ok, durability} = Durability.activate_shard(durability, 0, 1000)
      {:ok, durability} = Durability.update_shard(durability, 0, 1000)
      assert Durability.shard_version(durability, 0) == 1000
    end
  end

  describe "min_durable_version/1" do
    test "returns nil for empty tracker" do
      durability = Durability.new()
      assert Durability.min_durable_version(durability) == nil
    end

    test "returns single shard version" do
      durability = Durability.new()
      {:ok, durability} = Durability.activate_shard(durability, 0, 1000)
      assert Durability.min_durable_version(durability) == 1000
    end

    test "returns minimum across multiple shards" do
      durability = Durability.new()
      {:ok, durability} = Durability.activate_shard(durability, 0, 1000)
      {:ok, durability} = Durability.activate_shard(durability, 1, 500)
      {:ok, durability} = Durability.activate_shard(durability, 2, 2000)

      assert Durability.min_durable_version(durability) == 500
    end

    test "updates minimum when shard advances" do
      durability = Durability.new()
      {:ok, durability} = Durability.activate_shard(durability, 0, 1000)
      {:ok, durability} = Durability.activate_shard(durability, 1, 500)

      assert Durability.min_durable_version(durability) == 500

      # Advance shard 1
      {:ok, durability} = Durability.update_shard(durability, 1, 1500)
      assert Durability.min_durable_version(durability) == 1000

      # Advance shard 0
      {:ok, durability} = Durability.update_shard(durability, 0, 2000)
      assert Durability.min_durable_version(durability) == 1500
    end
  end

  describe "deactivate_shard/2" do
    test "removes shard from tracking" do
      durability = Durability.new()
      {:ok, durability} = Durability.activate_shard(durability, 0, 1000)
      durability = Durability.deactivate_shard(durability, 0)

      refute Durability.active?(durability, 0)
      assert Durability.shard_version(durability, 0) == nil
    end

    test "updates min when deactivating minimum shard" do
      durability = Durability.new()
      {:ok, durability} = Durability.activate_shard(durability, 0, 1000)
      {:ok, durability} = Durability.activate_shard(durability, 1, 500)

      assert Durability.min_durable_version(durability) == 500

      durability = Durability.deactivate_shard(durability, 1)
      assert Durability.min_durable_version(durability) == 1000
    end

    test "handles deactivating non-existent shard" do
      durability = Durability.new()
      durability = Durability.deactivate_shard(durability, 999)
      assert Durability.active_shard_count(durability) == 0
    end
  end

  describe "active_shards/1" do
    test "returns list of active shard IDs" do
      durability = Durability.new()
      {:ok, durability} = Durability.activate_shard(durability, 5, 1000)
      {:ok, durability} = Durability.activate_shard(durability, 10, 2000)
      {:ok, durability} = Durability.activate_shard(durability, 0, 500)

      shards = durability |> Durability.active_shards() |> Enum.sort()
      assert shards == [0, 5, 10]
    end
  end
end
