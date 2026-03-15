defmodule Bedrock.Internal.LayoutRoutingTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.ShardRouter
  alias Bedrock.Internal.LayoutRouting

  describe "ordered_log_ids/1" do
    test "sorts log ids deterministically" do
      logs = %{"log_c" => [], "log_a" => [], "log_b" => []}

      assert LayoutRouting.ordered_log_ids(logs) == ["log_a", "log_b", "log_c"]
    end
  end

  describe "build_log_map/1" do
    test "builds the same index mapping from maps and lists" do
      logs = %{"log_c" => [], "log_a" => [], "log_b" => []}
      log_ids = ["log_c", "log_a", "log_b"]

      assert LayoutRouting.build_log_map(logs) == %{0 => "log_a", 1 => "log_b", 2 => "log_c"}
      assert LayoutRouting.build_log_map(log_ids) == %{0 => "log_a", 1 => "log_b", 2 => "log_c"}
    end
  end

  describe "effective_replication_factor/2" do
    test "clamps to the available log count and never drops below one" do
      assert LayoutRouting.effective_replication_factor(3, 5) == 3
      assert LayoutRouting.effective_replication_factor(3, 2) == 2
      assert LayoutRouting.effective_replication_factor(3, nil) == 3
      assert LayoutRouting.effective_replication_factor(0, 3) == 1
    end
  end

  describe "log_ids_for_shard/3" do
    test "uses the deterministic sorted order for shard routing" do
      logs = %{"log_c" => [], "log_a" => [], "log_b" => []}
      ordered_log_ids = ["log_a", "log_b", "log_c"]

      expected_log_ids =
        7
        |> ShardRouter.get_log_indices(length(ordered_log_ids), 2)
        |> Enum.map(&Enum.at(ordered_log_ids, &1))

      assert LayoutRouting.log_ids_for_shard(logs, 7, 2) == expected_log_ids
    end

    test "returns an empty list when there are no logs" do
      assert LayoutRouting.log_ids_for_shard(%{}, 0, 1) == []
    end
  end

  describe "log_subset_for_shard/3" do
    test "returns the same subset as the routed log ids" do
      logs = %{"log_c" => [], "log_a" => [], "log_b" => []}
      expected_log_ids = LayoutRouting.log_ids_for_shard(logs, 4, 1)

      assert LayoutRouting.log_subset_for_shard(logs, 4, 1) == Map.take(logs, expected_log_ids)
    end
  end
end
