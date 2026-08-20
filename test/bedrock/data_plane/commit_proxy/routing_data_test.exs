defmodule Bedrock.DataPlane.CommitProxy.RoutingDataTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.CommitProxy.RoutingData
  alias Bedrock.DataPlane.ShardRouter
  alias Bedrock.DataPlane.Version
  alias Bedrock.SystemKeys
  alias Bedrock.SystemKeys.Values

  defp v(n), do: Version.from_integer(n)

  # Shard boundaries as a plain sorted list of {end_key, {tag, start_key}}.
  defp shard_list(%RoutingData{shards: shards}), do: :gb_trees.to_list(shards)

  describe "from_snapshot/1" do
    test "builds fully-populated routing data" do
      snapshot = %{
        shard_layout: %{"m" => {1, ""}, <<0xFF, 0xFF>> => {0, "m"}},
        log_map: %{0 => "log-a", 1 => "log-b"},
        log_services: %{"log-a" => {:log_a, :node1}, "log-b" => {:log_b, :node2}},
        replication_factor: 2
      }

      routing_data = RoutingData.from_snapshot(snapshot)

      assert shard_list(routing_data) == [{"m", {1, ""}}, {<<0xFF, 0xFF>>, {0, "m"}}]
      assert routing_data.log_map == %{0 => "log-a", 1 => "log-b"}
      assert routing_data.log_services == %{"log-a" => {:log_a, :node1}, "log-b" => {:log_b, :node2}}
      assert routing_data.replication_factor == 2
    end

    test "builds empty routing data from an empty snapshot" do
      snapshot = %{shard_layout: %{}, log_map: %{}, log_services: %{}, replication_factor: 1}

      routing_data = RoutingData.from_snapshot(snapshot)

      assert shard_list(routing_data) == []
      assert routing_data.replication_factor == 1
    end

    test "is a plain immutable value: derived copies do not affect the original" do
      original =
        RoutingData.from_snapshot(%{
          shard_layout: %{"m" => {1, ""}},
          log_map: %{},
          log_services: %{},
          replication_factor: 1
        })

      _derived = RoutingData.insert_shard(original, "z", 2, "m")

      assert shard_list(original) == [{"m", {1, ""}}]
    end
  end

  describe "new_empty/0" do
    test "creates empty routing data with all fields initialized" do
      routing_data = RoutingData.new_empty()

      assert %RoutingData{} = routing_data
      assert shard_list(routing_data) == []
      assert routing_data.log_map == %{}
      assert routing_data.log_services == %{}
      assert routing_data.replication_factor == 1
    end
  end

  describe "insert_shard/4 and delete_shard/2" do
    test "inserts and overwrites shard entries" do
      routing_data =
        RoutingData.new_empty()
        |> RoutingData.insert_shard("m", 1, "")
        |> RoutingData.insert_shard(<<0xFF, 0xFF>>, 2, "m")
        |> RoutingData.insert_shard("m", 7, "")

      assert shard_list(routing_data) == [{"m", {7, ""}}, {<<0xFF, 0xFF>>, {2, "m"}}]
    end

    test "deletes a shard entry; deleting an absent key is a no-op" do
      routing_data =
        RoutingData.new_empty()
        |> RoutingData.insert_shard("m", 1, "")
        |> RoutingData.delete_shard("m")
        |> RoutingData.delete_shard("never_there")

      assert shard_list(routing_data) == []
    end
  end

  describe "insert_log/2" do
    test "adds log to empty log_map at index 0" do
      updated = RoutingData.insert_log(RoutingData.new_empty(), "log-1")

      assert updated.log_map == %{0 => "log-1"}
    end

    test "adds logs at sequential indices" do
      updated =
        RoutingData.new_empty()
        |> RoutingData.insert_log("log-a")
        |> RoutingData.insert_log("log-b")
        |> RoutingData.insert_log("log-c")

      assert updated.log_map == %{0 => "log-a", 1 => "log-b", 2 => "log-c"}
    end

    test "is idempotent by log id: a re-inserted log keeps its index" do
      updated =
        RoutingData.new_empty()
        |> RoutingData.insert_log("log-a")
        |> RoutingData.insert_log("log-b")
        |> RoutingData.insert_log("log-a")

      assert updated.log_map == %{0 => "log-a", 1 => "log-b"}
    end

    test "does not modify other fields" do
      routing_data = RoutingData.new_empty()

      updated = RoutingData.insert_log(routing_data, "log-1")

      assert updated.shards == routing_data.shards
      assert updated.log_services == %{}
      assert updated.replication_factor == 1
    end
  end

  describe "remove_log/2" do
    test "removes log from log_map and reindexes to contiguous indices" do
      updated =
        RoutingData.new_empty()
        |> RoutingData.insert_log("log-a")
        |> RoutingData.insert_log("log-b")
        |> RoutingData.insert_log("log-c")
        |> RoutingData.remove_log("log-b")

      assert updated.log_map == %{0 => "log-a", 1 => "log-c"}
    end

    test "removes first log and reindexes" do
      updated =
        RoutingData.new_empty()
        |> RoutingData.insert_log("log-a")
        |> RoutingData.insert_log("log-b")
        |> RoutingData.insert_log("log-c")
        |> RoutingData.remove_log("log-a")

      assert updated.log_map == %{0 => "log-b", 1 => "log-c"}
    end

    test "no-op if log not found, including on an empty map" do
      updated =
        RoutingData.new_empty()
        |> RoutingData.insert_log("log-a")
        |> RoutingData.remove_log("nonexistent")

      assert updated.log_map == %{0 => "log-a"}
      assert RoutingData.remove_log(RoutingData.new_empty(), "nonexistent").log_map == %{}
    end
  end

  describe "put_log_service/3 and delete_log_service/2" do
    test "adds, overwrites, and removes service refs" do
      routing_data =
        RoutingData.new_empty()
        |> RoutingData.put_log_service("log-1", {:log_1, :n1@host})
        |> RoutingData.put_log_service("log-2", {:log_2, :n2@host})
        |> RoutingData.put_log_service("log-1", {:log_1b, :n3@host})

      assert routing_data.log_services == %{"log-1" => {:log_1b, :n3@host}, "log-2" => {:log_2, :n2@host}}

      assert RoutingData.delete_log_service(routing_data, "log-1").log_services ==
               %{"log-2" => {:log_2, :n2@host}}

      assert RoutingData.delete_log_service(routing_data, "nonexistent").log_services ==
               routing_data.log_services
    end
  end

  describe "set_replication_factor/2" do
    test "updates replication factor without touching other fields" do
      routing_data = RoutingData.insert_log(RoutingData.new_empty(), "log-1")

      updated = RoutingData.set_replication_factor(routing_data, 3)

      assert updated.replication_factor == 3
      assert updated.log_map == %{0 => "log-1"}
    end
  end

  describe "integration: typical usage pattern" do
    test "builds complete routing data incrementally and routes with it" do
      routing_data =
        RoutingData.new_empty()
        |> RoutingData.insert_log("log-1")
        |> RoutingData.insert_log("log-2")
        |> RoutingData.insert_log("log-3")
        |> RoutingData.put_log_service("log-1", {:log_1, :n1@host})
        |> RoutingData.put_log_service("log-2", {:log_2, :n2@host})
        |> RoutingData.put_log_service("log-3", {:log_3, :n3@host})
        |> RoutingData.insert_shard("m", 0, "")
        |> RoutingData.insert_shard("z", 1, "m")
        |> RoutingData.insert_shard(<<0xFF, 0xFF>>, 2, "z")
        |> RoutingData.set_replication_factor(3)

      assert routing_data.replication_factor == 3

      assert shard_list(routing_data) == [
               {"m", {0, ""}},
               {"z", {1, "m"}},
               {<<0xFF, 0xFF>>, {2, "z"}}
             ]

      assert ShardRouter.lookup_shard(routing_data.shards, "apple") == 0
      assert ShardRouter.lookup_shard(routing_data.shards, "pear") == 1
      assert ShardRouter.lookup_shard(routing_data.shards, <<0xFF, "/system/x">>) == 2
    end
  end

  describe "apply_mutations/2" do
    test "handles shard_key set mutation" do
      updates = [{v(100), [{:set, SystemKeys.shard_key("m"), Values.encode_shard_key_entry(42, "")}]}]

      updated = RoutingData.apply_mutations(RoutingData.new_empty(), updates)

      assert shard_list(updated) == [{"m", {42, ""}}]
    end

    test "handles multiple shard_key mutations" do
      updates = [
        {v(100),
         [
           {:set, SystemKeys.shard_key("a"), Values.encode_shard_key_entry(1, "")},
           {:set, SystemKeys.shard_key("m"), Values.encode_shard_key_entry(2, "a")},
           {:set, SystemKeys.shard_key("z"), Values.encode_shard_key_entry(3, "m")}
         ]}
      ]

      updated = RoutingData.apply_mutations(RoutingData.new_empty(), updates)

      assert shard_list(updated) == [{"a", {1, ""}}, {"m", {2, "a"}}, {"z", {3, "m"}}]
    end

    test "skips a shard_key set whose value does not decode, keeping the last good entry" do
      routing_data = RoutingData.insert_shard(RoutingData.new_empty(), "m", 42, "")

      updates = [{v(100), [{:set, SystemKeys.shard_key("m"), <<0xEE, 0xEE, 0xEE>>}]}]

      updated = RoutingData.apply_mutations(routing_data, updates)

      assert shard_list(updated) == [{"m", {42, ""}}]
    end

    test "handles layout_log set mutation - updates log_map only" do
      updates = [{v(100), [{:set, SystemKeys.layout_log("log-1"), Values.encode_tag_list([0])}]}]

      updated = RoutingData.apply_mutations(RoutingData.new_empty(), updates)

      assert updated.log_map == %{0 => "log-1"}
      # log_services are NOT populated from persisted data
      assert updated.log_services == %{}
    end

    test "a re-set layout_log key keeps its index" do
      updates = [
        {v(100), [{:set, SystemKeys.layout_log("log-1"), Values.encode_tag_list([0])}]},
        {v(101),
         [
           {:set, SystemKeys.layout_log("log-1"), Values.encode_tag_list([1])},
           {:set, SystemKeys.layout_log("log-2"), Values.encode_tag_list([2])}
         ]}
      ]

      updated = RoutingData.apply_mutations(RoutingData.new_empty(), updates)

      assert updated.log_map == %{0 => "log-1", 1 => "log-2"}
    end

    test "handles shard_key clear mutation" do
      routing_data = RoutingData.insert_shard(RoutingData.new_empty(), "m", 42, "")

      updates = [{v(100), [{:clear, SystemKeys.shard_key("m")}]}]

      updated = RoutingData.apply_mutations(routing_data, updates)

      assert shard_list(updated) == []
    end

    test "handles layout_log clear mutation" do
      routing_data =
        RoutingData.new_empty()
        |> RoutingData.insert_log("log-123")
        |> RoutingData.put_log_service("log-123", {:my_log, :node@host})

      updates = [{v(100), [{:clear, SystemKeys.layout_log("log-123")}]}]

      updated = RoutingData.apply_mutations(routing_data, updates)

      assert updated.log_services == %{}
      assert updated.log_map == %{}
    end

    test "applies updates from multiple versions in order; later version wins" do
      updates = [
        {v(100), [{:set, SystemKeys.shard_key("a"), Values.encode_shard_key_entry(1, "")}]},
        {v(101), [{:set, SystemKeys.shard_key("b"), Values.encode_shard_key_entry(2, "a")}]},
        {v(102), [{:set, SystemKeys.shard_key("a"), Values.encode_shard_key_entry(99, "")}]}
      ]

      updated = RoutingData.apply_mutations(RoutingData.new_empty(), updates)

      assert shard_list(updated) == [{"a", {99, ""}}, {"b", {2, "a"}}]
    end

    test "ignores unknown system keys and non-system keys" do
      updates = [
        {v(100),
         [
           {:set, "\xff/system/unknown/foo", "bar"},
           {:set, "user/data", "value"}
         ]}
      ]

      updated = RoutingData.apply_mutations(RoutingData.new_empty(), updates)

      assert updated.log_map == %{}
      assert shard_list(updated) == []
    end

    test "ignores unsupported mutation types" do
      routing_data = RoutingData.new_empty()

      updates = [{v(100), [{:atomic, :add, "key", <<1>>}]}]

      assert RoutingData.apply_mutations(routing_data, updates) == routing_data
    end

    test "clear_range removes shard entries whose full key falls in range" do
      routing_data =
        RoutingData.new_empty()
        |> RoutingData.insert_shard("a", 1, "")
        |> RoutingData.insert_shard("m", 2, "a")
        |> RoutingData.insert_shard("z", 3, "m")

      # Clear [shard_key("a"), shard_key("z")) - end is exclusive, so "z" survives
      updates = [{v(100), [{:clear_range, SystemKeys.shard_key("a"), SystemKeys.shard_key("z")}]}]

      updated = RoutingData.apply_mutations(routing_data, updates)

      assert shard_list(updated) == [{"z", {3, "m"}}]
    end

    test "clear_range over the shard_keys prefix removes all shard entries" do
      routing_data =
        RoutingData.new_empty()
        |> RoutingData.insert_shard("m", 1, "")
        |> RoutingData.insert_shard(<<0xFF, 0xFF>>, 2, "m")

      {start_key, end_key} = Bedrock.KeyRange.from_prefix(SystemKeys.shard_keys_prefix())
      updates = [{v(100), [{:clear_range, start_key, end_key}]}]

      updated = RoutingData.apply_mutations(routing_data, updates)

      assert shard_list(updated) == []
    end

    test "clear_range removes layout_log entries in range, reindexing log_map and dropping services" do
      routing_data =
        RoutingData.new_empty()
        |> RoutingData.insert_log("log-a")
        |> RoutingData.insert_log("log-b")
        |> RoutingData.insert_log("log-c")
        |> RoutingData.put_log_service("log-a", {:log_a, :n1@host})
        |> RoutingData.put_log_service("log-b", {:log_b, :n2@host})

      # Clear [layout_log("log-a"), layout_log("log-c")) - "log-c" survives
      updates = [{v(100), [{:clear_range, SystemKeys.layout_log("log-a"), SystemKeys.layout_log("log-c")}]}]

      updated = RoutingData.apply_mutations(routing_data, updates)

      assert updated.log_map == %{0 => "log-c"}
      assert updated.log_services == %{}
    end

    test "clear_range outside routing families leaves routing data unchanged" do
      routing_data =
        RoutingData.new_empty()
        |> RoutingData.insert_shard("m", 1, "")
        |> RoutingData.insert_log("log-1")

      updates = [{v(100), [{:clear_range, "user/a", "user/z"}]}]

      updated = RoutingData.apply_mutations(routing_data, updates)

      assert shard_list(updated) == [{"m", {1, ""}}]
      assert updated.log_map == %{0 => "log-1"}
    end

    test "recovery-style rewrite: clear_range then sets shrinks 3 shards to 2" do
      routing_data =
        RoutingData.new_empty()
        |> RoutingData.insert_shard("g", 1, "")
        |> RoutingData.insert_shard("p", 2, "g")
        |> RoutingData.insert_shard(<<0xFF, 0xFF>>, 3, "p")

      {start_key, end_key} = Bedrock.KeyRange.from_prefix(SystemKeys.shard_keys_prefix())

      updates = [
        {v(100),
         [
           {:clear_range, start_key, end_key},
           {:set, SystemKeys.shard_key("m"), Values.encode_shard_key_entry(10, "")},
           {:set, SystemKeys.shard_key(<<0xFF, 0xFF>>), Values.encode_shard_key_entry(11, "m")}
         ]}
      ]

      updated = RoutingData.apply_mutations(routing_data, updates)

      assert shard_list(updated) == [{"m", {10, ""}}, {<<0xFF, 0xFF>>, {11, "m"}}]
    end

    test "handles mixed shard_key and layout_log mutations" do
      updates = [
        {v(100),
         [
           {:set, SystemKeys.shard_key("m"), Values.encode_shard_key_entry(1, "")},
           {:set, SystemKeys.layout_log("log-1"), Values.encode_tag_list([0])},
           {:set, SystemKeys.shard_key("z"), Values.encode_shard_key_entry(2, "m")},
           {:set, SystemKeys.layout_log("log-2"), Values.encode_tag_list([1])}
         ]}
      ]

      updated = RoutingData.apply_mutations(RoutingData.new_empty(), updates)

      assert shard_list(updated) == [{"m", {1, ""}}, {"z", {2, "m"}}]
      assert updated.log_map == %{0 => "log-1", 1 => "log-2"}
      assert updated.log_services == %{}
    end
  end
end
