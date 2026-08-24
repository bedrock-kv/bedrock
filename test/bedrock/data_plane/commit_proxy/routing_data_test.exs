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

    test "carries materializer refs when present; defaults to empty when absent" do
      base = %{shard_layout: %{}, log_map: %{}, log_services: %{}, replication_factor: 1}

      assert RoutingData.from_snapshot(base).materializers == %{}

      with_refs = Map.put(base, :materializers, %{0 => %{"wkr_sys" => "n1@host"}})
      assert RoutingData.from_snapshot(with_refs).materializers == %{0 => %{"wkr_sys" => "n1@host"}}
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

  describe "covering_entry/2" do
    defp routing_two_shards do
      RoutingData.from_snapshot(%{
        shard_layout: %{"m" => {1, ""}, <<0xFF, 0xFF>> => {0, "m"}},
        log_map: %{0 => "log-a"},
        log_services: %{"log-a" => {:log_a, :node1}},
        materializers: %{0 => %{"wkr_sys" => "n1@host"}, 1 => %{"wkr_a" => "n1@host"}},
        replication_factor: 1
      })
    end

    test "answers one covering entry per key by ceiling walk; log wiring stays proxy-internal" do
      routing = routing_two_shards()

      assert RoutingData.covering_entry(routing, "apple") == {:ok, {"", "m", 1, {"wkr_a", "n1@host"}}}
      # An end key is exclusive: "m" belongs to the NEXT shard.
      assert RoutingData.covering_entry(routing, "m") == {:ok, {"m", <<0xFF, 0xFF>>, 0, {"wkr_sys", "n1@host"}}}
      assert RoutingData.covering_entry(routing, "zebra") == {:ok, {"m", <<0xFF, 0xFF>>, 0, {"wkr_sys", "n1@host"}}}
    end

    test "a key beyond every boundary is :not_found" do
      assert RoutingData.covering_entry(routing_two_shards(), <<0xFF, 0xFF>>) == {:error, :not_found}
      assert RoutingData.covering_entry(RoutingData.new_empty(), "a") == {:error, :not_found}
    end

    test "a shard whose tag names no materializer is :not_found — an unroutable key" do
      routing =
        RoutingData.from_snapshot(%{
          shard_layout: %{"m" => {1, ""}},
          log_map: %{},
          log_services: %{},
          materializers: %{},
          replication_factor: 1
        })

      assert RoutingData.covering_entry(routing, "apple") == {:error, :not_found}
    end

    test "a tag whose members are all gone is :not_found — an empty set is no coverage" do
      routing =
        RoutingData.from_snapshot(%{
          shard_layout: %{"m" => {1, ""}},
          log_map: %{},
          log_services: %{},
          materializers: %{1 => %{}},
          replication_factor: 1
        })

      assert RoutingData.covering_entry(routing, "apple") == {:error, :not_found}
    end
  end

  describe "a tag's members: selection and membership" do
    defp with_members(members) do
      RoutingData.from_snapshot(%{
        shard_layout: %{"m" => {1, ""}},
        log_map: %{},
        log_services: %{},
        materializers: %{1 => members},
        replication_factor: 1
      })
    end

    test "routing prefers a real worker over the placeholder — coverage beats parking" do
      routing = with_members(%{SystemKeys.placeholder_worker_id() => "n0@host", "wkr_a" => "n1@host"})

      assert RoutingData.covering_entry(routing, "apple") == {:ok, {"", "m", 1, {"wkr_a", "n1@host"}}}
    end

    test "the placeholder answers only when no real worker does" do
      placeholder = SystemKeys.placeholder_worker_id()
      routing = with_members(%{placeholder => "n0@host"})

      assert RoutingData.covering_entry(routing, "apple") == {:ok, {"", "m", 1, {placeholder, "n0@host"}}}
    end

    test "the pick among several real workers is deterministic — every proxy answers alike" do
      routing = with_members(%{"wkr_c" => "n3@host", "wkr_a" => "n1@host", "wkr_b" => "n2@host"})

      assert {:ok, {_, _, _, picked}} = RoutingData.covering_entry(routing, "apple")
      assert picked == {"wkr_a", "n1@host"}

      # Insertion order must not change the answer.
      reordered = with_members(%{"wkr_b" => "n2@host", "wkr_c" => "n3@host", "wkr_a" => "n1@host"})
      assert {:ok, {_, _, _, ^picked}} = RoutingData.covering_entry(reordered, "apple")
    end

    test "materializer_members/2 answers the whole set — the worker's own question is membership" do
      routing = with_members(%{"wkr_a" => "n1@host", "wkr_b" => "n2@host"})

      assert RoutingData.materializer_members(routing, 1) == {:ok, %{"wkr_a" => "n1@host", "wkr_b" => "n2@host"}}
      assert RoutingData.materializer_members(routing, 9) == {:error, :not_found}
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

  describe "integration: typical usage pattern" do
    test "builds complete routing data from the seed and routes with it" do
      routing_data =
        %{
          shard_layout: %{},
          log_map: %{0 => "log-1", 1 => "log-2", 2 => "log-3"},
          log_services: %{"log-1" => {:log_1, :n1@host}, "log-2" => {:log_2, :n2@host}, "log-3" => {:log_3, :n3@host}},
          replication_factor: 3
        }
        |> RoutingData.from_snapshot()
        |> RoutingData.insert_shard("m", 0, "")
        |> RoutingData.insert_shard("z", 1, "m")
        |> RoutingData.insert_shard(<<0xFF, 0xFF>>, 2, "z")

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

  # An unlock-seeded wiring fixture: log wiring arrives in the seed and is
  # epoch-constant thereafter.
  defp seeded_wiring do
    RoutingData.from_snapshot(%{
      shard_layout: %{},
      log_map: %{0 => "log-a", 1 => "log-b"},
      log_services: %{"log-a" => {:log_a, :n1@host}, "log-b" => {:log_b, :n2@host}},
      replication_factor: 2
    })
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

    test "unrecognized system families are ignored — the fold reads only what it routes on" do
      # Log wiring is epoch-constant and rides the unlock seed, so no
      # window can carry a log-topology change (changing log topology IS
      # a recovery). An unknown family is forward-compatibility, not an
      # error: it must never disturb the wiring this proxy was seeded
      # with.
      seeded = seeded_wiring()
      foreign = "\xff/system/some_future_family/entry"

      updates = [{v(100), [{:set, foreign, "whatever"}, {:clear, foreign}]}]

      updated = RoutingData.apply_mutations(seeded, updates)

      assert updated.log_map == seeded.log_map
      assert updated.log_services == seeded.log_services
    end

    test "handles materializer_key set mutation - decoded refs stay strings" do
      updates = [
        {v(100), [{:set, SystemKeys.materializer_key(7, "wkr_a"), Values.encode_materializer_node("n1@host")}]}
      ]

      updated = RoutingData.apply_mutations(RoutingData.new_empty(), updates)

      assert updated.materializers == %{7 => %{"wkr_a" => "n1@host"}}
    end

    test "skips a materializer_key set whose value does not decode, keeping the last good entry" do
      routing_data = %{RoutingData.new_empty() | materializers: %{7 => %{"wkr_a" => "n1@host"}}}

      updates = [{v(100), [{:set, SystemKeys.materializer_key(7, "wkr_a"), <<0xEE, 0xEE>>}]}]

      assert RoutingData.apply_mutations(routing_data, updates).materializers == %{7 => %{"wkr_a" => "n1@host"}}
    end

    test "handles materializer_key clear mutation" do
      routing_data = %{RoutingData.new_empty() | materializers: %{7 => %{"wkr_a" => "n1@host"}}}

      updates = [{v(100), [{:clear, SystemKeys.materializer_key(7, "wkr_a")}]}]

      assert RoutingData.apply_mutations(routing_data, updates).materializers == %{}
    end

    test "clear_range over the materializers prefix drops covered entries only" do
      routing_data = %{
        RoutingData.new_empty()
        | materializers: %{
            0 => %{"wkr_sys" => "n1@host"},
            7 => %{"wkr_a" => "n1@host", "wkr_b" => "n3@host"},
            12 => %{"wkr_b" => "n2@host"}
          }
      }

      prefix = SystemKeys.materializers_prefix()
      updates = [{v(100), [{:clear_range, prefix, prefix <> <<0xFF>>}]}]

      assert RoutingData.apply_mutations(routing_data, updates).materializers == %{}
    end

    test "clear_range over an unrelated family leaves materializers untouched" do
      routing_data = %{RoutingData.new_empty() | materializers: %{7 => %{"wkr_a" => "n1@host"}}}

      prefix = SystemKeys.shard_keys_prefix()
      updates = [{v(100), [{:clear_range, prefix, prefix <> <<0xFF>>}]}]

      assert RoutingData.apply_mutations(routing_data, updates).materializers == %{7 => %{"wkr_a" => "n1@host"}}
    end

    test "handles shard_key clear mutation" do
      routing_data = RoutingData.insert_shard(RoutingData.new_empty(), "m", 42, "")

      updates = [{v(100), [{:clear, SystemKeys.shard_key("m")}]}]

      updated = RoutingData.apply_mutations(routing_data, updates)

      assert shard_list(updated) == []
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

    test "a clear_range over an unrouted family leaves the seeded wiring untouched" do
      seeded = seeded_wiring()
      prefix = "\xff/system/some_future_family/"

      updates = [{v(100), [{:clear_range, prefix <> "a", prefix <> "c"}]}]

      updated = RoutingData.apply_mutations(seeded, updates)

      assert updated.log_map == seeded.log_map
      assert updated.log_services == seeded.log_services
    end

    test "clear_range outside routing families leaves routing data unchanged" do
      routing_data = RoutingData.insert_shard(seeded_wiring(), "m", 1, "")

      updates = [{v(100), [{:clear_range, "user/a", "user/z"}]}]

      updated = RoutingData.apply_mutations(routing_data, updates)

      assert shard_list(updated) == [{"m", {1, ""}}]
      assert updated.log_map == seeded_wiring().log_map
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

    test "mixed mutations: shard_key entries apply, unrouted families do not" do
      foreign = "\xff/system/some_future_family/"

      updates = [
        {v(100),
         [
           {:set, SystemKeys.shard_key("m"), Values.encode_shard_key_entry(1, "")},
           {:set, foreign <> "log-1", "opaque"},
           {:set, SystemKeys.shard_key("z"), Values.encode_shard_key_entry(2, "m")},
           {:set, foreign <> "log-2", "opaque"}
         ]}
      ]

      updated = RoutingData.apply_mutations(RoutingData.new_empty(), updates)

      assert shard_list(updated) == [{"m", {1, ""}}, {"z", {2, "m"}}]
      assert updated.log_map == %{}
      assert updated.log_services == %{}
    end
  end
end
