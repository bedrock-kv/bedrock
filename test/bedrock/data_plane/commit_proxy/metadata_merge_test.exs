defmodule Bedrock.DataPlane.CommitProxy.MetadataMergeTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.CommitProxy.MetadataMerge
  alias Bedrock.SystemKeys
  alias Bedrock.SystemKeys.OtpRef
  alias Bedrock.SystemKeys.ShardMetadata

  # Helper to create shard metadata binary using ShardMetadata FlatBuffer
  defp make_shard_binary(start_key, end_key, born_at) do
    ShardMetadata.new(start_key, end_key, born_at)
  end

  describe "merge/2 with shard mutations" do
    test "accumulates shard metadata" do
      # ShardMetadata uses FlatBuffer encoding
      value = make_shard_binary("", "m", 100)
      key = SystemKeys.shard("1")
      updates = [{100, [{:set, key, value}]}]

      result = MetadataMerge.merge(%{}, updates)

      # ShardMetadata.read returns a map with start_key, end_key, born_at, ended_at
      assert result.shards["1"].born_at == 100
    end

    test "removes shard on clear" do
      existing = %{shards: %{"1" => %{born_at: 100}}}
      key = SystemKeys.shard("1")
      updates = [{100, [{:clear, key}]}]

      result = MetadataMerge.merge(existing, updates)

      assert result.shards == %{}
    end
  end

  describe "merge/2 preserves existing metadata" do
    test "preserves other fields when adding shards" do
      existing = %{other_field: "preserved"}
      key = SystemKeys.shard("1")
      value = make_shard_binary("", "m", 100)
      updates = [{100, [{:set, key, value}]}]

      result = MetadataMerge.merge(existing, updates)

      assert result.other_field == "preserved"
      assert result.shards["1"].born_at == 100
    end
  end

  describe "merge/2 ignores routing keys" do
    test "ignores shard_key mutations (handled by RoutingData)" do
      key = SystemKeys.shard_key("m")
      value = :erlang.term_to_binary(42)
      updates = [{100, [{:set, key, value}]}]

      result = MetadataMerge.merge(%{}, updates)

      # shard_key is handled by RoutingData, not MetadataMerge
      assert result == %{}
    end

    test "ignores layout_log mutations (handled by RoutingData)" do
      key = SystemKeys.layout_log("log-123")
      value = OtpRef.from_tuple({:my_log, :node@host})
      updates = [{100, [{:set, key, value}]}]

      result = MetadataMerge.merge(%{}, updates)

      # layout_log is handled by RoutingData, not MetadataMerge
      assert result == %{}
    end
  end

  describe "merge/2 with unknown key types" do
    test "ignores unrecognized system keys" do
      updates = [{100, [{:set, "\xff/system/unknown/foo", "bar"}]}]

      result = MetadataMerge.merge(%{}, updates)

      assert result == %{}
    end

    test "ignores non-system keys" do
      updates = [{100, [{:set, "user/data", "value"}]}]

      result = MetadataMerge.merge(%{}, updates)

      assert result == %{}
    end

    test "ignores unsupported mutation types" do
      updates = [{100, [{:clear_range, "start", "end"}]}]

      result = MetadataMerge.merge(%{}, updates)

      assert result == %{}
    end
  end

  describe "merge/2 with multiple versions" do
    test "applies updates from multiple versions in order" do
      updates = [
        {100, [{:set, SystemKeys.shard("1"), make_shard_binary("", "a", 100)}]},
        {101, [{:set, SystemKeys.shard("2"), make_shard_binary("a", "m", 101)}]},
        {102, [{:set, SystemKeys.shard("1"), make_shard_binary("", "b", 102)}]}
      ]

      result = MetadataMerge.merge(%{}, updates)

      # Later version (102) overwrites earlier (100)
      assert result.shards["1"].born_at == 102
      assert result.shards["2"].born_at == 101
    end
  end
end
