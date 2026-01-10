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

  describe "merge/2 with layout_resolver mutations" do
    test "accumulates resolvers in metadata" do
      key = SystemKeys.layout_resolver("m")
      value = OtpRef.from_tuple({:resolver_1, :node@host})
      updates = [{100, [{:set, key, value}]}]

      result = MetadataMerge.merge(%{}, updates)

      assert result.resolvers["m"] == {:resolver_1, :node@host}
    end

    test "accumulates multiple resolvers" do
      updates = [
        {100,
         [
           {:set, SystemKeys.layout_resolver("a"), OtpRef.from_tuple({:r1, :n1@host})},
           {:set, SystemKeys.layout_resolver("m"), OtpRef.from_tuple({:r2, :n2@host})}
         ]}
      ]

      result = MetadataMerge.merge(%{}, updates)

      assert result.resolvers["a"] == {:r1, :n1@host}
      assert result.resolvers["m"] == {:r2, :n2@host}
    end

    test "removes resolver on clear" do
      existing = %{resolvers: %{"m" => {:resolver_1, :node@host}}}
      key = SystemKeys.layout_resolver("m")
      updates = [{100, [{:clear, key}]}]

      result = MetadataMerge.merge(existing, updates)

      assert result.resolvers == %{}
    end
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
    test "preserves other fields when adding resolvers" do
      existing = %{other_field: "preserved"}
      key = SystemKeys.layout_resolver("m")
      value = OtpRef.from_tuple({:resolver_1, :node@host})
      updates = [{100, [{:set, key, value}]}]

      result = MetadataMerge.merge(existing, updates)

      assert result.other_field == "preserved"
      assert result.resolvers["m"] == {:resolver_1, :node@host}
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
        {100, [{:set, SystemKeys.layout_resolver("a"), OtpRef.from_tuple({:r1, :n1@host})}]},
        {101, [{:set, SystemKeys.layout_resolver("b"), OtpRef.from_tuple({:r2, :n2@host})}]},
        {102, [{:set, SystemKeys.layout_resolver("a"), OtpRef.from_tuple({:r3, :n3@host})}]}
      ]

      result = MetadataMerge.merge(%{}, updates)

      # Later version (102) overwrites earlier (100)
      assert result.resolvers["a"] == {:r3, :n3@host}
      assert result.resolvers["b"] == {:r2, :n2@host}
    end
  end
end
