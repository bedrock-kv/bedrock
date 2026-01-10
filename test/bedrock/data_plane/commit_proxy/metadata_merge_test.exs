defmodule Bedrock.DataPlane.CommitProxy.MetadataMergeTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.CommitProxy.MetadataMerge
  alias Bedrock.SystemKeys
  alias Bedrock.SystemKeys.OtpRef

  describe "merge/3 with shard_key mutations" do
    test "inserts shard_key into ETS shard_table" do
      table = :ets.new(:test_shards, [:ordered_set, :public])
      routing_data = {table, %{}, 3}

      key = SystemKeys.shard_key("m")
      value = :erlang.term_to_binary(42)
      updates = [{100, [{:set, key, value}]}]

      MetadataMerge.merge(%{}, updates, routing_data)

      assert :ets.lookup(table, "m") == [{"m", 42}]
    end

    test "inserts multiple shard_keys" do
      table = :ets.new(:test_shards, [:ordered_set, :public])
      routing_data = {table, %{}, 3}

      updates = [
        {100,
         [
           {:set, SystemKeys.shard_key("a"), :erlang.term_to_binary(1)},
           {:set, SystemKeys.shard_key("m"), :erlang.term_to_binary(2)},
           {:set, SystemKeys.shard_key("z"), :erlang.term_to_binary(3)}
         ]}
      ]

      MetadataMerge.merge(%{}, updates, routing_data)

      assert :ets.lookup(table, "a") == [{"a", 1}]
      assert :ets.lookup(table, "m") == [{"m", 2}]
      assert :ets.lookup(table, "z") == [{"z", 3}]
    end

    test "overwrites existing shard_key entry" do
      table = :ets.new(:test_shards, [:ordered_set, :public])
      :ets.insert(table, {"m", 99})
      routing_data = {table, %{}, 3}

      key = SystemKeys.shard_key("m")
      value = :erlang.term_to_binary(42)
      updates = [{100, [{:set, key, value}]}]

      MetadataMerge.merge(%{}, updates, routing_data)

      assert :ets.lookup(table, "m") == [{"m", 42}]
    end
  end

  describe "merge/3 with layout_log mutations" do
    test "accumulates log_services in metadata" do
      table = :ets.new(:test_shards, [:ordered_set, :public])
      routing_data = {table, %{}, 3}

      key = SystemKeys.layout_log("log-123")
      value = OtpRef.from_tuple({:my_log, :node@host})
      updates = [{100, [{:set, key, value}]}]

      result = MetadataMerge.merge(%{}, updates, routing_data)

      assert result.log_services["log-123"] == {:my_log, :node@host}
    end

    test "accumulates multiple logs" do
      table = :ets.new(:test_shards, [:ordered_set, :public])
      routing_data = {table, %{}, 3}

      updates = [
        {100,
         [
           {:set, SystemKeys.layout_log("log-1"), OtpRef.from_tuple({:log1, :n1@host})},
           {:set, SystemKeys.layout_log("log-2"), OtpRef.from_tuple({:log2, :n2@host})}
         ]}
      ]

      result = MetadataMerge.merge(%{}, updates, routing_data)

      assert result.log_services["log-1"] == {:log1, :n1@host}
      assert result.log_services["log-2"] == {:log2, :n2@host}
    end

    test "preserves existing metadata fields" do
      table = :ets.new(:test_shards, [:ordered_set, :public])
      routing_data = {table, %{}, 3}

      existing = %{other_field: "preserved"}
      key = SystemKeys.layout_log("log-123")
      value = OtpRef.from_tuple({:my_log, :node@host})
      updates = [{100, [{:set, key, value}]}]

      result = MetadataMerge.merge(existing, updates, routing_data)

      assert result.other_field == "preserved"
      assert result.log_services["log-123"] == {:my_log, :node@host}
    end
  end

  describe "merge/3 with clear mutations" do
    test "removes shard_key from ETS" do
      table = :ets.new(:test_shards, [:ordered_set, :public])
      :ets.insert(table, {"m", 42})
      routing_data = {table, %{}, 3}

      key = SystemKeys.shard_key("m")
      updates = [{100, [{:clear, key}]}]

      MetadataMerge.merge(%{}, updates, routing_data)

      assert :ets.lookup(table, "m") == []
    end

    test "removes layout_log from metadata" do
      table = :ets.new(:test_shards, [:ordered_set, :public])
      routing_data = {table, %{}, 3}

      existing = %{log_services: %{"log-123" => {:my_log, :node@host}}}
      key = SystemKeys.layout_log("log-123")
      updates = [{100, [{:clear, key}]}]

      result = MetadataMerge.merge(existing, updates, routing_data)

      assert result.log_services == %{}
    end

    test "clear on non-existent key is safe" do
      table = :ets.new(:test_shards, [:ordered_set, :public])
      routing_data = {table, %{}, 3}

      key = SystemKeys.shard_key("nonexistent")
      updates = [{100, [{:clear, key}]}]

      # Should not raise
      result = MetadataMerge.merge(%{}, updates, routing_data)
      assert result == %{}
    end
  end

  describe "merge/3 with multiple versions" do
    test "applies updates from multiple versions in order" do
      table = :ets.new(:test_shards, [:ordered_set, :public])
      routing_data = {table, %{}, 3}

      updates = [
        {100, [{:set, SystemKeys.shard_key("a"), :erlang.term_to_binary(1)}]},
        {101, [{:set, SystemKeys.shard_key("b"), :erlang.term_to_binary(2)}]},
        {102, [{:set, SystemKeys.shard_key("a"), :erlang.term_to_binary(99)}]}
      ]

      MetadataMerge.merge(%{}, updates, routing_data)

      # Later version (102) overwrites earlier (100)
      assert :ets.lookup(table, "a") == [{"a", 99}]
      assert :ets.lookup(table, "b") == [{"b", 2}]
    end
  end

  describe "merge/3 with unknown key types" do
    test "ignores unrecognized system keys" do
      table = :ets.new(:test_shards, [:ordered_set, :public])
      routing_data = {table, %{}, 3}

      # Unknown system key
      updates = [{100, [{:set, "\xff/system/unknown/foo", "bar"}]}]

      result = MetadataMerge.merge(%{}, updates, routing_data)

      assert result == %{}
      assert :ets.tab2list(table) == []
    end

    test "ignores non-system keys" do
      table = :ets.new(:test_shards, [:ordered_set, :public])
      routing_data = {table, %{}, 3}

      # Non-system key (shouldn't happen in practice, but handle gracefully)
      updates = [{100, [{:set, "user/data", "value"}]}]

      result = MetadataMerge.merge(%{}, updates, routing_data)

      assert result == %{}
    end

    test "ignores unsupported mutation types" do
      table = :ets.new(:test_shards, [:ordered_set, :public])
      routing_data = {table, %{}, 3}

      # clear_range not supported yet
      updates = [{100, [{:clear_range, "start", "end"}]}]

      result = MetadataMerge.merge(%{}, updates, routing_data)

      assert result == %{}
    end
  end
end
