defmodule Bedrock.SystemKeysTest do
  use ExUnit.Case, async: true

  alias Bedrock.SystemKeys

  describe "key construction and parsing round-trip" do
    test "shard_key/1 round-trips through parse_key/1" do
      assert SystemKeys.parse_key(SystemKeys.shard_key("m")) == {:shard_key, "m"}
      assert SystemKeys.parse_key(SystemKeys.shard_key(<<0xFF, 0xFF>>)) == {:shard_key, <<0xFF, 0xFF>>}
    end

    test "layout_log/1 round-trips through parse_key/1" do
      assert SystemKeys.parse_key(SystemKeys.layout_log("log_1")) == {:layout_log, "log_1"}
    end

    test "prefixes cover exactly their families" do
      assert String.starts_with?(SystemKeys.shard_key("x"), SystemKeys.shard_keys_prefix())
      assert String.starts_with?(SystemKeys.layout_log("x"), SystemKeys.layout_logs_prefix())
      refute String.starts_with?(SystemKeys.layout_log("x"), SystemKeys.shard_keys_prefix())
    end

    test "unknown system keys parse as :unknown, non-system keys as :error" do
      assert SystemKeys.parse_key(<<0xFF, "/system/future/feature">>) == :unknown
      assert SystemKeys.parse_key("user/key") == :error
      assert SystemKeys.parse_key(:not_a_key) == :error
    end
  end
end
