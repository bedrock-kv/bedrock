defmodule Bedrock.SystemKeysTest do
  use ExUnit.Case, async: true

  alias Bedrock.KeyRange
  alias Bedrock.SystemKeys

  describe "key construction and parsing round-trip" do
    test "shard_key/1 round-trips through parse_key/1" do
      assert SystemKeys.parse_key(SystemKeys.shard_key("m")) == {:shard_key, "m"}
      assert SystemKeys.parse_key(SystemKeys.shard_key(<<0xFF, 0xFF>>)) == {:shard_key, <<0xFF, 0xFF>>}
    end

    test "materializer_key/2 round-trips through parse_key/1, carrying tag AND worker" do
      assert SystemKeys.parse_key(SystemKeys.materializer_key(0, "wkr_sys")) == {:materializer_key, 0, "wkr_sys"}
      assert SystemKeys.parse_key(SystemKeys.materializer_key(42, "abc12def")) == {:materializer_key, 42, "abc12def"}
    end

    test "a tag's members share a prefix that excludes neighbouring tags" do
      prefix = SystemKeys.materializer_tag_prefix(7)

      assert String.starts_with?(SystemKeys.materializer_key(7, "wkr_a"), prefix)
      assert String.starts_with?(SystemKeys.materializer_key(7, "wkr_b"), prefix)
      refute String.starts_with?(SystemKeys.materializer_key(70, "wkr_c"), prefix)
      refute String.starts_with?(SystemKeys.materializer_key(1, "wkr_d"), prefix)
    end

    test "malformed materializer keys parse as :unknown" do
      assert SystemKeys.parse_key(SystemKeys.materializers_prefix() <> "not_a_tag/wkr") == :unknown
      assert SystemKeys.parse_key(SystemKeys.materializers_prefix() <> "12x/wkr") == :unknown
      assert SystemKeys.parse_key(SystemKeys.materializers_prefix()) == :unknown

      # A tag with a trailing slash and no worker is the prefix, not an
      # entry; a tag with no slash at all names no member either.
      assert SystemKeys.parse_key(SystemKeys.materializers_prefix() <> "7/") == :unknown
      assert SystemKeys.parse_key(SystemKeys.materializers_prefix() <> "7") == :unknown
    end

    test "prefixes cover exactly their families" do
      assert String.starts_with?(SystemKeys.shard_key("x"), SystemKeys.shard_keys_prefix())
      assert String.starts_with?(SystemKeys.materializer_key(3, "wkr_a"), SystemKeys.materializers_prefix())
      refute String.starts_with?(SystemKeys.materializer_key(3, "wkr_a"), SystemKeys.shard_keys_prefix())
    end

    test "each family's prefix RANGE holds its own keys and no other family's" do
      # A prefix is only safe to scan or clear if [prefix, strinc(prefix))
      # is strictly its own family. An over-broad definition (a
      # materializers_prefix of "materializers" without the slash, say)
      # would swallow a sibling family that shares prefix bytes. Pinned
      # on the RANGE, not on starts_with?, because the range is what
      # readers and any future clear actually use.
      families = %{
        shard_key: {SystemKeys.shard_keys_prefix(), [SystemKeys.shard_key(<<>>), SystemKeys.shard_key(<<0xFF, 0xFF>>)]},
        materializer_key:
          {SystemKeys.materializers_prefix(),
           [SystemKeys.materializer_key(0, "wkr_a"), SystemKeys.materializer_key(4200, "wkr_b")]},
        config:
          {SystemKeys.config_prefix(),
           [SystemKeys.config_key(SystemKeys.desired_commit_proxies()), SystemKeys.config_key("z")]}
      }

      # Plausible future siblings that share leading bytes with a family.
      siblings = ["\xff/system/shards/0", "\xff/system/materializer_policy/0", "\xff/system/layout/id"]

      for {family, {prefix, keys}} <- families do
        range = KeyRange.from_prefix(prefix)

        for key <- keys do
          assert KeyRange.contains?(range, key), "#{family} range must hold its own key #{inspect(key)}"
        end

        for {other_family, {_p, other_keys}} <- families,
            other_family != family,
            other_key <- other_keys do
          refute KeyRange.contains?(range, other_key),
                 "#{family} range must not cover #{other_family} key #{inspect(other_key)}"
        end

        for sibling <- siblings do
          refute KeyRange.contains?(range, sibling),
                 "#{family} range must not cover sibling #{inspect(sibling)}"
        end
      end
    end

    test "unknown system keys parse as :unknown, non-system keys as :error" do
      assert SystemKeys.parse_key(<<0xFF, "/system/future/feature">>) == :unknown
      assert SystemKeys.parse_key("user/key") == :error
      assert SystemKeys.parse_key(:not_a_key) == :error
    end
  end

  describe "cluster configuration keys" do
    test "config_key/1 round-trips through parse_key/1, carrying the parameter name" do
      assert SystemKeys.config_key(SystemKeys.desired_commit_proxies()) ==
               "\xff/system/config/desired_commit_proxies"

      assert SystemKeys.parse_key(SystemKeys.config_key("desired_commit_proxies")) ==
               {:config, "desired_commit_proxies"}
    end

    test "the bare prefix names no parameter" do
      assert SystemKeys.parse_key(SystemKeys.config_prefix()) == :unknown
    end
  end

  describe "distributor lock keys" do
    test "construct under the system prefix and parse back" do
      assert SystemKeys.distributor_lock_owner() == "\xff/system/distributor_lock/owner"
      assert SystemKeys.distributor_lock_write() == "\xff/system/distributor_lock/write"

      assert SystemKeys.parse_key(SystemKeys.distributor_lock_owner()) == {:distributor_lock, :owner}
      assert SystemKeys.parse_key(SystemKeys.distributor_lock_write()) == {:distributor_lock, :write}
    end

    test "near-miss keys under the family prefix are unknown, not lock keys" do
      assert SystemKeys.parse_key("\xff/system/distributor_lock/other") == :unknown
      assert SystemKeys.parse_key("\xff/system/distributor_lock/owner2") == :unknown
    end
  end
end
