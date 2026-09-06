defmodule Bedrock.SystemKeys.ValuesTest do
  use ExUnit.Case, async: true

  alias Bedrock.SystemKeys
  alias Bedrock.SystemKeys.Values

  describe "shard key entries" do
    test "round-trip" do
      assert {:ok, {7, "m"}} = Values.decode_shard_key_entry(Values.encode_shard_key_entry(7, "m"))
      assert {:ok, {0, ""}} = Values.decode_shard_key_entry(Values.encode_shard_key_entry(0, ""))
    end

    test "encoder rejects invalid input loudly" do
      assert_raise FunctionClauseError, fn -> Values.encode_shard_key_entry("not_a_tag", "m") end
    end

    test "decoder never raises on garbage or wrong shapes" do
      assert {:error, :invalid_encoding} = Values.decode_shard_key_entry(<<0xEE, 0xEE>>)
      assert {:error, :invalid_type} = Values.decode_shard_key_entry(Values.encode_materializer_node("n"))
      assert {:error, :invalid_encoding} = Values.decode_shard_key_entry(:not_binary)
    end
  end

  describe "materializer refs" do
    test "round-trip" do
      # The worker id lives in the KEY now; the value carries only what a
      # consumer needs to build the callable ref.
      assert {:ok, "node@host"} = Values.decode_materializer_node(Values.encode_materializer_node("node@host"))
    end

    test "encoder rejects non-binary input loudly - refs are encoded as strings, never atoms or pids" do
      assert_raise FunctionClauseError, fn -> Values.encode_materializer_node(:node@host) end
      assert_raise FunctionClauseError, fn -> Values.encode_materializer_node(self()) end
    end

    test "decoder never raises on garbage or wrong shapes, and never creates atoms" do
      assert {:error, :invalid_encoding} = Values.decode_materializer_node(<<0xEE, 0xEE>>)
      assert {:error, :invalid_type} = Values.decode_materializer_node(Values.encode_shard_key_entry(1, "m"))
      assert {:error, :invalid_encoding} = Values.decode_materializer_node(:not_binary)
    end
  end

  describe "log locations" do
    test "round-trip" do
      # The log id and its generation live in the KEY; the value carries
      # only the address, which is what exclusion safety judges against.
      assert {:ok, "node@host"} = Values.decode_log_node(Values.encode_log_node("node@host"))
    end

    test "encoder rejects non-binary input loudly - the node is a string, never an atom" do
      assert_raise FunctionClauseError, fn -> Values.encode_log_node(:node@host) end
    end

    test "decoder never raises on garbage or wrong shapes, and never creates atoms" do
      assert {:error, :invalid_encoding} = Values.decode_log_node(<<0xEE, 0xEE>>)
      assert {:error, :invalid_type} = Values.decode_log_node(Values.encode_shard_key_entry(1, "m"))
      assert {:error, :invalid_encoding} = Values.decode_log_node(:not_binary)
    end
  end

  describe "decode_for/2 - the writer/reader contract" do
    test "dispatches by parsed key family" do
      shard_value = Values.encode_shard_key_entry(3, "a")
      ref_value = Values.encode_materializer_node("node@host")
      log_value = Values.encode_log_node("log_node@host")

      assert {:ok, {3, "a"}} = Values.decode_for(SystemKeys.parse_key(SystemKeys.shard_key("m")), shard_value)

      assert {:ok, "node@host"} =
               Values.decode_for(SystemKeys.parse_key(SystemKeys.materializer_key(7, "wkr_abc")), ref_value)

      assert {:ok, "log_node@host"} =
               Values.decode_for(SystemKeys.parse_key(SystemKeys.log_key(:old, "log_abc")), log_value)
    end

    test "unknown families decode as errors, never raise" do
      assert {:error, :unknown_family} = Values.decode_for(:unknown, "anything")
      assert {:error, :unknown_family} = Values.decode_for(:error, "anything")
    end
  end

  describe "distributor lock UIDs" do
    test "round-trips a 16-byte token and dispatches through decode_for" do
      uid = :crypto.strong_rand_bytes(16)

      assert Values.encode_lock_uid(uid) == uid
      assert Values.decode_lock_uid(uid) == {:ok, uid}
      assert Values.decode_for({:distributor_lock, :owner}, uid) == {:ok, uid}
      assert Values.decode_for({:distributor_lock, :write}, uid) == {:ok, uid}
    end

    test "the encoder refuses non-16-byte input; the decoder never raises" do
      assert_raise ArgumentError, fn -> Values.encode_lock_uid("short") end
      assert_raise ArgumentError, fn -> Values.encode_lock_uid(:not_a_binary) end

      assert Values.decode_lock_uid("short") == {:error, :invalid_encoding}
      assert Values.decode_lock_uid(:crypto.strong_rand_bytes(17)) == {:error, :invalid_encoding}
    end
  end
end
