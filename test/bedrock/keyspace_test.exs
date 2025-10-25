defmodule Bedrock.KeyspaceTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Bedrock.Keyspace

  describe "new/1" do
    test "creates keyspace from binary prefix" do
      prefix = <<1, 2, 3>>
      assert %Keyspace{prefix: ^prefix} = Keyspace.new(prefix)
    end

    test "creates keyspace from tuple" do
      tuple = {"users", 42}
      expected_prefix = Bedrock.Encoding.Tuple.pack(tuple)
      assert %Keyspace{prefix: ^expected_prefix} = Keyspace.new(expected_prefix)
    end

    test "creates keyspace from empty binary" do
      assert %Keyspace{prefix: <<>>} = Keyspace.new(<<>>)
    end

    test "creates keyspace from single element tuple" do
      expected_prefix = Bedrock.Encoding.Tuple.pack({"test"})
      assert %Keyspace{prefix: ^expected_prefix} = Keyspace.new(expected_prefix)
    end

    test "creates keyspace from complex nested tuple" do
      tuple = {"app", "users", {"profile", 123}, [1, 2, 3]}
      expected_prefix = Bedrock.Encoding.Tuple.pack(tuple)
      assert %Keyspace{prefix: ^expected_prefix} = Keyspace.new(expected_prefix)
    end
  end

  describe "all/0" do
    test "creates keyspace that contains all keys" do
      assert %Keyspace{prefix: <<>>} = Keyspace.all()
    end
  end

  describe "create/2" do
    test "creates new keyspace by extending with tuple" do
      base = Keyspace.new(<<1, 2>>, key_encoding: Bedrock.Encoding.Tuple)
      expected_prefix = <<1, 2>> <> Bedrock.Encoding.Tuple.pack({"users", 42})
      assert %Keyspace{prefix: ^expected_prefix} = Keyspace.partition(base, {"users", 42})
    end

    test "creates nested keyspace from empty base" do
      base = Keyspace.new(<<>>, key_encoding: Bedrock.Encoding.Tuple)
      expected_prefix = Bedrock.Encoding.Tuple.pack({"test"})
      assert %Keyspace{prefix: ^expected_prefix} = Keyspace.partition(base, {"test"})
    end

    test "handles complex tuple extension" do
      base = Keyspace.new(<<"app">>, key_encoding: Bedrock.Encoding.Tuple)
      expected_prefix = <<"app">> <> Bedrock.Encoding.Tuple.pack({"module", "component", 123})
      assert %Keyspace{prefix: ^expected_prefix} = Keyspace.partition(base, {"module", "component", 123})
    end
  end

  describe "add/2" do
    test "adds single item to keyspace" do
      base = Keyspace.new(<<1, 2>>)
      expected_prefix = <<1, 2>> <> Bedrock.Encoding.Tuple.pack({"users"})
      assert %Keyspace{prefix: ^expected_prefix} = Keyspace.add(base, "users")
    end

    test "adds integer to keyspace" do
      base = Keyspace.new(<<"prefix">>)
      expected_prefix = <<"prefix">> <> Bedrock.Encoding.Tuple.pack({42})
      assert %Keyspace{prefix: ^expected_prefix} = Keyspace.add(base, 42)
    end

    test "adds tuple to keyspace" do
      base = Keyspace.new(<<>>)
      expected_prefix = Bedrock.Encoding.Tuple.pack({{"complex", [1, 2, 3]}})
      assert %Keyspace{prefix: ^expected_prefix} = Keyspace.add(base, {"complex", [1, 2, 3]})
    end
  end

  describe "keyspace/2" do
    test "creates nested keyspace (alias for create/2)" do
      base = Keyspace.new(<<"base">>, key_encoding: Bedrock.Encoding.Tuple)
      expected_prefix = <<"base">> <> Bedrock.Encoding.Tuple.pack({"nested", "path"})
      assert %Keyspace{prefix: ^expected_prefix} = Keyspace.partition(base, {"nested", "path"})
    end
  end

  describe "prefix/1" do
    test "prefix/1 returns raw prefix bytes" do
      prefix = <<1, 2, 3, 4>>
      assert Keyspace.prefix(Keyspace.new(prefix)) == prefix
    end

    test "prefix/1 works with string prefixes" do
      prefix = <<"test_prefix">>
      keyspace = Keyspace.new(prefix)
      assert Keyspace.prefix(keyspace) == prefix
    end
  end

  describe "pack/2" do
    test "packs binary within keyspace" do
      keyspace = Keyspace.new(<<"prefix">>)
      data_binary = Bedrock.Encoding.Tuple.pack(["user", 42, "profile"])
      packed = Keyspace.pack(keyspace, data_binary)

      expected = <<"prefix">> <> data_binary
      assert packed == expected
    end

    test "packs binary tuple within keyspace" do
      keyspace = Keyspace.new(<<1, 2>>)
      data_binary = Bedrock.Encoding.Tuple.pack({"user", 42})
      packed = Keyspace.pack(keyspace, data_binary)

      expected = <<1, 2>> <> data_binary
      assert packed == expected
    end

    test "packs empty binary list" do
      keyspace = Keyspace.new(<<"test">>)
      data_binary = Bedrock.Encoding.Tuple.pack([])
      packed = Keyspace.pack(keyspace, data_binary)

      expected = <<"test">> <> data_binary
      assert packed == expected
    end

    test "packs with empty keyspace prefix" do
      keyspace = Keyspace.new(<<>>)
      data_binary = Bedrock.Encoding.Tuple.pack(["test", 123])
      packed = Keyspace.pack(keyspace, data_binary)

      expected = data_binary
      assert packed == expected
    end

    test "packs complex nested data" do
      keyspace = Keyspace.new(<<"app">>)
      complex_data = {"users", {"profile", 123}, ["name", "email"]}
      data_binary = Bedrock.Encoding.Tuple.pack(complex_data)
      packed = Keyspace.pack(keyspace, data_binary)

      expected = <<"app">> <> data_binary
      assert packed == expected
    end
  end

  describe "unpack/2" do
    test "unpacks key that belongs to keyspace" do
      keyspace = Keyspace.new(<<"prefix">>)
      original_data = ["user", 42, "profile"]
      data_binary = Bedrock.Encoding.Tuple.pack(original_data)
      packed_key = Keyspace.pack(keyspace, data_binary)

      unpacked = Keyspace.unpack(keyspace, packed_key)
      assert unpacked == data_binary
      # Verify the unpacked binary can be decoded back to original data
      assert Bedrock.Encoding.Tuple.unpack(unpacked) == original_data
    end

    test "unpacks tuple data" do
      keyspace = Keyspace.new(<<1, 2, 3>>)
      original_tuple = {"test", 456, "atom"}
      data_binary = Bedrock.Encoding.Tuple.pack(original_tuple)
      packed_key = Keyspace.pack(keyspace, data_binary)

      unpacked = Keyspace.unpack(keyspace, packed_key)
      assert unpacked == data_binary
      # Verify the unpacked binary can be decoded back to original tuple
      assert Bedrock.Encoding.Tuple.unpack(unpacked) == original_tuple
    end

    test "unpacks from empty prefix keyspace" do
      keyspace = Keyspace.new(<<>>)
      original_data = ["test", "data"]
      data_binary = Bedrock.Encoding.Tuple.pack(original_data)
      packed_key = Keyspace.pack(keyspace, data_binary)

      unpacked = Keyspace.unpack(keyspace, packed_key)
      assert unpacked == data_binary
      # Verify the unpacked binary can be decoded back to original data
      assert Bedrock.Encoding.Tuple.unpack(unpacked) == original_data
    end

    test "raises error when key doesn't belong to keyspace" do
      keyspace = Keyspace.new(<<"prefix">>)
      foreign_key = <<"other_prefix", 1, 2, 3>>

      assert_raise ArgumentError, "Key does not belong to this keyspace", fn ->
        Keyspace.unpack(keyspace, foreign_key)
      end
    end

    test "raises error when key is shorter than prefix" do
      keyspace = Keyspace.new(<<"long_prefix">>)
      short_key = <<"short">>

      assert_raise ArgumentError, "Key does not belong to this keyspace", fn ->
        Keyspace.unpack(keyspace, short_key)
      end
    end

    test "handles exact prefix match (empty remaining data)" do
      keyspace = Keyspace.new(<<"prefix">>)
      # Create a key that is exactly the prefix plus empty packed data
      empty_data_binary = Bedrock.Encoding.Tuple.pack([])
      exact_key = <<"prefix">> <> empty_data_binary

      unpacked = Keyspace.unpack(keyspace, exact_key)
      assert unpacked == empty_data_binary
      # Verify the unpacked binary can be decoded back to empty list
      assert Bedrock.Encoding.Tuple.unpack(unpacked) == []
    end
  end

  describe "contains?/2" do
    test "returns true for keys that start with keyspace prefix" do
      keyspace = Keyspace.new(<<"prefix">>)
      valid_key1 = <<"prefix", "extra", "data">>
      valid_key2 = <<"prefix">>

      assert Keyspace.contains?(keyspace, valid_key1)
      assert Keyspace.contains?(keyspace, valid_key2)
    end

    test "returns false for keys that don't start with prefix" do
      keyspace = Keyspace.new(<<"prefix">>)
      invalid_key1 = <<"other", "data">>
      # shorter than prefix
      invalid_key2 = <<"pref">>
      invalid_key3 = <<"different_prefix">>

      refute Keyspace.contains?(keyspace, invalid_key1)
      refute Keyspace.contains?(keyspace, invalid_key2)
      refute Keyspace.contains?(keyspace, invalid_key3)
    end

    test "works with empty prefix (contains everything)" do
      keyspace = Keyspace.new(<<>>)

      assert Keyspace.contains?(keyspace, <<"anything">>)
      assert Keyspace.contains?(keyspace, <<1, 2, 3, 4>>)
      assert Keyspace.contains?(keyspace, <<>>)
    end

    test "exact prefix match" do
      keyspace = Keyspace.new(<<"exact">>)

      assert Keyspace.contains?(keyspace, <<"exact">>)
      assert Keyspace.contains?(keyspace, <<"exact", "more">>)
      refute Keyspace.contains?(keyspace, <<"exac">>)
    end
  end

  describe "range/1" do
    test "returns correct range for non-empty prefix" do
      assert {"prefix", "prefiy"} = Bedrock.ToKeyRange.to_key_range(Keyspace.new(<<"prefix">>))
    end

    test "returns special range for empty prefix" do
      assert {<<>>, <<0xFF>>} = Bedrock.ToKeyRange.to_key_range(Keyspace.new(<<>>))
    end

    test "works with binary prefixes" do
      assert {<<1, 2, 3>>, <<1, 2, 4>>} = Bedrock.ToKeyRange.to_key_range(Keyspace.new(<<1, 2, 3>>))
    end

    test "all keyspace returns full range" do
      assert {<<>>, <<0xFF>>} = Bedrock.ToKeyRange.to_key_range(Keyspace.all())
    end
  end

  describe "range/2" do
    test "returns range for tuple within keyspace" do
      keyspace = Keyspace.new(<<"prefix">>, key_encoding: Bedrock.Encoding.Tuple)
      tuple = {"user", 42}
      tuple_packed = Bedrock.Encoding.Tuple.pack(tuple)
      expected_start = <<"prefix">> <> tuple_packed
      expected_end = Bedrock.Key.strinc(expected_start)

      assert {^expected_start, ^expected_end} =
               keyspace |> Keyspace.partition(tuple) |> Bedrock.ToKeyRange.to_key_range()
    end

    test "returns range for binary key within keyspace" do
      keyspace = Keyspace.new(<<1, 2>>)
      key = <<"user_key">>
      expected_start = <<1, 2>> <> key
      expected_end = Bedrock.Key.strinc(expected_start)
      assert {^expected_start, ^expected_end} = keyspace |> Keyspace.partition(key) |> Bedrock.ToKeyRange.to_key_range()
    end

    test "works with empty keyspace prefix" do
      assert {"test", "tesu"} =
               <<>> |> Keyspace.new() |> Keyspace.partition(<<"test">>) |> Bedrock.ToKeyRange.to_key_range()
    end
  end

  describe "to_string/1" do
    test "returns debug string representation" do
      keyspace = Keyspace.new(<<"test">>)
      str = Keyspace.to_string(keyspace)

      assert str =~ "Keyspace<"
      assert str =~ inspect(<<"test">>)
    end

    test "works with String.Chars protocol" do
      keyspace = Keyspace.new(<<1, 2, 3>>)
      str = to_string(keyspace)

      assert str =~ "Keyspace<"
      assert str =~ inspect(<<1, 2, 3>>)
    end
  end

  describe "Inspect protocol" do
    test "provides readable inspect output" do
      keyspace = Keyspace.new(<<"prefix">>)
      inspected = inspect(keyspace)

      assert inspected =~ "#Keyspace<"
      # hex representation of "prefix"
      assert inspected =~ "0x707265666978"
    end
  end

  describe "integration scenarios" do
    test "hierarchical keyspaces work correctly" do
      # Create a hierarchy: app -> users -> profiles
      app_keyspace = Keyspace.new(<<"app">>, key_encoding: Bedrock.Encoding.Tuple)
      users_keyspace = Keyspace.partition(app_keyspace, {"users"})
      profiles_keyspace = Keyspace.partition(users_keyspace, {"profiles"})

      # Pack some data in the deepest keyspace
      profile_data = ["user_123", "settings", {"theme", "dark"}]
      # Use Bedrock.Encoding.Tuple directly since profiles_keyspace doesn't have encoding
      packed_key = Keyspace.prefix(profiles_keyspace) <> Bedrock.Encoding.Tuple.pack(profile_data)

      # Verify containment hierarchy
      assert Keyspace.contains?(app_keyspace, packed_key)
      assert Keyspace.contains?(users_keyspace, packed_key)
      assert Keyspace.contains?(profiles_keyspace, packed_key)

      # Verify unpacking works by manually unpacking
      prefix = Keyspace.prefix(profiles_keyspace)
      <<^prefix::binary, suffix::binary>> = packed_key
      unpacked = Bedrock.Encoding.Tuple.unpack(suffix)
      assert unpacked == profile_data
    end

    test "round-trip pack/unpack preserves data" do
      keyspace = Keyspace.new(<<"test_keyspace">>)

      test_cases = [
        [],
        ["simple", "list"],
        {"tuple", "data", 42},
        ["mixed", {"nested", [1, 2, 3]}, "strings"],
        [1, 2.5, "string", {"nested", "tuple"}]
      ]

      for original_data <- test_cases do
        data_binary = Bedrock.Encoding.Tuple.pack(original_data)
        packed = Keyspace.pack(keyspace, data_binary)
        unpacked = Keyspace.unpack(keyspace, packed)
        assert unpacked == data_binary
        # Verify the unpacked binary can be decoded back to original data
        roundtrip_data = Bedrock.Encoding.Tuple.unpack(unpacked)
        assert roundtrip_data == original_data, "Round-trip failed for: #{inspect(original_data)}"
      end
    end

    test "range operations work for hierarchical data" do
      keyspace = Keyspace.new(<<"users">>)

      # Create keys for different users
      user1_data = Bedrock.Encoding.Tuple.pack(["user_001", "profile"])
      user2_data = Bedrock.Encoding.Tuple.pack(["user_002", "profile"])
      user100_data = Bedrock.Encoding.Tuple.pack(["user_100", "profile"])
      user1_key = Keyspace.pack(keyspace, user1_data)
      user2_key = Keyspace.pack(keyspace, user2_data)
      user100_key = Keyspace.pack(keyspace, user100_data)

      # Get range for all users
      {range_start, range_end} = Bedrock.ToKeyRange.to_key_range(keyspace)

      # All user keys should be within the range
      assert range_start <= user1_key and user1_key < range_end
      assert range_start <= user2_key and user2_key < range_end
      assert range_start <= user100_key and user100_key < range_end

      # Keys outside keyspace should not be in range
      other_key = <<"other_app", "data">>
      refute range_start <= other_key and other_key < range_end
    end

    test "keyspace isolation prevents cross-contamination" do
      users_keyspace = Keyspace.new(<<"users">>)
      posts_keyspace = Keyspace.new(<<"posts">>)

      user_data = Bedrock.Encoding.Tuple.pack(["user_123"])
      post_data = Bedrock.Encoding.Tuple.pack(["post_456"])
      user_key = Keyspace.pack(users_keyspace, user_data)
      post_key = Keyspace.pack(posts_keyspace, post_data)

      # Each keyspace should only contain its own keys
      assert Keyspace.contains?(users_keyspace, user_key)
      refute Keyspace.contains?(users_keyspace, post_key)

      assert Keyspace.contains?(posts_keyspace, post_key)
      refute Keyspace.contains?(posts_keyspace, user_key)

      # Unpacking should fail for foreign keys
      assert_raise ArgumentError, fn ->
        Keyspace.unpack(users_keyspace, post_key)
      end

      assert_raise ArgumentError, fn ->
        Keyspace.unpack(posts_keyspace, user_key)
      end
    end
  end

  describe "property-based tests" do
    property "pack and unpack are inverses for any binary suffix" do
      check all(
              prefix <- binary(min_length: 0, max_length: 20),
              suffix <- binary(min_length: 0, max_length: 50)
            ) do
        keyspace = Keyspace.new(prefix)
        packed = Keyspace.pack(keyspace, suffix)
        unpacked = Keyspace.unpack(keyspace, packed)
        assert unpacked == suffix
      end
    end

    property "contains? returns true for all packed keys" do
      check all(
              prefix <- binary(min_length: 1, max_length: 20),
              suffix <- binary(min_length: 0, max_length: 50)
            ) do
        keyspace = Keyspace.new(prefix)
        packed = Keyspace.pack(keyspace, suffix)
        assert Keyspace.contains?(keyspace, packed)
      end
    end

    property "packed keys maintain prefix ordering" do
      check all(
              prefix <- binary(min_length: 1, max_length: 10),
              suffix1 <- binary(max_length: 20),
              suffix2 <- binary(max_length: 20)
            ) do
        keyspace = Keyspace.new(prefix)
        packed1 = Keyspace.pack(keyspace, suffix1)
        packed2 = Keyspace.pack(keyspace, suffix2)

        # Both should start with prefix
        assert Keyspace.contains?(keyspace, packed1)
        assert Keyspace.contains?(keyspace, packed2)

        # Ordering should be preserved
        cond do
          suffix1 < suffix2 -> assert packed1 < packed2
          suffix1 > suffix2 -> assert packed1 > packed2
          true -> assert packed1 == packed2
        end
      end
    end

    property "partition creates valid nested keyspaces" do
      check all(
              base_prefix <- binary(min_length: 0, max_length: 10),
              partition_name <- binary(min_length: 1, max_length: 10)
            ) do
        base = Keyspace.new(base_prefix)
        nested = Keyspace.partition(base, partition_name)

        # Nested prefix should start with base prefix
        assert String.starts_with?(Keyspace.prefix(nested), Keyspace.prefix(base))

        # Any key in nested should also be in base
        test_suffix = "test_data"
        nested_key = Keyspace.pack(nested, test_suffix)

        if base_prefix != <<>> do
          assert Keyspace.contains?(base, nested_key)
        end
      end
    end

    property "add creates deterministic keyspaces" do
      check all(
              prefix <- binary(min_length: 0, max_length: 10),
              item <- one_of([binary(), integer(), constant(nil)])
            ) do
        base = Keyspace.new(prefix)
        ks1 = Keyspace.add(base, item)
        ks2 = Keyspace.add(base, item)

        # Adding the same item twice should produce identical keyspaces
        assert Keyspace.prefix(ks1) == Keyspace.prefix(ks2)
      end
    end

    property "prefix returns the exact prefix provided" do
      check all(prefix <- binary(min_length: 0, max_length: 50)) do
        keyspace = Keyspace.new(prefix)
        assert Keyspace.prefix(keyspace) == prefix
      end
    end
  end

  describe "edge cases with encoding" do
    test "handles keyspace with tuple encoding for complex keys" do
      keyspace = Keyspace.new(<<"users">>, key_encoding: Bedrock.Encoding.Tuple)

      # Pack a complex tuple key
      user_id = {123, "active", [1, 2, 3]}
      packed = Keyspace.pack(keyspace, user_id)

      # Should contain the packed key
      assert Keyspace.contains?(keyspace, packed)

      # Unpack should return the original tuple
      unpacked = Keyspace.unpack(keyspace, packed)
      assert unpacked == user_id
    end

    test "handles keyspace with value encoding" do
      keyspace =
        Keyspace.new(<<"data">>,
          key_encoding: Bedrock.Encoding.Tuple,
          value_encoding: Bedrock.Encoding.Tuple
        )

      # Verify encodings are set
      assert keyspace.key_encoding == Bedrock.Encoding.Tuple
      assert keyspace.value_encoding == Bedrock.Encoding.Tuple
    end

    test "partition preserves encoding options" do
      base =
        Keyspace.new(<<"base">>,
          key_encoding: Bedrock.Encoding.Tuple,
          value_encoding: Bedrock.Encoding.Tuple
        )

      # Partition without options should preserve base encoding
      nested = Keyspace.partition(base, {"section"})

      # The nested keyspace inherits the encoding
      assert nested.key_encoding == Bedrock.Encoding.Tuple
      assert nested.value_encoding == Bedrock.Encoding.Tuple
    end

    test "partition can override encoding options" do
      base = Keyspace.new(<<"base">>, key_encoding: Bedrock.Encoding.Tuple)

      nested = Keyspace.partition(base, {"section"}, value_encoding: Bedrock.Encoding.Tuple)

      assert nested.key_encoding == Bedrock.Encoding.Tuple
      assert nested.value_encoding == Bedrock.Encoding.Tuple
    end

    test "raises when packing non-binary key without encoding" do
      keyspace = Keyspace.new(<<"test">>)

      assert_raise UndefinedFunctionError, fn ->
        Keyspace.pack(keyspace, {1, 2, 3})
      end
    end

    test "raises when partitioning with non-binary name and no key encoding" do
      keyspace = Keyspace.new(<<"test">>)

      assert_raise ArgumentError, ~r/Keyspace does not support key encoding/, fn ->
        Keyspace.partition(keyspace, {1, 2, 3})
      end
    end

    test "partition can be called on non-keyspace that implements ToKeyspace" do
      # Binary implements ToKeyspace protocol
      result = Keyspace.partition(<<"prefix">>, <<"suffix">>)

      assert result.prefix == <<"prefix", "suffix">>
    end

    test "raises when unpacking key from wrong keyspace" do
      ks1 = Keyspace.new(<<"keyspace1">>)
      ks2 = Keyspace.new(<<"keyspace2">>)

      key_in_ks1 = Keyspace.pack(ks1, <<"data">>)

      assert_raise ArgumentError, "Key does not belong to this keyspace", fn ->
        Keyspace.unpack(ks2, key_in_ks1)
      end
    end
  end

  describe "ToKeyRange protocol" do
    test "converts keyspace to key range correctly" do
      keyspace = Keyspace.new(<<"test">>)
      {start_key, end_key} = Bedrock.ToKeyRange.to_key_range(keyspace)

      assert start_key == <<"test">>
      assert end_key == <<"tesu">>
    end

    test "handles binary prefixes with 0xFF correctly" do
      # A prefix ending in 0xFF should increment to next valid byte
      keyspace = Keyspace.new(<<1, 2, 0xFF>>)
      {start_key, end_key} = Bedrock.ToKeyRange.to_key_range(keyspace)

      assert start_key == <<1, 2, 0xFF>>
      assert end_key == <<1, 3>>
    end
  end

  describe "Inspect and String protocols" do
    test "inspect shows hex representation of prefix" do
      keyspace = Keyspace.new(<<0x12, 0x34, 0x56>>)
      inspected = inspect(keyspace)

      assert inspected =~ "#Keyspace<"
      assert inspected =~ "0x123456"
    end

    test "inspect shows encoding information when present" do
      keyspace =
        Keyspace.new(<<"test">>,
          key_encoding: Bedrock.Encoding.Tuple,
          value_encoding: Bedrock.Encoding.Tuple
        )

      inspected = inspect(keyspace)
      assert inspected =~ "keys:Tuple"
      assert inspected =~ "values:Tuple"
    end

    test "to_string creates readable representation" do
      keyspace = Keyspace.new(<<"my_prefix">>)
      str = to_string(keyspace)

      assert str =~ "Keyspace<"
      assert str =~ inspect(<<"my_prefix">>)
    end
  end
end
