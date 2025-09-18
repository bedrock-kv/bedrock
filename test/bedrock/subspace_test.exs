defmodule Bedrock.SubspaceTest do
  use ExUnit.Case, async: true

  alias Bedrock.Key
  alias Bedrock.Subspace

  describe "new/1" do
    test "creates subspace from binary prefix" do
      prefix = <<1, 2, 3>>
      assert %Subspace{prefix: ^prefix} = Subspace.new(prefix)
    end

    test "creates subspace from tuple" do
      tuple = {"users", 42}
      expected_prefix = Key.pack(tuple)
      assert %Subspace{prefix: ^expected_prefix} = Subspace.new(tuple)
    end

    test "creates subspace from empty binary" do
      assert %Subspace{prefix: <<>>} = Subspace.new(<<>>)
    end

    test "creates subspace from single element tuple" do
      expected_prefix = Key.pack({"test"})
      assert %Subspace{prefix: ^expected_prefix} = Subspace.new({"test"})
    end

    test "creates subspace from complex nested tuple" do
      tuple = {"app", "users", {"profile", 123}, [1, 2, 3]}
      expected_prefix = Key.pack(tuple)
      assert %Subspace{prefix: ^expected_prefix} = Subspace.new(tuple)
    end
  end

  describe "all/0" do
    test "creates subspace that contains all keys" do
      assert %Subspace{prefix: <<>>} = Subspace.all()
    end
  end

  describe "create/2" do
    test "creates new subspace by extending with tuple" do
      base = Subspace.new(<<1, 2>>)
      expected_prefix = <<1, 2>> <> Key.pack({"users", 42})
      assert %Subspace{prefix: ^expected_prefix} = Subspace.create(base, {"users", 42})
    end

    test "creates nested subspace from empty base" do
      base = Subspace.new(<<>>)
      expected_prefix = Key.pack({"test"})
      assert %Subspace{prefix: ^expected_prefix} = Subspace.create(base, {"test"})
    end

    test "handles complex tuple extension" do
      base = Subspace.new(<<"app">>)
      expected_prefix = <<"app">> <> Key.pack({"module", "component", 123})
      assert %Subspace{prefix: ^expected_prefix} = Subspace.create(base, {"module", "component", 123})
    end
  end

  describe "add/2" do
    test "adds single item to subspace" do
      base = Subspace.new(<<1, 2>>)
      expected_prefix = <<1, 2>> <> Key.pack({"users"})
      assert %Subspace{prefix: ^expected_prefix} = Subspace.add(base, "users")
    end

    test "adds integer to subspace" do
      base = Subspace.new(<<"prefix">>)
      expected_prefix = <<"prefix">> <> Key.pack({42})
      assert %Subspace{prefix: ^expected_prefix} = Subspace.add(base, 42)
    end

    test "adds tuple to subspace" do
      base = Subspace.new(<<>>)
      expected_prefix = Key.pack({{"complex", [1, 2, 3]}})
      assert %Subspace{prefix: ^expected_prefix} = Subspace.add(base, {"complex", [1, 2, 3]})
    end
  end

  describe "subspace/2" do
    test "creates nested subspace (alias for create/2)" do
      base = Subspace.new(<<"base">>)
      expected_prefix = <<"base">> <> Key.pack({"nested", "path"})
      assert %Subspace{prefix: ^expected_prefix} = Subspace.subspace(base, {"nested", "path"})
    end
  end

  describe "prefix/1" do
    test "prefix/1 returns raw prefix bytes" do
      prefix = <<1, 2, 3, 4>>
      assert Subspace.prefix(Subspace.new(prefix)) == prefix
    end

    test "prefix/1 works with string prefixes" do
      prefix = <<"test_prefix">>
      subspace = Subspace.new(prefix)
      assert Subspace.prefix(subspace) == prefix
    end
  end

  describe "pack/2" do
    test "packs list within subspace" do
      subspace = Subspace.new(<<"prefix">>)
      packed = Subspace.pack(subspace, ["user", 42, "profile"])

      expected = <<"prefix">> <> Key.pack(["user", 42, "profile"])
      assert packed == expected
    end

    test "packs tuple within subspace" do
      subspace = Subspace.new(<<1, 2>>)
      packed = Subspace.pack(subspace, {"user", 42})

      expected = <<1, 2>> <> Key.pack({"user", 42})
      assert packed == expected
    end

    test "packs empty list" do
      subspace = Subspace.new(<<"test">>)
      packed = Subspace.pack(subspace, [])

      expected = <<"test">> <> Key.pack([])
      assert packed == expected
    end

    test "packs with empty subspace prefix" do
      subspace = Subspace.new(<<>>)
      packed = Subspace.pack(subspace, ["test", 123])

      expected = Key.pack(["test", 123])
      assert packed == expected
    end

    test "packs complex nested data" do
      subspace = Subspace.new(<<"app">>)
      complex_data = {"users", {"profile", 123}, ["name", "email"]}
      packed = Subspace.pack(subspace, complex_data)

      expected = <<"app">> <> Key.pack(complex_data)
      assert packed == expected
    end
  end

  describe "unpack/2" do
    test "unpacks key that belongs to subspace" do
      subspace = Subspace.new(<<"prefix">>)
      original_data = ["user", 42, "profile"]
      packed_key = Subspace.pack(subspace, original_data)

      unpacked = Subspace.unpack(subspace, packed_key)
      assert unpacked == original_data
    end

    test "unpacks tuple data" do
      subspace = Subspace.new(<<1, 2, 3>>)
      original_tuple = {"test", 456, "atom"}
      packed_key = Subspace.pack(subspace, original_tuple)

      unpacked = Subspace.unpack(subspace, packed_key)
      assert unpacked == original_tuple
    end

    test "unpacks from empty prefix subspace" do
      subspace = Subspace.new(<<>>)
      original_data = ["test", "data"]
      packed_key = Subspace.pack(subspace, original_data)

      unpacked = Subspace.unpack(subspace, packed_key)
      assert unpacked == original_data
    end

    test "raises error when key doesn't belong to subspace" do
      subspace = Subspace.new(<<"prefix">>)
      foreign_key = <<"other_prefix", 1, 2, 3>>

      assert_raise ArgumentError, "Key does not belong to this subspace", fn ->
        Subspace.unpack(subspace, foreign_key)
      end
    end

    test "raises error when key is shorter than prefix" do
      subspace = Subspace.new(<<"long_prefix">>)
      short_key = <<"short">>

      assert_raise ArgumentError, "Key does not belong to this subspace", fn ->
        Subspace.unpack(subspace, short_key)
      end
    end

    test "handles exact prefix match (empty remaining data)" do
      subspace = Subspace.new(<<"prefix">>)
      # Create a key that is exactly the prefix plus empty packed data
      exact_key = <<"prefix">> <> Key.pack([])

      unpacked = Subspace.unpack(subspace, exact_key)
      assert unpacked == []
    end
  end

  describe "contains?/2" do
    test "returns true for keys that start with subspace prefix" do
      subspace = Subspace.new(<<"prefix">>)
      valid_key1 = <<"prefix", "extra", "data">>
      valid_key2 = <<"prefix">>

      assert Subspace.contains?(subspace, valid_key1)
      assert Subspace.contains?(subspace, valid_key2)
    end

    test "returns false for keys that don't start with prefix" do
      subspace = Subspace.new(<<"prefix">>)
      invalid_key1 = <<"other", "data">>
      # shorter than prefix
      invalid_key2 = <<"pref">>
      invalid_key3 = <<"different_prefix">>

      refute Subspace.contains?(subspace, invalid_key1)
      refute Subspace.contains?(subspace, invalid_key2)
      refute Subspace.contains?(subspace, invalid_key3)
    end

    test "works with empty prefix (contains everything)" do
      subspace = Subspace.new(<<>>)

      assert Subspace.contains?(subspace, <<"anything">>)
      assert Subspace.contains?(subspace, <<1, 2, 3, 4>>)
      assert Subspace.contains?(subspace, <<>>)
    end

    test "exact prefix match" do
      subspace = Subspace.new(<<"exact">>)

      assert Subspace.contains?(subspace, <<"exact">>)
      assert Subspace.contains?(subspace, <<"exact", "more">>)
      refute Subspace.contains?(subspace, <<"exac">>)
    end
  end

  describe "range/1" do
    test "returns correct range for non-empty prefix" do
      assert {<<"prefix", 0x00>>, <<"prefix", 0xFF>>} = Subspace.range(Subspace.new(<<"prefix">>))
    end

    test "returns special range for empty prefix" do
      assert {<<>>, <<0xFF>>} = Subspace.range(Subspace.new(<<>>))
    end

    test "works with binary prefixes" do
      assert {<<1, 2, 3, 0x00>>, <<1, 2, 3, 0xFF>>} = Subspace.range(Subspace.new(<<1, 2, 3>>))
    end

    test "all subspace returns full range" do
      assert {<<>>, <<0xFF>>} = Subspace.range(Subspace.all())
    end
  end

  describe "range/2" do
    test "returns range for tuple within subspace" do
      subspace = Subspace.new(<<"prefix">>)
      tuple = {"user", 42}
      tuple_packed = Key.pack(tuple)
      expected_start = <<"prefix">> <> tuple_packed <> <<0x00>>
      expected_end = <<"prefix">> <> tuple_packed <> <<0xFF>>
      assert {^expected_start, ^expected_end} = Subspace.range(subspace, tuple)
    end

    test "returns range for binary key within subspace" do
      subspace = Subspace.new(<<1, 2>>)
      key = <<"user_key">>
      expected_start = <<1, 2>> <> key <> <<0x00>>
      expected_end = <<1, 2>> <> key <> <<0xFF>>
      assert {^expected_start, ^expected_end} = Subspace.range(subspace, key)
    end

    test "works with empty subspace prefix" do
      assert {<<"test", 0x00>>, <<"test", 0xFF>>} = Subspace.range(Subspace.new(<<>>), <<"test">>)
    end
  end

  describe "to_string/1" do
    test "returns debug string representation" do
      subspace = Subspace.new(<<"test">>)
      str = Subspace.to_string(subspace)

      assert str =~ "Subspace<"
      assert str =~ inspect(<<"test">>)
    end

    test "works with String.Chars protocol" do
      subspace = Subspace.new(<<1, 2, 3>>)
      str = to_string(subspace)

      assert str =~ "Subspace<"
      assert str =~ inspect(<<1, 2, 3>>)
    end
  end

  describe "Inspect protocol" do
    test "provides readable inspect output" do
      subspace = Subspace.new(<<"prefix">>)
      inspected = inspect(subspace)

      assert inspected =~ "#Subspace<"
      assert inspected =~ inspect(<<"prefix">>)
    end
  end

  describe "integration scenarios" do
    test "hierarchical subspaces work correctly" do
      # Create a hierarchy: app -> users -> profiles
      app_subspace = Subspace.new(<<"app">>)
      users_subspace = Subspace.create(app_subspace, {"users"})
      profiles_subspace = Subspace.create(users_subspace, {"profiles"})

      # Pack some data in the deepest subspace
      profile_data = ["user_123", "settings", {"theme", "dark"}]
      packed_key = Subspace.pack(profiles_subspace, profile_data)

      # Verify containment hierarchy
      assert Subspace.contains?(app_subspace, packed_key)
      assert Subspace.contains?(users_subspace, packed_key)
      assert Subspace.contains?(profiles_subspace, packed_key)

      # Verify unpacking works
      unpacked = Subspace.unpack(profiles_subspace, packed_key)
      assert unpacked == profile_data
    end

    test "round-trip pack/unpack preserves data" do
      subspace = Subspace.new(<<"test_subspace">>)

      test_cases = [
        [],
        ["simple", "list"],
        {"tuple", "data", 42},
        ["mixed", {"nested", [1, 2, 3]}, "strings"],
        [1, 2.5, "string", {"nested", "tuple"}]
      ]

      for original_data <- test_cases do
        packed = Subspace.pack(subspace, original_data)
        unpacked = Subspace.unpack(subspace, packed)
        assert unpacked == original_data, "Round-trip failed for: #{inspect(original_data)}"
      end
    end

    test "range operations work for hierarchical data" do
      subspace = Subspace.new(<<"users">>)

      # Create keys for different users
      user1_key = Subspace.pack(subspace, ["user_001", "profile"])
      user2_key = Subspace.pack(subspace, ["user_002", "profile"])
      user100_key = Subspace.pack(subspace, ["user_100", "profile"])

      # Get range for all users
      {range_start, range_end} = Subspace.range(subspace)

      # All user keys should be within the range
      assert range_start <= user1_key and user1_key < range_end
      assert range_start <= user2_key and user2_key < range_end
      assert range_start <= user100_key and user100_key < range_end

      # Keys outside subspace should not be in range
      other_key = <<"other_app", "data">>
      refute range_start <= other_key and other_key < range_end
    end

    test "subspace isolation prevents cross-contamination" do
      users_subspace = Subspace.new(<<"users">>)
      posts_subspace = Subspace.new(<<"posts">>)

      user_key = Subspace.pack(users_subspace, ["user_123"])
      post_key = Subspace.pack(posts_subspace, ["post_456"])

      # Each subspace should only contain its own keys
      assert Subspace.contains?(users_subspace, user_key)
      refute Subspace.contains?(users_subspace, post_key)

      assert Subspace.contains?(posts_subspace, post_key)
      refute Subspace.contains?(posts_subspace, user_key)

      # Unpacking should fail for foreign keys
      assert_raise ArgumentError, fn ->
        Subspace.unpack(users_subspace, post_key)
      end

      assert_raise ArgumentError, fn ->
        Subspace.unpack(posts_subspace, user_key)
      end
    end
  end
end
