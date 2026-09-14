defmodule Bedrock.ObjectStorage.KeysTest do
  use ExUnit.Case, async: true

  alias Bedrock.ObjectStorage.Keys

  @max_version 0xFFFFFFFFFFFFFFFF

  describe "invert_version/1" do
    test "inverts zero to max" do
      assert Keys.invert_version(0) == @max_version
    end

    test "inverts max to zero" do
      assert Keys.invert_version(@max_version) == 0
    end

    test "inversion is symmetric" do
      for version <- [0, 1, 1000, 999_999, @max_version] do
        assert Keys.restore_version(Keys.invert_version(version)) == version
      end
    end
  end

  describe "format_inverted_version/1" do
    test "formats zero as 13-character padded base36 string" do
      assert Keys.format_inverted_version(0) == "0000000000000"
    end

    test "formats small number with padding" do
      # 1000 in base36 is "rs"
      assert Keys.format_inverted_version(1000) == "00000000000rs"
    end

    test "formats max version" do
      # max uint64 in base36 is "3w5e11264sgsf"
      assert Keys.format_inverted_version(@max_version) == "3w5e11264sgsf"
    end
  end

  describe "parse_inverted_version/1" do
    test "parses valid padded string" do
      assert Keys.parse_inverted_version("00000000000rs") == {:ok, 1000}
    end

    test "parses zero" do
      assert Keys.parse_inverted_version("0000000000000") == {:ok, 0}
    end

    test "parses max version" do
      assert Keys.parse_inverted_version("3w5e11264sgsf") == {:ok, @max_version}
    end

    test "returns error for invalid format" do
      # Use characters outside base36 (0-9, a-z)
      assert Keys.parse_inverted_version("invalid!") == {:error, :invalid_format}
      assert Keys.parse_inverted_version("123@abc") == {:error, :invalid_format}
      assert Keys.parse_inverted_version("-1") == {:error, :invalid_format}
    end

    test "rejects non-canonical renderings rather than inventing a version" do
      # base36 will read a number out of nearly anything; only the exact
      # 13-character lowercase padding this module writes is a version.
      assert Keys.parse_inverted_version("rs") == {:error, :invalid_format}
      assert Keys.parse_inverted_version("0000000000rs") == {:error, :invalid_format}
      assert Keys.parse_inverted_version("000000000000rs") == {:error, :invalid_format}
      assert Keys.parse_inverted_version("3W5E11264SGSF") == {:error, :invalid_format}
      assert Keys.parse_inverted_version("+000000000rs") == {:error, :invalid_format}
    end

    test "rejects a 13-character value past uint64" do
      # "zzzzzzzzzzzzz" is 13 base36 digits but 36^13-1, well past the
      # inverted uint64 `restore_version/1` accepts.
      assert Keys.parse_inverted_version("zzzzzzzzzzzzz") == {:error, :invalid_format}
    end
  end

  describe "version_to_key/1 and key_to_version/1" do
    test "round-trips correctly" do
      for version <- [0, 1, 1000, 999_999, @max_version] do
        key = Keys.version_to_key(version)
        assert {:ok, ^version} = Keys.key_to_version(key)
      end
    end

    test "produces keys that sort newest first" do
      versions = [100, 200, 300]
      keys = Enum.map(versions, &Keys.version_to_key/1)
      sorted_keys = Enum.sort(keys)

      # Sorted keys should correspond to versions in descending order
      sorted_versions =
        Enum.map(sorted_keys, fn key ->
          {:ok, v} = Keys.key_to_version(key)
          v
        end)

      assert sorted_versions == [300, 200, 100]
    end
  end

  describe "shard_tag/1" do
    test "formats single digit shard IDs" do
      assert Keys.shard_tag(0) == "0"
      assert Keys.shard_tag(9) == "9"
    end

    test "formats shard IDs 10-35 as single letters" do
      assert Keys.shard_tag(10) == "a"
      assert Keys.shard_tag(35) == "z"
    end

    test "formats larger shard IDs" do
      assert Keys.shard_tag(36) == "10"
      assert Keys.shard_tag(1000) == "rs"
    end
  end

  describe "parse_shard_tag/1" do
    test "parses single digit tags" do
      assert Keys.parse_shard_tag("0") == {:ok, 0}
      assert Keys.parse_shard_tag("9") == {:ok, 9}
    end

    test "parses letter tags" do
      assert Keys.parse_shard_tag("a") == {:ok, 10}
      assert Keys.parse_shard_tag("z") == {:ok, 35}
    end

    test "parses multi-character tags" do
      assert Keys.parse_shard_tag("10") == {:ok, 36}
      assert Keys.parse_shard_tag("rs") == {:ok, 1000}
    end

    test "round-trips correctly" do
      for id <- [0, 1, 10, 35, 36, 100, 1000] do
        tag = Keys.shard_tag(id)
        assert {:ok, ^id} = Keys.parse_shard_tag(tag)
      end
    end
  end

  describe "chunk_path/2" do
    test "builds correct path with inverted version" do
      path = Keys.chunk_path("a", 1000)
      assert String.starts_with?(path, "c/a/")
      assert {:ok, 1000} = Keys.extract_version(path, Keys.chunks_prefix("a"))
    end
  end

  describe "chunks_prefix/1" do
    test "builds correct prefix" do
      assert Keys.chunks_prefix("a") == "c/a/"
    end
  end

  describe "snapshot_path/2" do
    test "builds correct path with inverted version" do
      path = Keys.snapshot_path("b", 2000)
      assert String.starts_with?(path, "s/b/")
      assert {:ok, 2000} = Keys.extract_version(path, Keys.snapshots_prefix("b"))
    end
  end

  describe "snapshots_prefix/1" do
    test "builds correct prefix" do
      assert Keys.snapshots_prefix("b") == "s/b/"
    end
  end

  describe "extract_version/2" do
    test "extracts version from chunk path" do
      path = Keys.chunk_path("c", 12_345)
      assert {:ok, 12_345} = Keys.extract_version(path, Keys.chunks_prefix("c"))
    end

    test "extracts version from snapshot path" do
      path = Keys.snapshot_path("d", 67_890)
      assert {:ok, 67_890} = Keys.extract_version(path, Keys.snapshots_prefix("d"))
    end

    test "returns error for a direct child that will not parse" do
      assert {:error, :invalid_format} = Keys.extract_version("c/a/manifest.json", "c/a/")
    end

    test "reports an object nested below the prefix as foreign" do
      # Bedrock only ever writes direct children here, so anything deeper
      # belongs to someone else who happens to share the bucket — and its
      # last path segment must not be mistaken for one of our versions.
      assert :foreign = Keys.extract_version("c/a/backup/" <> Keys.version_to_key(1000), "c/a/")
      assert :foreign = Keys.extract_version("c/a/vendor/notes.txt", "c/a/")
    end

    test "reports a key outside the prefix as foreign" do
      assert :foreign = Keys.extract_version("c/b/" <> Keys.version_to_key(1000), "c/a/")
    end

    test "reports the prefix's own folder marker as foreign" do
      # S3 consoles and sync tools leave a zero-byte object whose key IS
      # the prefix. Bedrock never writes one, and there is no name in it
      # to misread, so it is not a chunk we failed to understand.
      assert :foreign = Keys.extract_version("c/a/", "c/a/")
    end
  end
end
