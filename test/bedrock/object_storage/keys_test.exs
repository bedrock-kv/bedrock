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

  describe "cluster_state_path/0" do
    test "builds correct path" do
      assert Keys.cluster_state_path() == "state"
    end
  end

  describe "chunk_path/2" do
    test "builds correct path with inverted version" do
      path = Keys.chunk_path("a", 1000)
      assert String.starts_with?(path, "c/a/")
      assert {:ok, 1000} = Keys.extract_version(path)
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
      assert {:ok, 2000} = Keys.extract_version(path)
    end
  end

  describe "snapshots_prefix/1" do
    test "builds correct prefix" do
      assert Keys.snapshots_prefix("b") == "s/b/"
    end
  end

  describe "extract_version/1" do
    test "extracts version from chunk path" do
      path = Keys.chunk_path("c", 12_345)
      assert {:ok, 12_345} = Keys.extract_version(path)
    end

    test "extracts version from snapshot path" do
      path = Keys.snapshot_path("d", 67_890)
      assert {:ok, 67_890} = Keys.extract_version(path)
    end

    test "returns error for invalid path" do
      # Path basename must contain invalid base36 chars
      assert {:error, :invalid_format} = Keys.extract_version("invalid/path/abc!")
    end
  end
end
