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
    test "formats zero as 20-character padded string" do
      assert Keys.format_inverted_version(0) == "00000000000000000000"
    end

    test "formats small number with padding" do
      assert Keys.format_inverted_version(1000) == "00000000000000001000"
    end

    test "formats max version" do
      # 18446744073709551615 is 20 digits
      assert Keys.format_inverted_version(@max_version) == "18446744073709551615"
    end
  end

  describe "parse_inverted_version/1" do
    test "parses valid padded string" do
      assert Keys.parse_inverted_version("00000000000000001000") == {:ok, 1000}
    end

    test "parses zero" do
      assert Keys.parse_inverted_version("00000000000000000000") == {:ok, 0}
    end

    test "parses max version" do
      assert Keys.parse_inverted_version("18446744073709551615") == {:ok, @max_version}
    end

    test "returns error for invalid format" do
      assert Keys.parse_inverted_version("invalid") == {:error, :invalid_format}
      assert Keys.parse_inverted_version("123abc") == {:error, :invalid_format}
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

  describe "cluster_state_path/1" do
    test "builds correct path" do
      assert Keys.cluster_state_path("my-cluster") == "my-cluster/state"
    end
  end

  describe "chunk_path/3" do
    test "builds correct path with inverted version" do
      path = Keys.chunk_path("my-cluster", "shard-01", 1000)
      assert String.starts_with?(path, "my-cluster/shards/shard-01/chunks/")
      assert {:ok, 1000} = Keys.extract_version(path)
    end
  end

  describe "chunks_prefix/2" do
    test "builds correct prefix" do
      assert Keys.chunks_prefix("my-cluster", "shard-01") == "my-cluster/shards/shard-01/chunks/"
    end
  end

  describe "snapshot_path/3" do
    test "builds correct path with inverted version" do
      path = Keys.snapshot_path("my-cluster", "shard-01", 2000)
      assert String.starts_with?(path, "my-cluster/shards/shard-01/snapshots/")
      assert {:ok, 2000} = Keys.extract_version(path)
    end
  end

  describe "snapshots_prefix/2" do
    test "builds correct prefix" do
      assert Keys.snapshots_prefix("my-cluster", "shard-01") == "my-cluster/shards/shard-01/snapshots/"
    end
  end

  describe "extract_version/1" do
    test "extracts version from chunk path" do
      path = Keys.chunk_path("cluster", "shard", 12_345)
      assert {:ok, 12_345} = Keys.extract_version(path)
    end

    test "extracts version from snapshot path" do
      path = Keys.snapshot_path("cluster", "shard", 67_890)
      assert {:ok, 67_890} = Keys.extract_version(path)
    end

    test "returns error for invalid path" do
      assert {:error, :invalid_format} = Keys.extract_version("invalid/path/abc")
    end
  end
end
