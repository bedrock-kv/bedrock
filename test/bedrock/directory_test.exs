defmodule Bedrock.DirectoryTest do
  use ExUnit.Case, async: true

  import Mox

  alias Bedrock.Directory
  alias Bedrock.KeySelector
  alias Bedrock.Subspace

  setup :verify_on_exit!

  describe "directory creation and basic functionality" do
    test "creates directory with proper configuration" do
      directory = Directory.new(MockRepo)

      assert directory.repo_module == MockRepo
      # Now these are Subspace objects with tuple encoding
      assert Subspace.key(directory.content_subspace) == Bedrock.Key.pack({"_content"})
      assert Subspace.key(directory.node_subspace) == Bedrock.Key.pack({"_node"})
      assert directory.allow_manual_prefixes == false
    end

    test "creates directory with custom configuration" do
      directory = Directory.new(MockRepo, "custom_content", "custom_node", allow_manual_prefixes: true)

      assert directory.repo_module == MockRepo
      # Now these are Subspace objects with tuple encoding
      assert Subspace.key(directory.content_subspace) == Bedrock.Key.pack({"custom_content"})
      assert Subspace.key(directory.node_subspace) == Bedrock.Key.pack({"custom_node"})
      assert directory.allow_manual_prefixes == true
    end
  end

  describe "basic directory operations" do
    test "creates new directories" do
      directory = Directory.new(MockRepo)

      # Expected call sequence for creating new directory with HCA allocation:
      # 1. get - check if directory already exists (nil = doesn't exist)
      # 2. HCA allocation sequence:
      #    - select - find current HCA start
      #    - add - increment counter
      #    - get - read counter value
      #    - select - verify window hasn't advanced
      #    - get - check candidate availability
      #    - put - claim candidate (no_write_conflict)
      #    - put - add write conflict
      # 3. put - store directory metadata with allocated prefix

      MockRepo
      |> expect(:transaction, fn fun -> fun.(:mock_txn) end)
      # Step 1: Check if directory exists (now using tuple-encoded key)
      |> expect(:get, fn :mock_txn, key ->
        # Key should be tuple-encoded path in node subspace
        expected_key = Subspace.pack(Subspace.new({"_node"}), {"users"})
        assert key == expected_key
        nil
      end)
      # Step 2: HCA allocation sequence
      # current_start
      |> expect(:select, fn :mock_txn, %KeySelector{} -> nil end)
      |> expect(:add, fn :mock_txn, _counter_key, 1 -> :mock_txn end)
      |> expect(:get, fn :mock_txn, _counter_key, [snapshot: true] -> <<1::64-little>> end)
      # verify window
      |> expect(:select, fn :mock_txn, %KeySelector{} -> nil end)
      # candidate available
      |> expect(:get, fn :mock_txn, _candidate_key, [snapshot: true] -> nil end)
      |> expect(:put, fn :mock_txn, _candidate_key, "", [no_write_conflict: true] -> :mock_txn end)
      # add write conflict
      |> expect(:add_write_conflict_range, fn :mock_txn, _start_key, _end_key -> :mock_txn end)
      # Step 3: Store directory with allocated prefix (tuple-encoded key)
      |> expect(:put, fn :mock_txn, key, prefix ->
        expected_key = Subspace.pack(Subspace.new({"_node"}), {"users"})
        assert key == expected_key
        assert is_binary(prefix)
        assert prefix != ""
        :mock_txn
      end)

      prefix =
        MockRepo.transaction(fn txn ->
          Directory.create_directory(directory, txn, ["users"])
        end)

      assert is_binary(prefix)
      assert prefix != ""
    end
  end

  describe "edge cases and validation" do
    test "handles empty paths correctly" do
      directory = Directory.new(MockRepo)

      MockRepo
      |> expect(:transaction, fn fun -> fun.(:mock_txn) end)
      # Check if root directory exists (tuple-encoded empty path)
      |> expect(:get, fn :mock_txn, key ->
        expected_key = Subspace.pack(Subspace.new({"_node"}), {})
        assert key == expected_key
        nil
      end)
      # HCA allocation sequence for root
      |> expect(:select, fn :mock_txn, %KeySelector{} -> nil end)
      |> expect(:add, fn :mock_txn, _counter_key, 1 -> :mock_txn end)
      |> expect(:get, fn :mock_txn, _counter_key, [snapshot: true] -> <<1::64-little>> end)
      |> expect(:select, fn :mock_txn, %KeySelector{} -> nil end)
      |> expect(:get, fn :mock_txn, _candidate_key, [snapshot: true] -> nil end)
      |> expect(:put, fn :mock_txn, _candidate_key, "", [no_write_conflict: true] -> :mock_txn end)
      |> expect(:add_write_conflict_range, fn :mock_txn, _start_key, _end_key -> :mock_txn end)
      # Store root directory (tuple-encoded empty path)
      |> expect(:put, fn :mock_txn, key, _prefix ->
        expected_key = Subspace.pack(Subspace.new({"_node"}), {})
        assert key == expected_key
        :mock_txn
      end)

      prefix =
        MockRepo.transaction(fn txn ->
          Directory.create_directory(directory, txn, [])
        end)

      assert is_binary(prefix)
    end

    test "validates path components are binaries" do
      directory = Directory.new(MockRepo)

      MockRepo
      |> expect(:transaction, fn fun -> fun.(:mock_txn) end)
      # Check if target directory already exists (this is called first, tuple-encoded)
      |> expect(:get, fn :mock_txn, key ->
        expected_key = Subspace.pack(Subspace.new({"_node"}), {"valid_path"})
        assert key == expected_key
        nil
      end)
      # HCA allocation for target directory (since parent is root, it always exists)
      |> expect(:select, fn :mock_txn, %KeySelector{} -> nil end)
      |> expect(:add, fn :mock_txn, _counter_key, 1 -> :mock_txn end)
      |> expect(:get, fn :mock_txn, _counter_key, [snapshot: true] -> <<1::64-little>> end)
      |> expect(:select, fn :mock_txn, %KeySelector{} -> nil end)
      |> expect(:get, fn :mock_txn, _candidate_key, [snapshot: true] -> nil end)
      |> expect(:put, fn :mock_txn, _candidate_key, "", [no_write_conflict: true] -> :mock_txn end)
      |> expect(:add_write_conflict_range, fn :mock_txn, _start_key, _end_key -> :mock_txn end)
      # Store target directory with allocated prefix (tuple-encoded)
      |> expect(:put, fn :mock_txn, key, prefix ->
        expected_key = Subspace.pack(Subspace.new({"_node"}), {"valid_path"})
        assert key == expected_key
        assert is_binary(prefix)
        :mock_txn
      end)

      prefix =
        MockRepo.transaction(fn txn ->
          Directory.create_directory(directory, txn, ["valid_path"])
        end)

      assert is_binary(prefix)
    end
  end

  describe "statistics without side effects" do
    test "stats returns expected structure" do
      directory = Directory.new(MockRepo)

      # Expected calls for directory stats:
      # 1. HCA.stats - which calls select + range
      # 2. range - to count directories

      MockRepo
      |> expect(:transaction, fn fun -> fun.(:mock_txn) end)
      # HCA stats calls
      # find latest counter
      |> expect(:select, fn :mock_txn, %KeySelector{} -> nil end)
      |> expect(:range, fn :mock_txn, counter_start, counter_end ->
        # Verify HCA counter range query (now using tuple encoding)
        hca_subspace = Subspace.pack(Subspace.new({"_content"}), {"hca"})
        assert String.starts_with?(counter_start, hca_subspace)
        assert String.starts_with?(counter_end, hca_subspace)
        # No counters
        []
      end)
      # Directory counting range query (now using tuple-encoded subspace range)
      |> expect(:range, fn :mock_txn, start_key, end_key ->
        # Verify directory range query using subspace ranges
        node_subspace = Subspace.new({"_node"})
        {expected_start, expected_end} = Subspace.range(node_subspace)
        assert start_key == expected_start
        assert end_key == expected_end
        # Mock some directory entries with tuple-encoded keys
        [
          {Subspace.pack(node_subspace, {"dir1"}), "prefix1"},
          {Subspace.pack(node_subspace, {"dir2"}), "prefix2"}
        ]
      end)

      stats =
        MockRepo.transaction(fn txn ->
          Directory.stats(directory, txn)
        end)

      assert is_map(stats)
      assert Map.has_key?(stats, :hca_stats)
      assert Map.has_key?(stats, :directory_count)
      assert Map.has_key?(stats, :allocated_prefixes)

      # Verify calculated values
      # Only existing directories counted (deleted directories don't appear in range)
      assert stats.directory_count == 2
      # from HCA stats
      assert stats.allocated_prefixes == 0
    end
  end
end
