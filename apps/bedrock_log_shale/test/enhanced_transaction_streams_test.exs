defmodule Bedrock.DataPlane.Log.EnhancedTransactionStreamsTest do
  @moduledoc """
  Enhanced tests for TransactionStreams that work with real WAL files.

  These tests specifically target the TransactionStreams module to catch bugs like:
  - segments with transactions: nil causing KeyError crashes
  - from_segments/2 failing to handle version ranges correctly
  - stream operations not properly loading transaction data

  This addresses the crashes we discovered in WALFileOperationsTest.
  """
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Log.Shale.Segment
  alias Bedrock.DataPlane.Log.Shale.TransactionStreams
  alias Bedrock.DataPlane.Version
  alias Bedrock.Test.DataPlane.WALTestSupport

  # Test setup helpers
  defp create_test_segment(file_path, min_version) do
    %Segment{
      path: file_path,
      min_version: Version.from_integer(min_version),
      transactions: nil
    }
  end

  defp with_test_wal(version_data_pairs, test_fn) do
    {file_path, version_map} = WALTestSupport.create_test_wal(version_data_pairs)

    try do
      test_fn.(file_path, version_map)
    after
      WALTestSupport.cleanup_test_file(file_path)
    end
  end

  defp with_multiple_test_wals(wal_specs, test_fn) do
    file_paths =
      Enum.map(wal_specs, fn {version_data_pairs, _min_version} ->
        {file_path, _} = WALTestSupport.create_test_wal(version_data_pairs)
        file_path
      end)

    try do
      test_fn.(file_paths)
    after
      Enum.each(file_paths, &WALTestSupport.cleanup_test_file/1)
    end
  end

  defp extract_versions_from_stream(segments, start_after) do
    assert {:ok, stream} = TransactionStreams.from_segments(segments, start_after)

    stream
    |> Enum.to_list()
    |> Enum.map(&WALTestSupport.extract_version/1)
  end

  describe "TransactionStreams.from_segments/2" do
    test "streams transactions from segments loaded from actual files" do
      version_data_pairs = [
        {100, %{"account" => "alice", "balance" => "1000"}},
        {200, %{"account" => "bob", "balance" => "500"}},
        {300, %{"account" => "charlie", "balance" => "750"}}
      ]

      with_test_wal(version_data_pairs, fn file_path, _version_map ->
        segment = create_test_segment(file_path, 100)
        start_after = Version.from_integer(150)

        versions = extract_versions_from_stream([segment], start_after)

        # Use variables for pattern matching
        expected_versions = [Version.from_integer(200), Version.from_integer(300)]
        assert ^expected_versions = Enum.sort(versions)
      end)
    end

    test "handles segments with nil transactions gracefully" do
      # This specifically tests the KeyError crash we discovered
      version_data_pairs = [
        {100, %{"key1" => "value1"}},
        {200, %{"key2" => "value2"}}
      ]

      with_test_wal(version_data_pairs, fn file_path, _version_map ->
        segment = create_test_segment(file_path, 100)

        assert {:ok, stream} = TransactionStreams.from_segments([segment], Version.from_integer(150))
        transactions = Enum.to_list(stream)
        # Ensure we get exactly one transaction and it's properly formed
        assert [transaction] = transactions
        assert is_binary(transaction)
      end)
    end

    test "applies start_after filtering across multiple segments" do
      wal_specs = [
        {[{100, %{"seg1" => "tx1"}}, {200, %{"seg1" => "tx2"}}], 100},
        {[{300, %{"seg2" => "tx1"}}, {400, %{"seg2" => "tx2"}}], 300}
      ]

      with_multiple_test_wals(wal_specs, fn [file1_path, file2_path] ->
        segments = [
          create_test_segment(file1_path, 100),
          create_test_segment(file2_path, 300)
        ]

        versions = extract_versions_from_stream(segments, Version.from_integer(250))

        # Pattern match expected results - should get 300 and 400 only
        expected_versions = [Version.from_integer(300), Version.from_integer(400)]
        assert ^expected_versions = Enum.sort(versions)
      end)
    end

    test "handles empty segments correctly" do
      empty_file_path = "test_empty_wal_#{:rand.uniform(999_999)}.log"
      File.write!(empty_file_path, :binary.copy(<<0>>, 1024))

      try do
        segment = create_test_segment(empty_file_path, 0)

        # Pattern match error tuple directly
        assert {:error, _reason} = TransactionStreams.from_segments([segment], Version.from_integer(0))
      after
        WALTestSupport.cleanup_test_file(empty_file_path)
      end
    end

    test "handles version boundary edge cases" do
      version_data_pairs = [
        {100, %{"boundary" => "test1"}},
        {200, %{"boundary" => "test2"}},
        {300, %{"boundary" => "test3"}}
      ]

      with_test_wal(version_data_pairs, fn file_path, _version_map ->
        segment = create_test_segment(file_path, 100)

        # Test start_after exactly at a transaction version - should exclude that version
        versions = extract_versions_from_stream([segment], Version.from_integer(200))

        # Pattern match expected result directly
        expected_version = Version.from_integer(300)
        assert [^expected_version] = versions
      end)
    end

    test "handles various start_after values without crashing" do
      # This test reproduces production conditions that led to KeyError crashes
      transactions = for i <- 1..5, do: {i * 100, %{"account" => "account_#{i}", "balance" => "#{i * 100}"}}

      with_test_wal(transactions, fn file_path, _version_map ->
        segment = create_test_segment(file_path, 100)

        test_cases = [
          # Before all - should get all 5
          {Version.from_integer(99), 5},
          # After first - should get 4
          {Version.from_integer(150), 4},
          # After second - should get 3
          {Version.from_integer(250), 3},
          # After fourth - should get 1
          {Version.from_integer(450), 1},
          # After all - should get none
          {Version.from_integer(600), 0}
        ]

        # Test all scenarios handle gracefully and return expected counts
        Enum.each(test_cases, fn {start_after, expected_count} ->
          result = TransactionStreams.from_segments([segment], start_after)

          case result do
            {:ok, stream} ->
              transactions = Enum.to_list(stream)
              assert length(transactions) == expected_count

            {:error, :not_found} ->
              # This is acceptable when start_after is beyond all transactions
              assert expected_count == 0

            {:error, _other_reason} ->
              flunk("Unexpected error for start_after #{inspect(start_after)}: #{inspect(result)}")
          end
        end)
      end)
    end
  end
end
