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

  import Bedrock.DataPlane.WALTestSupport, only: [setup_wal_test: 0]

  alias Bedrock.DataPlane.Log.Shale.Segment
  alias Bedrock.DataPlane.Log.Shale.TransactionStreams
  alias Bedrock.DataPlane.Version
  alias Bedrock.DataPlane.WALTestSupport

  setup_wal_test()

  describe "TransactionStreams with real WAL files" do
    test "can stream transactions from segments loaded from actual files" do
      # Create WAL file with known transactions
      version_data_pairs = [
        {100, %{"account" => "alice", "balance" => "1000"}},
        {200, %{"account" => "bob", "balance" => "500"}},
        {300, %{"account" => "charlie", "balance" => "750"}}
      ]

      {file_path, _version_map} = WALTestSupport.create_test_wal(version_data_pairs)

      try do
        segment = %Segment{
          path: file_path,
          min_version: Version.from_integer(100),
          # Start with nil - should be loaded on demand
          transactions: nil
        }

        start_after = Version.from_integer(150)

        assert {:ok, stream} = TransactionStreams.from_segments([segment], start_after)
        transactions = Enum.to_list(stream)
        assert length(transactions) == 2

        versions = Enum.map(transactions, &WALTestSupport.extract_version/1)
        assert Version.from_integer(200) in versions
        assert Version.from_integer(300) in versions
        refute Version.from_integer(100) in versions
      after
        WALTestSupport.cleanup_test_file(file_path)
      end
    end

    test "handles segments with nil transactions gracefully" do
      # This specifically tests the KeyError crash we discovered
      version_data_pairs = [
        {100, %{"key1" => "value1"}},
        {200, %{"key2" => "value2"}}
      ]

      {file_path, _version_map} = WALTestSupport.create_test_wal(version_data_pairs)

      try do
        segment = %Segment{
          path: file_path,
          min_version: Version.from_integer(100),
          # This was causing KeyError crashes
          transactions: nil
        }

        assert {:ok, stream} = TransactionStreams.from_segments([segment], Version.from_integer(150))
        transactions = Enum.to_list(stream)
        assert is_list(transactions)
      after
        WALTestSupport.cleanup_test_file(file_path)
      end
    end

    test "correctly applies start_after filtering across multiple segments" do
      segment1_data = [
        {100, %{"seg1" => "tx1"}},
        {200, %{"seg1" => "tx2"}}
      ]

      segment2_data = [
        {300, %{"seg2" => "tx1"}},
        {400, %{"seg2" => "tx2"}}
      ]

      {file1_path, _} = WALTestSupport.create_test_wal(segment1_data)
      {file2_path, _} = WALTestSupport.create_test_wal(segment2_data)

      try do
        segments = [
          %Segment{
            path: file1_path,
            min_version: Version.from_integer(100),
            transactions: nil
          },
          %Segment{
            path: file2_path,
            min_version: Version.from_integer(300),
            transactions: nil
          }
        ]

        # Request transactions after version 250 - should get 300 and 400, not 100 or 200
        assert {:ok, stream} = TransactionStreams.from_segments(segments, Version.from_integer(250))
        transactions = Enum.to_list(stream)
        versions = Enum.map(transactions, &WALTestSupport.extract_version/1)

        assert Version.from_integer(300) in versions
        assert Version.from_integer(400) in versions
        refute Version.from_integer(100) in versions
        refute Version.from_integer(200) in versions
      after
        WALTestSupport.cleanup_test_file(file1_path)
        WALTestSupport.cleanup_test_file(file2_path)
      end
    end

    test "handles empty segments correctly" do
      empty_file_path = "test_empty_wal_#{:rand.uniform(999_999)}.log"
      File.write!(empty_file_path, :binary.copy(<<0>>, 1024))

      try do
        segment = %Segment{
          path: empty_file_path,
          min_version: Version.from_integer(0),
          transactions: nil
        }

        # Should handle empty/invalid WAL files gracefully
        assert {:error, _reason} = TransactionStreams.from_segments([segment], Version.from_integer(0))
      after
        WALTestSupport.cleanup_test_file(empty_file_path)
      end
    end

    test "version boundary edge cases" do
      version_data_pairs = [
        {100, %{"boundary" => "test1"}},
        {200, %{"boundary" => "test2"}},
        {300, %{"boundary" => "test3"}}
      ]

      {file_path, _version_map} = WALTestSupport.create_test_wal(version_data_pairs)

      try do
        segment = %Segment{
          path: file_path,
          min_version: Version.from_integer(100),
          transactions: nil
        }

        # Test start_after exactly at a transaction version
        assert {:ok, stream} = TransactionStreams.from_segments([segment], Version.from_integer(200))
        transactions = Enum.to_list(stream)
        versions = Enum.map(transactions, &WALTestSupport.extract_version/1)

        assert Version.from_integer(300) in versions
        refute Version.from_integer(200) in versions
        refute Version.from_integer(100) in versions
      after
        WALTestSupport.cleanup_test_file(file_path)
      end
    end

    test "reproduces segment loading issue from production scenario" do
      # This test specifically reproduces the conditions that led to
      # the KeyError crash in the main test suite

      # Create realistic transaction pattern
      transactions =
        for i <- 1..5 do
          {i * 100, %{"account" => "account_#{i}", "balance" => "#{i * 100}"}}
        end

      {file_path, _version_map} = WALTestSupport.create_test_wal(transactions)

      try do
        # This replicates the exact state we saw in the crash: active_segment with transactions: nil
        segment = %Segment{
          path: file_path,
          min_version: Version.from_integer(100),
          transactions: nil
        }

        test_start_values = [
          Version.from_integer(99),
          Version.from_integer(150),
          Version.from_integer(250),
          Version.from_integer(450),
          Version.from_integer(600)
        ]

        # Test that we can handle various edge cases without crashing
        Enum.each(test_start_values, fn start_after ->
          result = TransactionStreams.from_segments([segment], start_after)

          case result do
            {:ok, stream} -> Enum.to_list(stream)
            {:error, _reason} -> :ok
          end
        end)
      after
        WALTestSupport.cleanup_test_file(file_path)
      end
    end
  end
end
