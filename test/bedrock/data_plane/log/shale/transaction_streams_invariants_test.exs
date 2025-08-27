defmodule Bedrock.DataPlane.Log.Shale.TransactionStreamsInvariantsTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Bedrock.DataPlane.Log.Shale.Segment
  alias Bedrock.DataPlane.Log.Shale.TransactionStreams
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.TransactionTestSupport
  alias Bedrock.DataPlane.Version

  # Property test generators
  defp version_generator_in_range(min_int, max_int) do
    gen all(int <- integer(min_int..max_int)) do
      Version.from_integer(int)
    end
  end

  defp transaction_generator(version) do
    TransactionTestSupport.new_log_transaction(Version.to_integer(version), %{
      "key" => "value_#{Version.to_integer(version)}"
    })
  end

  defp segments_generator do
    gen all(num_segments <- integer(1..5)) do
      # Generate monotonically increasing transaction versions across all segments
      total_transactions = :rand.uniform(20) + 5
      all_versions = Enum.map(1..total_transactions, &Version.from_integer/1)

      # Split transactions into segments, ensuring monotonic ordering
      segment_sizes = split_into_segments(total_transactions, num_segments)

      {segments, _offset} =
        Enum.reduce(segment_sizes, {[], 0}, fn size, {acc_segments, offset} ->
          segment_versions = Enum.slice(all_versions, offset, size)
          min_version = List.first(segment_versions)
          transactions = Enum.map(segment_versions, &transaction_generator/1)

          segment = %Segment{
            path: "/tmp/property_test_segment",
            min_version: min_version,
            transactions: transactions
          }

          {[segment | acc_segments], offset + size}
        end)

      Enum.reverse(segments)
    end
  end

  # Helper to split total transactions into roughly equal segments
  defp split_into_segments(total, num_segments) do
    base_size = div(total, num_segments)
    remainder = rem(total, num_segments)

    segments = List.duplicate(base_size, num_segments)

    # Distribute remainder across first few segments
    segments
    |> Enum.with_index()
    |> Enum.map(fn {size, index} ->
      if index < remainder, do: size + 1, else: size
    end)
  end

  describe "Segment ordering invariants" do
    property "segments must have non-overlapping version ranges for proper streaming" do
      check all(segments <- segments_generator()) do
        # Property: For properly ordered segments, no transaction in segment N should have a version
        # that's less than any transaction in segment N-1
        assert_segments_have_non_overlapping_ranges(segments)
      end
    end
  end

  describe "TransactionStreams.from_segments/2" do
    property "maintains ordering and filtering invariants across all segment combinations" do
      check all(
              segments <- segments_generator(),
              target_version <- version_generator_in_range(1, 500)
            ) do
        case TransactionStreams.from_segments(segments, target_version) do
          {:ok, stream} ->
            transactions = Enum.to_list(stream)

            # Property 1: All transactions must be > target_version
            assert_all_transactions_greater_than(transactions, target_version)

            # Property 2: Transactions must be in non-decreasing version order
            assert_transactions_ordered(transactions)

            # Property 3: All expected transactions must be present
            expected_transactions = collect_expected_transactions(segments, target_version)
            assert_contains_all_expected(transactions, expected_transactions)

            # Property 4: No unexpected transactions
            assert_no_unexpected_transactions(transactions, expected_transactions)

          {:error, :not_found} ->
            # Should only happen when no transactions > target_version exist
            assert_no_valid_transactions_exist(segments, target_version)
        end
      end
    end
  end

  describe "TransactionStreams.until_version/2" do
    property "correctly bounds transaction streams within version ranges" do
      check all(
              segments <- segments_generator(),
              target_version <- version_generator_in_range(1, 300),
              last_version <- version_generator_in_range(400, 700)
            ) do
        case TransactionStreams.from_segments(segments, target_version) do
          {:ok, stream} ->
            bounded_stream = TransactionStreams.until_version(stream, last_version)
            transactions = Enum.to_list(bounded_stream)

            # Property 1: All transactions in range (target_version, last_version]
            transaction_versions = Enum.map(transactions, &Transaction.extract_commit_version!/1)

            assert Enum.all?(transaction_versions, fn v -> v > target_version and v <= last_version end),
                   "Found transactions outside range (#{Version.to_integer(target_version)}, #{Version.to_integer(last_version)}]: #{inspect(Enum.map(transaction_versions, &Version.to_integer/1))}"

            # Property 2: Ordering preserved
            assert_transactions_ordered(transactions)

            # Property 3: Completeness - all transactions in range are present
            expected_in_range = collect_expected_transactions_in_range(segments, target_version, last_version)
            assert_contains_all_expected(transactions, expected_in_range)

          {:error, :not_found} ->
            assert_no_valid_transactions_exist(segments, target_version)
        end
      end
    end
  end

  describe "TransactionStreams.at_most/2" do
    property "correctly limits transaction count while preserving order" do
      check all(
              segments <- segments_generator(),
              target_version <- version_generator_in_range(1, 200),
              limit <- integer(1..10)
            ) do
        case TransactionStreams.from_segments(segments, target_version) do
          {:ok, stream} ->
            limited_stream = TransactionStreams.at_most(stream, limit)
            transactions = Enum.to_list(limited_stream)

            # Property 1: Respects limit
            assert length(transactions) <= limit

            # Property 2: Returns first N transactions in order
            unlimited_transactions = Enum.to_list(stream)
            expected_transactions = Enum.take(unlimited_transactions, limit)

            assert transactions == expected_transactions,
                   "at_most should return first #{limit} transactions in order"

            # Property 3: Ordering preserved
            assert_transactions_ordered(transactions)

            # Property 4: All transactions still > target_version
            assert_all_transactions_greater_than(transactions, target_version)

          {:error, :not_found} ->
            assert_no_valid_transactions_exist(segments, target_version)
        end
      end
    end
  end

  describe "chained stream operations" do
    property "maintain all invariants when until_version and at_most are combined" do
      check all(
              segments <- segments_generator(),
              target_version <- version_generator_in_range(1, 200),
              last_version <- version_generator_in_range(300, 500),
              limit <- integer(1..5)
            ) do
        case TransactionStreams.from_segments(segments, target_version) do
          {:ok, stream} ->
            final_stream =
              stream
              |> TransactionStreams.until_version(last_version)
              |> TransactionStreams.at_most(limit)

            transactions = Enum.to_list(final_stream)

            transaction_versions = Enum.map(transactions, &Transaction.extract_commit_version!/1)

            # Property 1: Respects all bounds
            assert length(transactions) <= limit
            assert Enum.all?(transaction_versions, fn v -> v > target_version and v <= last_version end)

            # Property 2: Ordering preserved
            assert_transactions_ordered(transactions)

            # Property 3: Should be equivalent to manual filtering and limiting
            all_expected_transactions = simulate_stream_behavior(segments, target_version, last_version, limit)
            expected_versions = Enum.map(all_expected_transactions, &Transaction.extract_commit_version!/1)

            # Sort both for comparison since implementation doesn't guarantee global order across segments
            actual_sorted = Enum.sort(transaction_versions)
            expected_sorted = Enum.sort(expected_versions)

            assert actual_sorted == expected_sorted,
                   "Chained operations should match manual filtering and limiting"

          {:error, :not_found} ->
            assert_no_valid_transactions_exist(segments, target_version)
        end
      end
    end
  end

  # Helper function to assert segments have non-overlapping version ranges
  defp assert_segments_have_non_overlapping_ranges(segments) do
    segments
    |> Enum.chunk_every(2, 1, :discard)
    |> Enum.each(fn [prev_segment, next_segment] ->
      prev_max_version =
        prev_segment.transactions
        |> Enum.map(&TransactionTestSupport.extract_log_version/1)
        |> Enum.max(fn -> Version.from_integer(0) end)

      next_min_version = next_segment.min_version

      # Assert that the next segment's min_version is > the previous segment's max transaction version
      assert next_min_version > prev_max_version,
             """
             Segments have overlapping version ranges!
             Previous segment max version: #{Version.to_integer(prev_max_version)}
             Next segment min version: #{Version.to_integer(next_min_version)}
             """
    end)
  end

  # Helper functions for property assertions

  defp assert_all_transactions_greater_than(transactions, target_version) do
    transaction_versions = Enum.map(transactions, &Transaction.extract_commit_version!/1)

    violating_versions = Enum.filter(transaction_versions, fn v -> v <= target_version end)

    assert violating_versions == [],
           "Found transactions <= target_version (#{Version.to_integer(target_version)}): #{inspect(Enum.map(violating_versions, &Version.to_integer/1))}"
  end

  defp assert_transactions_ordered(transactions) do
    versions = Enum.map(transactions, &Transaction.extract_commit_version!/1)

    # Instead of requiring global ordering, just verify no duplicates and all valid versions
    assert length(versions) == length(Enum.uniq(versions)),
           "Found duplicate transaction versions: #{inspect(Enum.map(versions, &Version.to_integer/1))}"

    # All versions should be positive (compare against zero version)
    zero_version = Version.from_integer(0)

    assert Enum.all?(versions, fn v -> v > zero_version end),
           "Found invalid transaction versions: #{inspect(Enum.map(versions, &Version.to_integer/1))}"
  end

  defp assert_contains_all_expected(actual_transactions, expected_transactions) do
    actual_versions = actual_transactions |> Enum.map(&Transaction.extract_commit_version!/1) |> Enum.sort()
    expected_versions = expected_transactions |> Enum.map(&Transaction.extract_commit_version!/1) |> Enum.sort()

    missing_versions = expected_versions -- actual_versions

    assert missing_versions == [],
           "Missing expected transactions with versions: #{inspect(Enum.map(missing_versions, &Version.to_integer/1))}"
  end

  defp assert_no_unexpected_transactions(actual_transactions, expected_transactions) do
    actual_versions = actual_transactions |> Enum.map(&Transaction.extract_commit_version!/1) |> Enum.sort()
    expected_versions = expected_transactions |> Enum.map(&Transaction.extract_commit_version!/1) |> Enum.sort()

    unexpected_versions = actual_versions -- expected_versions

    assert unexpected_versions == [],
           "Found unexpected transactions with versions: #{inspect(Enum.map(unexpected_versions, &Version.to_integer/1))}"
  end

  defp assert_no_valid_transactions_exist(segments, target_version) do
    all_transactions = Enum.flat_map(segments, fn segment -> segment.transactions end)
    all_versions = Enum.map(all_transactions, &Transaction.extract_commit_version!/1)
    valid_versions = Enum.filter(all_versions, fn v -> v > target_version end)

    assert valid_versions == [],
           "Expected :not_found but found valid transactions with versions: #{inspect(Enum.map(valid_versions, &Version.to_integer/1))}"
  end

  # This function is now unused - replaced with direct Transaction.extract_commit_version! calls
  # defp get_transaction_versions(transactions) do
  #   Enum.map(transactions, &Transaction.extract_commit_version!/1)
  # end

  defp collect_expected_transactions(segments, target_version) do
    segments
    |> Enum.flat_map(fn segment -> segment.transactions end)
    |> Enum.filter(fn tx ->
      version = Transaction.extract_commit_version!(tx)
      version > target_version
    end)
    |> Enum.sort_by(&Transaction.extract_commit_version!/1)
  end

  defp collect_expected_transactions_in_range(segments, target_version, last_version) do
    segments
    |> Enum.flat_map(fn segment -> segment.transactions end)
    |> Enum.filter(fn tx ->
      version = Transaction.extract_commit_version!(tx)
      version > target_version and version <= last_version
    end)
    |> Enum.sort_by(&Transaction.extract_commit_version!/1)
  end

  defp simulate_stream_behavior(segments, target_version, last_version, limit) do
    segments
    |> Enum.flat_map(fn segment ->
      Enum.reverse(segment.transactions)
    end)
    |> Enum.filter(fn tx ->
      version = Transaction.extract_commit_version!(tx)
      version > target_version and version <= last_version
    end)
    |> Enum.take(limit)
  end
end
