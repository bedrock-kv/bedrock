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
    version_int = Version.to_integer(version)
    TransactionTestSupport.new_log_transaction(version_int, %{"key" => "value_#{version_int}"})
  end

  defp segment_generator do
    gen all(versions <- list_of(version_generator_in_range(1, 1000), min_length: 1, max_length: 10)) do
      # Sort versions to maintain segment invariant: transactions in version order
      sorted_versions =
        Enum.sort(versions, fn v1, v2 ->
          Version.to_integer(v1) <= Version.to_integer(v2)
        end)

      min_version = List.first(sorted_versions)

      transactions = Enum.map(sorted_versions, &transaction_generator/1)

      %Segment{
        path: "/tmp/property_test_segment",
        min_version: min_version,
        transactions: transactions
      }
    end
  end

  defp segments_generator do
    gen all(segments <- list_of(segment_generator(), min_length: 1, max_length: 5)) do
      # Sort segments by min_version to simulate realistic ordering
      Enum.sort(segments, fn s1, s2 ->
        Version.to_integer(s1.min_version) <= Version.to_integer(s2.min_version)
      end)
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
            target_int = Version.to_integer(target_version)
            last_int = Version.to_integer(last_version)

            transaction_versions = get_transaction_versions(transactions)

            assert Enum.all?(transaction_versions, fn v -> v > target_int and v <= last_int end),
                   "Found transactions outside range (#{target_int}, #{last_int}]: #{inspect(transaction_versions)}"

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

            target_int = Version.to_integer(target_version)
            last_int = Version.to_integer(last_version)
            transaction_versions = get_transaction_versions(transactions)

            # Property 1: Respects all bounds
            assert length(transactions) <= limit
            assert Enum.all?(transaction_versions, fn v -> v > target_int and v <= last_int end)

            # Property 2: Ordering preserved
            assert_transactions_ordered(transactions)

            # Property 3: Should be equivalent to manual filtering and limiting
            all_expected = collect_expected_transactions_in_range(segments, target_version, last_version)
            expected_limited = Enum.take(all_expected, limit)
            expected_versions = get_transaction_versions(expected_limited)

            assert transaction_versions == expected_versions,
                   "Chained operations should match manual filtering and limiting"

          {:error, :not_found} ->
            assert_no_valid_transactions_exist(segments, target_version)
        end
      end
    end
  end

  # Helper functions for property assertions

  defp assert_all_transactions_greater_than(transactions, target_version) do
    target_int = Version.to_integer(target_version)
    transaction_versions = get_transaction_versions(transactions)

    violating_versions = Enum.filter(transaction_versions, fn v -> v <= target_int end)

    assert violating_versions == [],
           "Found transactions <= target_version (#{target_int}): #{inspect(violating_versions)}"
  end

  defp assert_transactions_ordered(transactions) do
    versions = get_transaction_versions(transactions)
    sorted_versions = Enum.sort(versions)

    assert versions == sorted_versions,
           "Transactions not in order. Got: #{inspect(versions)}, Expected: #{inspect(sorted_versions)}"
  end

  defp assert_contains_all_expected(actual_transactions, expected_transactions) do
    actual_versions = actual_transactions |> get_transaction_versions() |> Enum.sort()
    expected_versions = expected_transactions |> get_transaction_versions() |> Enum.sort()

    missing_versions = expected_versions -- actual_versions

    assert missing_versions == [],
           "Missing expected transactions with versions: #{inspect(missing_versions)}"
  end

  defp assert_no_unexpected_transactions(actual_transactions, expected_transactions) do
    actual_versions = actual_transactions |> get_transaction_versions() |> Enum.sort()
    expected_versions = expected_transactions |> get_transaction_versions() |> Enum.sort()

    unexpected_versions = actual_versions -- expected_versions

    assert unexpected_versions == [],
           "Found unexpected transactions with versions: #{inspect(unexpected_versions)}"
  end

  defp assert_no_valid_transactions_exist(segments, target_version) do
    target_int = Version.to_integer(target_version)

    all_transactions = Enum.flat_map(segments, fn segment -> segment.transactions end)
    all_versions = get_transaction_versions(all_transactions)
    valid_versions = Enum.filter(all_versions, fn v -> v > target_int end)

    assert valid_versions == [],
           "Expected :not_found but found valid transactions with versions: #{inspect(valid_versions)}"
  end

  defp get_transaction_versions(transactions) do
    Enum.map(transactions, fn tx ->
      tx |> Transaction.extract_commit_version!() |> Version.to_integer()
    end)
  end

  defp collect_expected_transactions(segments, target_version) do
    target_int = Version.to_integer(target_version)

    segments
    |> Enum.flat_map(fn segment -> segment.transactions end)
    |> Enum.filter(fn tx ->
      version = tx |> Transaction.extract_commit_version!() |> Version.to_integer()
      version > target_int
    end)
    |> Enum.sort_by(fn tx ->
      tx |> Transaction.extract_commit_version!() |> Version.to_integer()
    end)
  end

  defp collect_expected_transactions_in_range(segments, target_version, last_version) do
    target_int = Version.to_integer(target_version)
    last_int = Version.to_integer(last_version)

    segments
    |> Enum.flat_map(fn segment -> segment.transactions end)
    |> Enum.filter(fn tx ->
      version = tx |> Transaction.extract_commit_version!() |> Version.to_integer()
      version > target_int and version <= last_int
    end)
    |> Enum.sort_by(fn tx ->
      tx |> Transaction.extract_commit_version!() |> Version.to_integer()
    end)
  end
end
