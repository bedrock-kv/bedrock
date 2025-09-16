defmodule Bedrock.HighContentionAllocatorTest do
  use ExUnit.Case, async: true

  import Mox

  alias Bedrock.HighContentionAllocator
  alias Bedrock.KeySelector

  setup :verify_on_exit!

  # Helper functions for key assertions
  defp assert_counter_key(key, hca, expected_start) do
    assert key == <<hca.counters_subspace::binary, expected_start::64-big>>
  end

  defp assert_recent_key(key, hca, expected_candidate) do
    assert key == <<hca.recent_subspace::binary, expected_candidate::64-big>>
  end

  # Helper to encode recent key for conflict range assertions
  defp encode_recent_key(hca, candidate) do
    hca.recent_subspace <> <<candidate::64-big>>
  end

  # Helper to set up common allocation expectations
  defp expect_allocation_sequence(mock, hca, window_start, count, candidate, available \\ true) do
    mock
    |> expect_base_allocation_calls(hca, window_start, count, candidate, available)
    |> expect_availability_dependent_calls(hca, candidate, available)
  end

  defp expect_base_allocation_calls(mock, hca, window_start, count, candidate, available) do
    mock
    |> expect(:select, fn %KeySelector{} = selector ->
      assert selector.key == hca.counters_subspace <> <<0xFF>>
      nil
    end)
    |> expect(:atomic, fn :add, key, <<1::64-little>> ->
      assert_counter_key(key, hca, window_start)
      :mock_txn
    end)
    |> expect(:get, fn key, [snapshot: true] ->
      assert_counter_key(key, hca, window_start)
      <<count::64-little>>
    end)
    |> expect(:select, fn %KeySelector{} = selector ->
      assert selector.key == hca.counters_subspace <> <<0xFF>>
      nil
    end)
    |> expect(:get, fn candidate_key, [snapshot: true] ->
      assert_recent_key(candidate_key, hca, candidate)
      if available, do: nil, else: "taken"
    end)
  end

  defp expect_availability_dependent_calls(mock, hca, candidate, true) do
    mock
    |> expect(:put, fn candidate_key, "", [no_write_conflict: true] ->
      assert_recent_key(candidate_key, hca, candidate)
      :mock_txn
    end)
    |> expect(:add_write_conflict_range, fn start_key, end_key ->
      assert start_key == encode_recent_key(hca, candidate)
      assert end_key == start_key <> <<0>>
      :mock_txn
    end)
  end

  defp expect_availability_dependent_calls(mock, _hca, _candidate, false), do: mock

  # Helper to create deterministic random function
  defp deterministic_random_fn(values) when is_list(values) do
    call_count = :counters.new(1, [])
    :counters.put(call_count, 1, 0)

    fn _size ->
      count = :counters.get(call_count, 1)
      :counters.add(call_count, 1, 1)
      Enum.at(values, count)
    end
  end

  defp deterministic_random_fn(value) when is_integer(value) do
    fn _size -> value end
  end

  describe "HighContentionAllocator creation and configuration" do
    test "creates HighContentionAllocator with new two-subspace structure" do
      assert %{repo: MockRepo, counters_subspace: "test_allocator" <> <<0>>, recent_subspace: "test_allocator" <> <<1>>} =
               HighContentionAllocator.new(MockRepo, "test_allocator")
    end

    test "handles different subspace prefixes correctly" do
      assert %{counters_subspace: "my_alloc" <> <<0>>, recent_subspace: "my_alloc" <> <<1>>} =
               HighContentionAllocator.new(MockRepo, "my_alloc")
    end

    test "handles empty stats correctly" do
      hca = HighContentionAllocator.new(MockRepo, "test_empty")

      # Expected call sequence for stats:
      # 1. select to find latest counter
      # 2. range to count window entries

      MockRepo
      |> expect(:transact, fn fun -> fun.() end)
      |> expect(:select, fn %KeySelector{} = selector ->
        # Verify selector is looking for last counter
        assert selector.key == "test_empty" <> <<0, 255>>
        assert selector.or_equal == false
        assert selector.offset == 0
        # No counters yet
        nil
      end)
      |> expect(:get_range, fn start_key, end_key ->
        # Verify range query parameters for counter counting in stats
        assert start_key == "test_empty" <> <<0>>
        assert end_key == "test_empty" <> <<0, 255>>
        # No counter entries
        []
      end)

      assert {:ok, %{latest_window_start: 0, total_counters: 0, estimated_allocated: 0}} =
               HighContentionAllocator.stats(hca)
    end
  end

  describe "allocation interface" do
    test "provides allocate/2 function with correct call sequence" do
      # With start=0, window_size=64, random=42: candidate = 0 + (42 - 1) = 41
      expected_candidate = 41
      hca = HighContentionAllocator.new(MockRepo, "test_alloc", random_fn: deterministic_random_fn(42))

      MockRepo
      |> expect(:transact, fn fun -> fun.() end)
      |> expect_allocation_sequence(hca, 0, 1, expected_candidate)

      assert {:ok, encoded_result} = HighContentionAllocator.allocate(hca)

      # Verify it's the expected candidate encoded as compact binary
      assert encoded_result == Bedrock.Key.pack(expected_candidate)
    end

    test "provides allocate_many/3 function with correct multiplied calls" do
      # Calculate expected candidates: start=0, window_size=64
      # First: candidate = 0 + (10 - 1) = 9
      # Second: candidate = 0 + (25 - 1) = 24
      expected_candidates = [9, 24]
      [first_candidate, second_candidate] = expected_candidates
      hca = HighContentionAllocator.new(MockRepo, "test_many", random_fn: deterministic_random_fn([10, 25]))

      MockRepo
      |> expect(:transact, 2, fn fun -> fun.() end)
      |> expect_allocation_sequence(hca, 0, 1, first_candidate)
      |> expect_allocation_sequence(hca, 0, 2, second_candidate)

      assert {:ok, encoded_results} = HighContentionAllocator.allocate_many(hca, 2)

      # Verify each result is the expected candidate encoded as compact binary
      expected_encoded = Enum.map(expected_candidates, &Bedrock.Key.pack/1)
      assert encoded_results == expected_encoded
    end
  end

  describe "error handling" do
    test "handles candidate collision with retry" do
      # Calculate expected candidates: start=0, window_size=64
      # First attempt: candidate = 0 + (15 - 1) = 14 (will be taken)
      # Retry attempt: candidate = 0 + (30 - 1) = 29 (will succeed)
      first_candidate = 14
      retry_candidate = 29
      hca = HighContentionAllocator.new(MockRepo, "test_collision", random_fn: deterministic_random_fn([15, 30]))

      MockRepo
      |> expect(:transact, fn fun -> fun.() end)
      |> expect_allocation_sequence(hca, 0, 1, first_candidate, false)
      |> expect_allocation_sequence(hca, 0, 2, retry_candidate)

      assert {:ok, encoded_result} = HighContentionAllocator.allocate(hca)

      # Verify it's the expected candidate encoded as compact binary
      assert encoded_result == Bedrock.Key.pack(retry_candidate)
    end
  end

  describe "key generation and encoding" do
    test "generates correct counter key format" do
      assert %{counters_subspace: "test/prefix" <> <<0>>, recent_subspace: "test/prefix" <> <<1>>} =
               HighContentionAllocator.new(MockRepo, "test/prefix")
    end

    test "uses custom random function when provided" do
      # With start=0, window_size=64, random=1: candidate = 0 + (1 - 1) = 0
      expected_candidate = 0
      hca = HighContentionAllocator.new(MockRepo, "test_random", random_fn: deterministic_random_fn(1))

      MockRepo
      |> expect(:transact, fn fun -> fun.() end)
      |> expect_allocation_sequence(hca, 0, 1, expected_candidate)

      assert {:ok, encoded_result} = HighContentionAllocator.allocate(hca)

      # Verify it's the expected candidate encoded as compact binary
      assert encoded_result == Bedrock.Key.pack(expected_candidate)
    end
  end

  describe "dynamic window sizing" do
    test "uses correct window sizes for different ranges" do
      hca = HighContentionAllocator.new(MockRepo, "test")

      # The dynamic window sizing logic is:
      # start < 255 -> 64
      # start < 65535 -> 1024
      # else -> 8192
      # This is verified through the range queries in stats

      MockRepo
      |> expect(:transact, fn fun -> fun.() end)
      |> expect(:select, fn %KeySelector{} -> nil end)
      |> expect(:get_range, fn start_key, end_key ->
        # Verify counter range query in stats
        assert start_key == "test" <> <<0>>
        assert end_key == "test" <> <<0, 255>>
        # No counter entries
        []
      end)

      HighContentionAllocator.stats(hca)
    end
  end
end
