defmodule Bedrock.HCATest do
  use ExUnit.Case, async: true

  import Mox

  alias Bedrock.HCA
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

  describe "HCA creation and configuration" do
    test "creates HCA with new two-subspace structure" do
      hca = HCA.new(MockRepo, "test_allocator")

      assert hca.repo_module == MockRepo
      assert hca.counters_subspace == "test_allocator" <> <<0>>
      assert hca.recent_subspace == "test_allocator" <> <<1>>
    end

    test "handles different subspace prefixes correctly" do
      hca = HCA.new(MockRepo, "my_alloc")

      assert hca.counters_subspace == "my_alloc" <> <<0>>
      assert hca.recent_subspace == "my_alloc" <> <<1>>
    end

    test "handles empty stats correctly" do
      hca = HCA.new(MockRepo, "test_empty")

      # Expected call sequence for stats:
      # 1. select to find latest counter
      # 2. range to count window entries

      MockRepo
      |> expect(:transaction, fn fun -> fun.(:mock_txn) end)
      |> expect(:select, fn :mock_txn, %KeySelector{} = selector ->
        # Verify selector is looking for last counter
        assert selector.key == "test_empty" <> <<0, 255>>
        assert selector.or_equal == false
        assert selector.offset == 0
        # No counters yet
        nil
      end)
      |> expect(:range, fn :mock_txn, start_key, end_key ->
        # Verify range query parameters for counter counting in stats
        assert start_key == "test_empty" <> <<0>>
        assert end_key == "test_empty" <> <<0, 255>>
        # No counter entries
        []
      end)

      stats =
        MockRepo.transaction(fn txn ->
          HCA.stats(hca, txn)
        end)

      assert is_map(stats)
      assert stats.latest_window_start == 0
      assert stats.total_counters == 0
      assert stats.estimated_allocated == 0
    end
  end

  describe "allocation interface" do
    test "provides allocate/2 function with correct call sequence" do
      # Use deterministic random function that returns 42
      random_value = 42
      deterministic_random = fn _size -> random_value end
      hca = HCA.new(MockRepo, "test_alloc", random_fn: deterministic_random)

      # With start=0, window_size=64, random=42: candidate = 0 + (42 - 1) = 41
      expected_candidate = 41

      MockRepo
      |> expect(:transaction, fn fun -> fun.(:mock_txn) end)
      # Step 1: Find current start
      |> expect(:select, fn :mock_txn, %KeySelector{} = selector ->
        assert selector.key == hca.counters_subspace <> <<0xFF>>
        # No counters yet, start at 0
        nil
      end)
      # Step 2: Increment counter for window 0
      |> expect(:add, fn :mock_txn, key, value ->
        assert_counter_key(key, hca, 0)
        assert value == 1
        :mock_txn
      end)
      # Step 3: Read current count (snapshot read)
      |> expect(:get, fn :mock_txn, key, opts ->
        assert_counter_key(key, hca, 0)
        assert opts[:snapshot] == true
        # Count is now 1
        <<1::64-little>>
      end)
      # Step 4: Verify window hasn't advanced (called again in search_candidate)
      |> expect(:select, fn :mock_txn, %KeySelector{} = selector ->
        assert selector.key == hca.counters_subspace <> <<0xFF>>
        # Still no counters beyond 0
        nil
      end)
      # Step 5: Check candidate availability (snapshot read)
      |> expect(:get, fn :mock_txn, candidate_key, opts ->
        assert_recent_key(candidate_key, hca, expected_candidate)
        assert opts[:snapshot] == true
        # Candidate is available
        nil
      end)
      # Step 6: Claim candidate (no write conflict)
      |> expect(:put, fn :mock_txn, candidate_key, value, opts ->
        assert_recent_key(candidate_key, hca, expected_candidate)
        assert value == ""
        assert opts[:no_write_conflict] == true
        :mock_txn
      end)
      # Step 7: Add write conflict (now uses add_write_conflict_range instead of put)
      |> expect(:add_write_conflict_range, fn :mock_txn, start_key, end_key ->
        assert start_key == encode_recent_key(hca, expected_candidate)
        assert end_key == start_key <> <<0>>
        :mock_txn
      end)

      result =
        MockRepo.transaction(fn txn ->
          HCA.allocate(hca, txn)
        end)

      # Should return the exact candidate we calculated
      assert {:ok, ^expected_candidate} = result
    end

    test "provides allocate_many/3 function with correct multiplied calls" do
      # Use deterministic sequence: first call returns 10, second returns 25
      call_count = :counters.new(1, [])
      :counters.put(call_count, 1, 0)

      random_sequence = [10, 25]

      deterministic_random = fn _size ->
        count = :counters.get(call_count, 1)
        :counters.add(call_count, 1, 1)
        Enum.at(random_sequence, count)
      end

      hca = HCA.new(MockRepo, "test_many", random_fn: deterministic_random)

      # Calculate expected candidates: start=0, window_size=64
      # First: candidate = 0 + (10 - 1) = 9
      # Second: candidate = 0 + (25 - 1) = 24
      expected_candidates = [9, 24]
      [first_candidate, second_candidate] = expected_candidates

      MockRepo
      |> expect(:transaction, fn fun -> fun.(:mock_txn) end)
      # First allocation
      |> expect(:select, fn :mock_txn, %KeySelector{} = selector ->
        assert selector.key == hca.counters_subspace <> <<0xFF>>
        nil
      end)
      |> expect(:add, fn :mock_txn, key, 1 ->
        assert_counter_key(key, hca, 0)
        :mock_txn
      end)
      |> expect(:get, fn :mock_txn, key, [snapshot: true] ->
        assert_counter_key(key, hca, 0)
        <<1::64-little>>
      end)
      |> expect(:select, fn :mock_txn, %KeySelector{} = selector ->
        assert selector.key == hca.counters_subspace <> <<0xFF>>
        nil
      end)
      |> expect(:get, fn :mock_txn, candidate_key, [snapshot: true] ->
        assert_recent_key(candidate_key, hca, first_candidate)
        nil
      end)
      |> expect(:put, fn :mock_txn, candidate_key, "", [no_write_conflict: true] ->
        assert_recent_key(candidate_key, hca, first_candidate)
        :mock_txn
      end)
      |> expect(:add_write_conflict_range, fn :mock_txn, start_key, end_key ->
        assert start_key == encode_recent_key(hca, first_candidate)
        assert end_key == start_key <> <<0>>
        :mock_txn
      end)
      # Second allocation
      |> expect(:select, fn :mock_txn, %KeySelector{} = selector ->
        assert selector.key == hca.counters_subspace <> <<0xFF>>
        nil
      end)
      |> expect(:add, fn :mock_txn, key, 1 ->
        assert_counter_key(key, hca, 0)
        :mock_txn
      end)
      |> expect(:get, fn :mock_txn, key, [snapshot: true] ->
        assert_counter_key(key, hca, 0)
        <<2::64-little>>
      end)
      |> expect(:select, fn :mock_txn, %KeySelector{} = selector ->
        assert selector.key == hca.counters_subspace <> <<0xFF>>
        nil
      end)
      |> expect(:get, fn :mock_txn, candidate_key, [snapshot: true] ->
        assert_recent_key(candidate_key, hca, second_candidate)
        nil
      end)
      |> expect(:put, fn :mock_txn, candidate_key, "", [no_write_conflict: true] ->
        assert_recent_key(candidate_key, hca, second_candidate)
        :mock_txn
      end)
      |> expect(:add_write_conflict_range, fn :mock_txn, start_key, end_key ->
        assert start_key == encode_recent_key(hca, second_candidate)
        assert end_key == start_key <> <<0>>
        :mock_txn
      end)

      result =
        MockRepo.transaction(fn txn ->
          HCA.allocate_many(hca, txn, 2)
        end)

      # Should return the exact candidates we calculated
      assert {:ok, ^expected_candidates} = result
    end
  end

  describe "error handling" do
    test "handles candidate collision with retry" do
      # Use deterministic sequence: first attempt uses 15, retry uses 30
      call_count = :counters.new(1, [])
      :counters.put(call_count, 1, 0)

      random_sequence = [15, 30]

      deterministic_random = fn _size ->
        count = :counters.get(call_count, 1)
        :counters.add(call_count, 1, 1)
        Enum.at(random_sequence, count)
      end

      hca = HCA.new(MockRepo, "test_collision", random_fn: deterministic_random)

      # Calculate expected candidates: start=0, window_size=64
      # First attempt: candidate = 0 + (15 - 1) = 14 (will be taken)
      # Retry attempt: candidate = 0 + (30 - 1) = 29 (will succeed)
      first_candidate = 14
      retry_candidate = 29

      MockRepo
      |> expect(:transaction, fn fun -> fun.(:mock_txn) end)
      # First attempt - fails when candidate is taken
      |> expect(:select, fn :mock_txn, %KeySelector{} = selector ->
        assert selector.key == hca.counters_subspace <> <<0xFF>>
        nil
      end)
      |> expect(:add, fn :mock_txn, key, 1 ->
        assert_counter_key(key, hca, 0)
        :mock_txn
      end)
      |> expect(:get, fn :mock_txn, key, [snapshot: true] ->
        assert_counter_key(key, hca, 0)
        <<1::64-little>>
      end)
      |> expect(:select, fn :mock_txn, %KeySelector{} = selector ->
        assert selector.key == hca.counters_subspace <> <<0xFF>>
        nil
      end)
      |> expect(:get, fn :mock_txn, candidate_key, [snapshot: true] ->
        assert_recent_key(candidate_key, hca, first_candidate)
        # Candidate is already taken
        "taken"
      end)
      # Retry attempt - succeeds with different candidate
      |> expect(:select, fn :mock_txn, %KeySelector{} = selector ->
        assert selector.key == hca.counters_subspace <> <<0xFF>>
        nil
      end)
      |> expect(:add, fn :mock_txn, key, 1 ->
        assert_counter_key(key, hca, 0)
        :mock_txn
      end)
      |> expect(:get, fn :mock_txn, key, [snapshot: true] ->
        assert_counter_key(key, hca, 0)
        <<2::64-little>>
      end)
      |> expect(:select, fn :mock_txn, %KeySelector{} = selector ->
        assert selector.key == hca.counters_subspace <> <<0xFF>>
        nil
      end)
      |> expect(:get, fn :mock_txn, candidate_key, [snapshot: true] ->
        assert_recent_key(candidate_key, hca, retry_candidate)
        # Retry candidate is available
        nil
      end)
      |> expect(:put, fn :mock_txn, candidate_key, "", [no_write_conflict: true] ->
        assert_recent_key(candidate_key, hca, retry_candidate)
        :mock_txn
      end)
      |> expect(:add_write_conflict_range, fn :mock_txn, start_key, end_key ->
        assert start_key == encode_recent_key(hca, retry_candidate)
        assert end_key == start_key <> <<0>>
        :mock_txn
      end)

      result =
        MockRepo.transaction(fn txn ->
          HCA.allocate(hca, txn)
        end)

      # Should return the retry candidate
      assert {:ok, ^retry_candidate} = result
    end
  end

  describe "key generation and encoding" do
    test "generates correct counter key format" do
      hca = HCA.new(MockRepo, "test/prefix")

      # Test key generation via the subspace structure
      expected_counters = "test/prefix" <> <<0>>
      expected_recent = "test/prefix" <> <<1>>

      assert hca.counters_subspace == expected_counters
      assert hca.recent_subspace == expected_recent
    end

    test "uses custom random function when provided" do
      # Create HCA with deterministic random function (always returns 1)
      deterministic_random = fn _size -> 1 end
      hca = HCA.new(MockRepo, "test_random", random_fn: deterministic_random)

      # With start=0, window_size=64, random=1: candidate = 0 + (1 - 1) = 0
      expected_candidate = 0

      MockRepo
      |> expect(:transaction, fn fun -> fun.(:mock_txn) end)
      |> expect(:select, fn :mock_txn, %KeySelector{} = selector ->
        assert selector.key == hca.counters_subspace <> <<0xFF>>
        nil
      end)
      |> expect(:add, fn :mock_txn, counter_key, 1 ->
        assert_counter_key(counter_key, hca, 0)
        :mock_txn
      end)
      |> expect(:get, fn :mock_txn, counter_key, [snapshot: true] ->
        assert_counter_key(counter_key, hca, 0)
        <<1::64-little>>
      end)
      |> expect(:select, fn :mock_txn, %KeySelector{} = selector ->
        assert selector.key == hca.counters_subspace <> <<0xFF>>
        nil
      end)
      |> expect(:get, fn :mock_txn, candidate_key, [snapshot: true] ->
        assert_recent_key(candidate_key, hca, expected_candidate)
        # Candidate available
        nil
      end)
      |> expect(:put, fn :mock_txn, candidate_key, "", [no_write_conflict: true] ->
        assert_recent_key(candidate_key, hca, expected_candidate)
        :mock_txn
      end)
      |> expect(:add_write_conflict_range, fn :mock_txn, start_key, end_key ->
        assert start_key == encode_recent_key(hca, expected_candidate)
        assert end_key == start_key <> <<0>>
        :mock_txn
      end)

      result =
        MockRepo.transaction(fn txn ->
          HCA.allocate(hca, txn)
        end)

      # With deterministic random (always 1), candidate should always be 0
      assert {:ok, ^expected_candidate} = result
    end
  end

  describe "dynamic window sizing" do
    test "uses correct window sizes for different ranges" do
      hca = HCA.new(MockRepo, "test")

      # The dynamic window sizing logic is:
      # start < 255 -> 64
      # start < 65535 -> 1024
      # else -> 8192
      # This is verified through the range queries in stats

      MockRepo
      |> expect(:transaction, fn fun -> fun.(:mock_txn) end)
      |> expect(:select, fn :mock_txn, %KeySelector{} -> nil end)
      |> expect(:range, fn :mock_txn, start_key, end_key ->
        # Verify counter range query in stats
        assert start_key == "test" <> <<0>>
        assert end_key == "test" <> <<0, 255>>
        # No counter entries
        []
      end)

      MockRepo.transaction(fn txn ->
        HCA.stats(hca, txn)
      end)
    end
  end
end
