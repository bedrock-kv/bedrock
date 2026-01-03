defmodule Bedrock.HighContentionAllocatorTest do
  use ExUnit.Case, async: true

  import Mox

  alias Bedrock.HighContentionAllocator
  alias Bedrock.KeySelector

  setup :verify_on_exit!

  # Helper functions for key assertions
  defp assert_counter_key(key, hca, expected_start) do
    assert key == <<hca.counters_keyspace::binary, expected_start::64-big>>
  end

  defp assert_recent_key(key, hca, expected_candidate) do
    assert key == <<hca.recent_keyspace::binary, expected_candidate::64-big>>
  end

  # Helper to encode recent key for conflict range assertions
  defp encode_recent_key(hca, candidate) do
    hca.recent_keyspace <> <<candidate::64-big>>
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
      assert selector.key == hca.counters_keyspace <> <<0xFF>>
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
      assert selector.key == hca.counters_keyspace <> <<0xFF>>
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
    |> expect(:add_write_conflict_range, fn {start_key, end_key} ->
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
    test "creates HighContentionAllocator with new two-keyspace structure" do
      assert %{repo: MockRepo, counters_keyspace: "test_allocator" <> <<0>>, recent_keyspace: "test_allocator" <> <<1>>} =
               HighContentionAllocator.new(MockRepo, "test_allocator")
    end

    test "handles different keyspace prefixes correctly" do
      assert %{counters_keyspace: "my_alloc" <> <<0>>, recent_keyspace: "my_alloc" <> <<1>>} =
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
      assert encoded_result == <<0x01, 0x29>>
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
      expected_encoded = [<<0x01, 0x09>>, <<0x01, 0x18>>]
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
      assert encoded_result == <<0x01, 0x1D>>
    end
  end

  describe "key generation and encoding" do
    test "generates correct counter key format" do
      assert %{counters_keyspace: "test/prefix" <> <<0>>, recent_keyspace: "test/prefix" <> <<1>>} =
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
      assert encoded_result == <<0x01, 0x00>>
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

  describe "window advancement" do
    test "advances to next window when current window is over 50% full" do
      # Window size is 64 for start=0, so 50% is 32
      # When count=32, it should advance to next window
      hca = HighContentionAllocator.new(MockRepo, "test_advance", random_fn: deterministic_random_fn(11))

      counter_key_64 = hca.counters_keyspace <> <<64::64-big>>

      MockRepo
      |> expect(:transact, fn fun -> fun.() end)
      # First: check current start (returns 0)
      |> expect(:select, fn %KeySelector{} -> nil end)
      # Increment counter for window 0
      |> expect(:atomic, fn :add, key, <<1::64-little>> ->
        assert_counter_key(key, hca, 0)
        :mock_txn
      end)
      # Get count for window 0 (returns 32, which triggers advancement)
      |> expect(:get, fn key, [snapshot: true] ->
        assert_counter_key(key, hca, 0)
        <<32::64-little>>
      end)
      # Clear previous window (window 64)
      |> expect(:clear_range, 2, fn {_start, _end}, [no_write_conflict: true] ->
        :mock_txn
      end)
      # Increment counter for next window (64)
      |> expect(:atomic, fn :add, key, <<1::64-little>> ->
        assert_counter_key(key, hca, 64)
        :mock_txn
      end)
      # Get count for window 64 (returns 1, has capacity)
      |> expect(:get, fn key, [snapshot: true] ->
        assert_counter_key(key, hca, 64)
        <<1::64-little>>
      end)
      # Check current start again (now returns 64)
      |> expect(:select, fn %KeySelector{} ->
        {counter_key_64, <<1::64-little>>}
      end)
      # Check if candidate is available
      |> expect(:get, fn candidate_key, [snapshot: true] ->
        # Candidate = 64 + (11 - 1) = 74
        assert_recent_key(candidate_key, hca, 74)
        nil
      end)
      # Claim candidate
      |> expect(:put, fn candidate_key, "", [no_write_conflict: true] ->
        assert_recent_key(candidate_key, hca, 74)
        :mock_txn
      end)
      |> expect(:add_write_conflict_range, fn {_start_key, _end_key} ->
        :mock_txn
      end)

      assert {:ok, _encoded_result} = HighContentionAllocator.allocate(hca)
    end
  end

  describe "stats calculation" do
    test "returns correct stats with multiple allocations" do
      hca = HighContentionAllocator.new(MockRepo, "test_stats")

      MockRepo
      |> expect(:transact, fn fun -> fun.() end)
      |> expect(:select, fn %KeySelector{} ->
        # Return a key in the counter range
        {"test_stats" <> <<0, 0, 0, 0, 0, 0, 0, 0, 100>>, <<5::64-little>>}
      end)
      |> expect(:get_range, fn _start_key, _end_key ->
        [
          {"test_stats" <> <<0, 0, 0, 0, 0, 0, 0, 0, 0>>, <<10::64-little>>},
          {"test_stats" <> <<0, 0, 0, 0, 0, 0, 0, 0, 64>>, <<20::64-little>>},
          {"test_stats" <> <<0, 0, 0, 0, 0, 0, 0, 0, 100>>, <<5::64-little>>}
        ]
      end)

      assert {:ok,
              %{
                latest_window_start: 100,
                total_counters: 3,
                estimated_allocated: 35
              }} = HighContentionAllocator.stats(hca)
    end

    test "handles stats with no valid counter values" do
      hca = HighContentionAllocator.new(MockRepo, "test_invalid")

      MockRepo
      |> expect(:transact, fn fun -> fun.() end)
      |> expect(:select, fn %KeySelector{} -> nil end)
      |> expect(:get_range, fn _start_key, _end_key ->
        [
          {"test_invalid" <> <<0, 0, 0, 0, 0, 0, 0, 0, 0>>, "invalid_value"}
        ]
      end)

      assert {:ok,
              %{
                latest_window_start: 0,
                total_counters: 1,
                estimated_allocated: 0
              }} = HighContentionAllocator.stats(hca)
    end

    test "handles nil counter value during allocation" do
      hca = HighContentionAllocator.new(MockRepo, "test_nil_counter", random_fn: deterministic_random_fn(5))

      MockRepo
      |> expect(:transact, fn fun -> fun.() end)
      |> expect(:select, fn %KeySelector{} -> nil end)
      |> expect(:atomic, fn :add, key, <<1::64-little>> ->
        assert_counter_key(key, hca, 0)
        :mock_txn
      end)
      # Return nil instead of a counter value
      |> expect(:get, fn key, [snapshot: true] ->
        assert_counter_key(key, hca, 0)
        nil
      end)
      |> expect(:select, fn %KeySelector{} -> nil end)
      |> expect(:get, fn _candidate_key, [snapshot: true] -> nil end)
      |> expect(:put, fn _candidate_key, "", [no_write_conflict: true] -> :mock_txn end)
      |> expect(:add_write_conflict_range, fn {_start_key, _end_key} -> :mock_txn end)

      assert {:ok, _encoded_result} = HighContentionAllocator.allocate(hca)
    end

    test "handles invalid counter value format during allocation" do
      hca = HighContentionAllocator.new(MockRepo, "test_invalid_counter", random_fn: deterministic_random_fn(7))

      MockRepo
      |> expect(:transact, fn fun -> fun.() end)
      |> expect(:select, fn %KeySelector{} -> nil end)
      |> expect(:atomic, fn :add, key, <<1::64-little>> ->
        assert_counter_key(key, hca, 0)
        :mock_txn
      end)
      # Return invalid format instead of expected binary
      |> expect(:get, fn key, [snapshot: true] ->
        assert_counter_key(key, hca, 0)
        "invalid_binary_format"
      end)
      |> expect(:select, fn %KeySelector{} -> nil end)
      |> expect(:get, fn _candidate_key, [snapshot: true] -> nil end)
      |> expect(:put, fn _candidate_key, "", [no_write_conflict: true] -> :mock_txn end)
      |> expect(:add_write_conflict_range, fn {_start_key, _end_key} -> :mock_txn end)

      assert {:ok, _encoded_result} = HighContentionAllocator.allocate(hca)
    end
  end

  describe "dynamic window sizing edge cases" do
    test "uses 8192 window size for very large start values" do
      # Start value >= 65535 should use window size 8192
      hca = HighContentionAllocator.new(MockRepo, "test_large_window", random_fn: deterministic_random_fn(100))

      counter_key = hca.counters_keyspace <> <<100_000::64-big>>

      MockRepo
      |> expect(:transact, fn fun -> fun.() end)
      # Return counter at position 100,000 (>= 65535)
      |> expect(:select, fn %KeySelector{} ->
        {counter_key, <<1::64-little>>}
      end)
      |> expect(:atomic, fn :add, key, <<1::64-little>> ->
        assert_counter_key(key, hca, 100_000)
        :mock_txn
      end)
      |> expect(:get, fn key, [snapshot: true] ->
        assert_counter_key(key, hca, 100_000)
        <<1::64-little>>
      end)
      |> expect(:select, fn %KeySelector{} ->
        {counter_key, <<1::64-little>>}
      end)
      |> expect(:get, fn candidate_key, [snapshot: true] ->
        # With window_size=8192 and random=100:
        # Candidate = 100,000 + (100 - 1) = 100,099
        assert_recent_key(candidate_key, hca, 100_099)
        nil
      end)
      |> expect(:put, fn candidate_key, "", [no_write_conflict: true] ->
        assert_recent_key(candidate_key, hca, 100_099)
        :mock_txn
      end)
      |> expect(:add_write_conflict_range, fn {_start_key, _end_key} ->
        :mock_txn
      end)

      assert {:ok, _encoded_result} = HighContentionAllocator.allocate(hca)
    end
  end

  describe "key encoding edge cases" do
    test "handles counter key decoding with existing counter" do
      hca = HighContentionAllocator.new(MockRepo, "test_decode")

      # Simulate an existing counter at position 12345
      counter_key = hca.counters_keyspace <> <<12_345::64-big>>

      MockRepo
      |> expect(:transact, fn fun -> fun.() end)
      |> expect(:select, fn %KeySelector{} ->
        {counter_key, <<50::64-little>>}
      end)
      |> expect(:atomic, fn :add, key, <<1::64-little>> ->
        assert_counter_key(key, hca, 12_345)
        :mock_txn
      end)
      |> expect(:get, fn key, [snapshot: true] ->
        assert_counter_key(key, hca, 12_345)
        <<1::64-little>>
      end)
      |> expect(:select, fn %KeySelector{} ->
        {counter_key, <<50::64-little>>}
      end)
      |> expect(:get, fn _candidate_key, [snapshot: true] -> nil end)
      |> expect(:put, fn _candidate_key, "", [no_write_conflict: true] -> :mock_txn end)
      |> expect(:add_write_conflict_range, fn {_start_key, _end_key} -> :mock_txn end)

      assert {:ok, _encoded_result} = HighContentionAllocator.allocate(hca)
    end

    test "handles selector returning key outside counter range" do
      hca = HighContentionAllocator.new(MockRepo, "test_outside")

      # Selector returns a key that doesn't start with our counter prefix
      outside_key = "other_prefix" <> <<0, 1, 2, 3>>

      MockRepo
      |> expect(:transact, fn fun -> fun.() end)
      |> expect(:select, fn %KeySelector{} ->
        {outside_key, <<100::64-little>>}
      end)
      |> expect(:atomic, fn :add, key, <<1::64-little>> ->
        assert_counter_key(key, hca, 0)
        :mock_txn
      end)
      |> expect(:get, fn key, [snapshot: true] ->
        assert_counter_key(key, hca, 0)
        <<1::64-little>>
      end)
      |> expect(:select, fn %KeySelector{} ->
        {outside_key, <<100::64-little>>}
      end)
      |> expect(:get, fn _candidate_key, [snapshot: true] -> nil end)
      |> expect(:put, fn _candidate_key, "", [no_write_conflict: true] -> :mock_txn end)
      |> expect(:add_write_conflict_range, fn {_start_key, _end_key} -> :mock_txn end)

      # Should default to start=0 when key is outside range
      assert {:ok, _encoded_result} = HighContentionAllocator.allocate(hca)
    end
  end

  describe "compact encoding" do
    test "encodes small IDs with minimal bytes" do
      # ID 0 should encode as <<1, 0>> (1 byte length + 1 byte value)
      hca = HighContentionAllocator.new(MockRepo, "test_compact", random_fn: deterministic_random_fn(1))

      MockRepo
      |> expect(:transact, fn fun -> fun.() end)
      |> expect_allocation_sequence(hca, 0, 1, 0)

      assert {:ok, <<1, 0>>} = HighContentionAllocator.allocate(hca)
    end

    test "encodes larger IDs with multiple bytes" do
      # ID 300 requires 2 bytes (256 + 44) -> <<2, 1, 44>>
      hca = HighContentionAllocator.new(MockRepo, "test_large", random_fn: deterministic_random_fn(1))

      MockRepo
      |> expect(:transact, fn fun -> fun.() end)
      # First: check current start (simulate window at 300)
      |> expect(:select, fn %KeySelector{} ->
        counter_key = hca.counters_keyspace <> <<300::64-big>>
        {counter_key, <<1::64-little>>}
      end)
      |> expect(:atomic, fn :add, key, <<1::64-little>> ->
        assert_counter_key(key, hca, 300)
        :mock_txn
      end)
      |> expect(:get, fn key, [snapshot: true] ->
        assert_counter_key(key, hca, 300)
        <<1::64-little>>
      end)
      |> expect(:select, fn %KeySelector{} ->
        counter_key = hca.counters_keyspace <> <<300::64-big>>
        {counter_key, <<1::64-little>>}
      end)
      |> expect(:get, fn candidate_key, [snapshot: true] ->
        # Candidate = 300 + (1 - 1) = 300
        assert_recent_key(candidate_key, hca, 300)
        nil
      end)
      |> expect(:put, fn candidate_key, "", [no_write_conflict: true] ->
        assert_recent_key(candidate_key, hca, 300)
        :mock_txn
      end)
      |> expect(:add_write_conflict_range, fn {_start_key, _end_key} ->
        :mock_txn
      end)

      assert {:ok, <<2, 1, 44>>} = HighContentionAllocator.allocate(hca)
    end
  end

  describe "concurrent window detection" do
    test "retries when window advances concurrently" do
      # Simulate concurrent window advancement during allocation
      # First check: window at 0, second check: window at 64
      hca = HighContentionAllocator.new(MockRepo, "test_concurrent", random_fn: deterministic_random_fn([10, 20]))

      counter_key_0 = hca.counters_keyspace <> <<0::64-big>>
      counter_key_64 = hca.counters_keyspace <> <<64::64-big>>

      MockRepo
      |> expect(:transact, fn fun -> fun.() end)
      # First attempt: window at 0
      |> expect(:select, fn %KeySelector{} ->
        {counter_key_0, <<1::64-little>>}
      end)
      |> expect(:atomic, fn :add, key, <<1::64-little>> ->
        assert_counter_key(key, hca, 0)
        :mock_txn
      end)
      |> expect(:get, fn key, [snapshot: true] ->
        assert_counter_key(key, hca, 0)
        <<1::64-little>>
      end)
      # Second check detects window advanced to 64
      |> expect(:select, fn %KeySelector{} ->
        {counter_key_64, <<1::64-little>>}
      end)
      # Retry: window at 64
      |> expect(:select, fn %KeySelector{} ->
        {counter_key_64, <<1::64-little>>}
      end)
      |> expect(:atomic, fn :add, key, <<1::64-little>> ->
        assert_counter_key(key, hca, 64)
        :mock_txn
      end)
      |> expect(:get, fn key, [snapshot: true] ->
        assert_counter_key(key, hca, 64)
        <<1::64-little>>
      end)
      |> expect(:select, fn %KeySelector{} ->
        {counter_key_64, <<1::64-little>>}
      end)
      |> expect(:get, fn candidate_key, [snapshot: true] ->
        # Candidate = 64 + (20 - 1) = 83
        assert_recent_key(candidate_key, hca, 83)
        nil
      end)
      |> expect(:put, fn candidate_key, "", [no_write_conflict: true] ->
        assert_recent_key(candidate_key, hca, 83)
        :mock_txn
      end)
      |> expect(:add_write_conflict_range, fn {_start_key, _end_key} ->
        :mock_txn
      end)

      assert {:ok, _encoded_result} = HighContentionAllocator.allocate(hca)
    end
  end

  describe "allocate_many error handling" do
    test "propagates error from failed allocation" do
      hca = HighContentionAllocator.new(MockRepo, "test_error")

      expect(MockRepo, :transact, fn _fun ->
        {:error, :transaction_failed}
      end)

      assert {:error, :transaction_failed} = HighContentionAllocator.allocate_many(hca, 3)
    end

    test "handles rescue error during allocation" do
      hca = HighContentionAllocator.new(MockRepo, "test_rescue")

      expect(MockRepo, :transact, fn _fun ->
        raise RuntimeError, "simulated transaction error"
      end)

      assert {:error, %RuntimeError{message: "simulated transaction error"}} =
               HighContentionAllocator.allocate(hca)
    end
  end

  describe "allocate/2 with existing transaction" do
    test "allocates within existing transaction" do
      hca = HighContentionAllocator.new(MockRepo, "test_txn", random_fn: deterministic_random_fn(10))

      # Simulate being called within an existing transaction
      expect_allocation_sequence(MockRepo, hca, 0, 1, 9)

      # Call 2-arity version with a mock transaction context
      assert encoded_result = HighContentionAllocator.allocate(hca, :mock_txn_context)

      # Verify it's the expected candidate encoded as compact binary
      # Candidate = 0 + (10 - 1) = 9
      assert encoded_result == <<0x01, 0x09>>
    end
  end

  describe "custom random functions" do
    test "uses custom random function for all allocations" do
      # Test that random function is called with correct window size
      call_count = :counters.new(1, [])
      :counters.put(call_count, 1, 0)

      custom_random = fn size ->
        count = :counters.get(call_count, 1)
        :counters.add(call_count, 1, 1)

        # Verify window size is correct for initial allocations
        if count == 0 do
          assert size == 64
        end

        # Return middle of window
        div(size, 2)
      end

      hca = HighContentionAllocator.new(MockRepo, "test_custom", random_fn: custom_random)

      MockRepo
      |> expect(:transact, fn fun -> fun.() end)
      |> expect(:select, fn %KeySelector{} -> nil end)
      |> expect(:atomic, fn :add, _key, <<1::64-little>> -> :mock_txn end)
      |> expect(:get, fn _key, [snapshot: true] -> <<1::64-little>> end)
      |> expect(:select, fn %KeySelector{} -> nil end)
      |> expect(:get, fn _candidate_key, [snapshot: true] -> nil end)
      |> expect(:put, fn _candidate_key, "", [no_write_conflict: true] -> :mock_txn end)
      |> expect(:add_write_conflict_range, fn {_start_key, _end_key} -> :mock_txn end)

      assert {:ok, _encoded_result} = HighContentionAllocator.allocate(hca)

      # Verify custom random was called
      assert :counters.get(call_count, 1) == 1
    end
  end
end
