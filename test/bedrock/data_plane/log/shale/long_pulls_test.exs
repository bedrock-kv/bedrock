defmodule Bedrock.DataPlane.Log.Shale.LongPullsTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Log.Shale.LongPulls

  describe "normalize_timeout_to_ms/1" do
    test "normalizes timeout to within the range" do
      assert LongPulls.normalize_timeout_to_ms(5) == 10
      assert LongPulls.normalize_timeout_to_ms(15_000) == 10_000
      assert LongPulls.normalize_timeout_to_ms(500) == 500
    end

    test "uses default timeout for non-integer values" do
      # Test the catchall clause for invalid timeout types
      assert LongPulls.normalize_timeout_to_ms(nil) == 5000
      assert LongPulls.normalize_timeout_to_ms(:infinity) == 5000
      assert LongPulls.normalize_timeout_to_ms("5000") == 5000
    end
  end

  describe "notify_waiting_pullers/3" do
    test "notifies pullers and removes them from waiting list" do
      reply_to_fn = fn _ -> send(self(), :notified) end
      waiting_pullers = %{1 => [{0, reply_to_fn, []}]}
      version = 1
      transaction = :transaction

      remaining_waiting_pullers =
        LongPulls.notify_waiting_pullers(waiting_pullers, version, transaction)

      assert_received :notified
      assert remaining_waiting_pullers == %{}
    end

    test "does nothing if no pullers are waiting" do
      waiting_pullers = %{}
      version = 1
      transaction = :transaction

      remaining_waiting_pullers =
        LongPulls.notify_waiting_pullers(waiting_pullers, version, transaction)

      assert remaining_waiting_pullers == %{}
    end
  end

  describe "try_to_add_to_waiting_pullers/4" do
    test "returns an error if not willing to wait" do
      waiting_pullers = %{}
      monotonic_now = :erlang.monotonic_time(:millisecond)
      reply_to_fn = fn _ -> :ok end
      from_version = 1
      opts = []

      assert {:error, :version_too_new} =
               LongPulls.try_to_add_to_waiting_pullers(
                 waiting_pullers,
                 monotonic_now,
                 reply_to_fn,
                 from_version,
                 opts
               )
    end

    test "adds puller to the waiting pullers map" do
      waiting_pullers = %{}
      monotonic_now = :erlang.monotonic_time(:millisecond)
      reply_to_fn = fn _ -> :ok end
      from_version = 1
      opts = [willing_to_wait_in_ms: 1000]

      assert {:ok, updated_waiting_pullers} =
               LongPulls.try_to_add_to_waiting_pullers(
                 waiting_pullers,
                 monotonic_now,
                 reply_to_fn,
                 from_version,
                 opts
               )

      assert Map.has_key?(updated_waiting_pullers, from_version)

      # Check the entry structure with pattern matching
      assert [{deadline, ^reply_to_fn, []}] = Map.get(updated_waiting_pullers, from_version)
      # Deadline should be in the future
      assert deadline > monotonic_now
    end

    test "maintains entries sorted by deadline when adding multiple pullers" do
      waiting_pullers = %{}
      monotonic_now = :erlang.monotonic_time(:millisecond)
      reply_to_fn = fn _ -> :ok end
      from_version = 1
      opts = [willing_to_wait_in_ms: 1000]

      assert {:ok, updated_waiting_pullers} =
               LongPulls.try_to_add_to_waiting_pullers(
                 waiting_pullers,
                 monotonic_now,
                 reply_to_fn,
                 from_version,
                 opts
               )

      # Verify first entry with pattern matching
      assert [{deadline, ^reply_to_fn, []}] = Map.get(updated_waiting_pullers, from_version)
      expected_deadline = monotonic_now + 1000
      assert abs(deadline - expected_deadline) <= 100

      # Add second puller
      reply_to_fn_2 = fn _ -> :ok end
      monotonic_now_2 = monotonic_now + :rand.uniform(1000)
      opts_2 = [willing_to_wait_in_ms: 1000, some_other_option: :value]

      assert {:ok, updated_waiting_pullers} =
               LongPulls.try_to_add_to_waiting_pullers(
                 updated_waiting_pullers,
                 monotonic_now_2,
                 reply_to_fn_2,
                 from_version,
                 opts_2
               )

      # Verify entries are sorted by deadline with pattern matching
      assert [{first_deadline, _, _}, {second_deadline, _, _}] =
               Map.get(updated_waiting_pullers, from_version)

      assert length(Map.get(updated_waiting_pullers, from_version)) == 2
      assert first_deadline <= second_deadline
    end
  end

  describe "process_expired_deadlines_for_waiting_pullers/2" do
    test "removes expired pullers and notifies them" do
      test_pid = self()
      reply_to_fn = fn _ -> send(test_pid, :notified) end

      # Create entries with deadlines in the past and future relative to current time
      now = Bedrock.Internal.Time.monotonic_now_in_ms()
      # 1 second ago (expired)
      expired_deadline = now - 1000
      # 1 second in future (not expired)
      future_deadline = now + 1000

      waiting_pullers = %{
        1 => [{expired_deadline, reply_to_fn, []}],
        2 => [{future_deadline, reply_to_fn, []}]
      }

      updated_waiting_pullers =
        LongPulls.process_expired_deadlines_for_waiting_pullers(waiting_pullers, now)

      assert_received :notified
      assert %{2 => [{^future_deadline, ^reply_to_fn, []}]} = updated_waiting_pullers
    end
  end

  describe "determine_timeout_for_next_puller_deadline/2" do
    test "returns the correct timeout" do
      # Use deterministic time for predictable test results
      current_time = 1_000_000

      waiting_pullers = %{
        1 => [{current_time + 1000, fn _ -> :ok end, []}],
        2 => [{current_time + 2000, fn _ -> :ok end, []}]
      }

      # Should return exactly 1000ms (time until first deadline)
      assert LongPulls.determine_timeout_for_next_puller_deadline(waiting_pullers, current_time) == 1000
    end

    test "returns nil if there are no pullers" do
      waiting_pullers = %{}
      now = :erlang.monotonic_time(:millisecond)

      assert LongPulls.determine_timeout_for_next_puller_deadline(waiting_pullers, now) == nil
    end
  end
end
