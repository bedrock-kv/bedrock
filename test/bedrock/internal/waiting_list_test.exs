defmodule Bedrock.Internal.WaitingListTest do
  use ExUnit.Case, async: true

  alias Bedrock.Internal.WaitingList

  describe "insert/5" do
    test "adds first entry to empty map" do
      map = %{}
      reply_fn = fn _ -> :ok end
      time_fn = fn -> 1000 end

      {new_map, timeout} = WaitingList.insert(map, 1, :data, reply_fn, 500, time_fn)

      assert new_map == %{1 => [{1500, reply_fn, :data}]}
      assert timeout == 500
    end

    test "adds entry to existing version, maintaining deadline sort order" do
      reply_fn1 = fn _ -> :ok end
      reply_fn2 = fn _ -> :ok end
      reply_fn3 = fn _ -> :ok end

      map = %{1 => [{1200, reply_fn1, :data1}]}
      time_fn = fn -> 1000 end

      {new_map, _} = WaitingList.insert(map, 1, :data2, reply_fn2, 100, time_fn)
      assert new_map == %{1 => [{1100, reply_fn2, :data2}, {1200, reply_fn1, :data1}]}

      # Add entry with later deadline
      {new_map, _} = WaitingList.insert(new_map, 1, :data3, reply_fn3, 300, time_fn)

      assert new_map == %{
               1 => [
                 {1100, reply_fn2, :data2},
                 {1200, reply_fn1, :data1},
                 {1300, reply_fn3, :data3}
               ]
             }
    end

    test "adds entry to new version" do
      reply_fn1 = fn _ -> :ok end
      reply_fn2 = fn _ -> :ok end

      map = %{1 => [{1200, reply_fn1, :data1}]}
      time_fn = fn -> 1000 end

      {new_map, timeout} = WaitingList.insert(map, 2, :data2, reply_fn2, 100, time_fn)

      assert new_map == %{
               1 => [{1200, reply_fn1, :data1}],
               2 => [{1100, reply_fn2, :data2}]
             }

      assert timeout == 100
    end

    test "calculates correct timeout for next deadline" do
      reply_fn = fn _ -> :ok end
      map = %{}
      time_fn = fn -> 1000 end

      {new_map, timeout} = WaitingList.insert(map, 1, :data, reply_fn, 500, time_fn)
      assert timeout == 500

      {_new_map, timeout} = WaitingList.insert(new_map, 2, :data, reply_fn, 200, time_fn)
      assert timeout == 200
    end
  end

  describe "insert/5 (without time function)" do
    test "uses default time function" do
      map = %{}
      reply_fn = fn _ -> :ok end

      {new_map, _timeout} = WaitingList.insert(map, 1, :data, reply_fn, 500)

      [{deadline, ^reply_fn, :data}] = Map.get(new_map, 1)
      assert is_integer(deadline)
    end
  end

  describe "remove/2" do
    test "removes first entry from version with single entry" do
      reply_fn = fn _ -> :ok end
      map = %{1 => [{1500, reply_fn, :data}]}

      {new_map, removed_entry} = WaitingList.remove(map, 1)

      assert new_map == %{}
      assert removed_entry == {1500, reply_fn, :data}
    end

    test "removes first entry from version with multiple entries" do
      reply_fn1 = fn _ -> :ok end
      reply_fn2 = fn _ -> :ok end

      map = %{1 => [{1200, reply_fn1, :data1}, {1500, reply_fn2, :data2}]}

      {new_map, removed_entry} = WaitingList.remove(map, 1)

      assert new_map == %{1 => [{1500, reply_fn2, :data2}]}
      assert removed_entry == {1200, reply_fn1, :data1}
    end

    test "returns nil when version does not exist" do
      map = %{2 => [{1500, fn _ -> :ok end, :data}]}

      {new_map, removed_entry} = WaitingList.remove(map, 1)

      assert new_map == map
      assert removed_entry == nil
    end

    test "returns nil when version has empty list" do
      map = %{1 => []}

      {new_map, removed_entry} = WaitingList.remove(map, 1)

      assert new_map == map
      assert removed_entry == nil
    end
  end

  describe "remove_all/2" do
    test "removes all entries for version" do
      reply_fn1 = fn _ -> :ok end
      reply_fn2 = fn _ -> :ok end

      map = %{
        1 => [{1200, reply_fn1, :data1}, {1500, reply_fn2, :data2}],
        2 => [{1300, reply_fn1, :other}]
      }

      {new_map, removed_entries} = WaitingList.remove_all(map, 1)

      assert new_map == %{2 => [{1300, reply_fn1, :other}]}
      assert removed_entries == [{1200, reply_fn1, :data1}, {1500, reply_fn2, :data2}]
    end

    test "returns empty list when version does not exist" do
      map = %{2 => [{1500, fn _ -> :ok end, :data}]}

      {new_map, removed_entries} = WaitingList.remove_all(map, 1)

      assert new_map == map
      assert removed_entries == []
    end
  end

  describe "find/2" do
    test "finds first entry for version" do
      reply_fn1 = fn _ -> :ok end
      reply_fn2 = fn _ -> :ok end

      map = %{1 => [{1200, reply_fn1, :data1}, {1500, reply_fn2, :data2}]}

      entry = WaitingList.find(map, 1)

      assert entry == {1200, reply_fn1, :data1}
    end

    test "returns nil when version does not exist" do
      map = %{2 => [{1500, fn _ -> :ok end, :data}]}

      entry = WaitingList.find(map, 1)

      assert entry == nil
    end

    test "returns nil when version has empty list" do
      map = %{1 => []}

      entry = WaitingList.find(map, 1)

      assert entry == nil
    end
  end

  describe "expire/2" do
    test "removes expired entries and keeps non-expired ones" do
      reply_fn1 = fn _ -> :ok end
      reply_fn2 = fn _ -> :ok end
      reply_fn3 = fn _ -> :ok end

      map = %{
        1 => [{900, reply_fn1, :expired1}, {1100, reply_fn2, :future1}],
        2 => [{800, reply_fn3, :expired2}],
        3 => [{1200, reply_fn1, :future2}]
      }

      time_fn = fn -> 1000 end

      {new_map, expired_entries} = WaitingList.expire(map, time_fn)

      assert new_map == %{
               1 => [{1100, reply_fn2, :future1}],
               3 => [{1200, reply_fn1, :future2}]
             }

      assert length(expired_entries) == 2
      assert {900, reply_fn1, :expired1} in expired_entries
      assert {800, reply_fn3, :expired2} in expired_entries
    end

    test "removes all entries when all are expired" do
      reply_fn1 = fn _ -> :ok end
      reply_fn2 = fn _ -> :ok end

      map = %{
        1 => [{900, reply_fn1, :expired1}],
        2 => [{800, reply_fn2, :expired2}]
      }

      time_fn = fn -> 1000 end

      {new_map, expired_entries} = WaitingList.expire(map, time_fn)

      assert new_map == %{}
      assert length(expired_entries) == 2
    end

    test "keeps all entries when none are expired" do
      reply_fn1 = fn _ -> :ok end
      reply_fn2 = fn _ -> :ok end

      map = %{
        1 => [{1100, reply_fn1, :future1}],
        2 => [{1200, reply_fn2, :future2}]
      }

      time_fn = fn -> 1000 end

      {new_map, expired_entries} = WaitingList.expire(map, time_fn)

      assert new_map == map
      assert expired_entries == []
    end

    test "handles empty map" do
      map = %{}
      time_fn = fn -> 1000 end

      {new_map, expired_entries} = WaitingList.expire(map, time_fn)

      assert new_map == %{}
      assert expired_entries == []
    end

    test "handles entries with exact deadline boundary" do
      reply_fn1 = fn _ -> :ok end
      reply_fn2 = fn _ -> :ok end

      map = %{
        1 => [{1000, reply_fn1, :exact}],
        2 => [{1001, reply_fn2, :future}]
      }

      time_fn = fn -> 1000 end

      {new_map, expired_entries} = WaitingList.expire(map, time_fn)

      assert new_map == %{2 => [{1001, reply_fn2, :future}]}
      assert expired_entries == [{1000, reply_fn1, :exact}]
    end
  end

  describe "expire/1 (without time function)" do
    test "uses default time function" do
      reply_fn = fn _ -> :ok end
      past_time = :erlang.monotonic_time(:millisecond) - 1000
      map = %{1 => [{past_time, reply_fn, :expired}]}

      {new_map, expired_entries} = WaitingList.expire(map)

      assert new_map == %{}
      assert expired_entries == [{past_time, reply_fn, :expired}]
    end
  end

  describe "next_timeout/2" do
    test "returns earliest deadline timeout" do
      reply_fn = fn _ -> :ok end

      map = %{
        1 => [{1200, reply_fn, :data1}],
        2 => [{1100, reply_fn, :data2}],
        3 => [{1300, reply_fn, :data3}]
      }

      time_fn = fn -> 1000 end

      timeout = WaitingList.next_timeout(map, time_fn)

      assert timeout == 100
    end

    test "returns zero when deadline has passed" do
      reply_fn = fn _ -> :ok end
      map = %{1 => [{900, reply_fn, :data}]}
      time_fn = fn -> 1000 end

      timeout = WaitingList.next_timeout(map, time_fn)

      assert timeout == 0
    end

    test "returns infinity for empty map" do
      map = %{}
      time_fn = fn -> 1000 end

      timeout = WaitingList.next_timeout(map, time_fn)

      assert timeout == :infinity
    end

    test "returns infinity when all versions have empty lists" do
      map = %{1 => [], 2 => []}
      time_fn = fn -> 1000 end

      timeout = WaitingList.next_timeout(map, time_fn)

      assert timeout == :infinity
    end

    test "ignores empty lists and finds earliest non-empty deadline" do
      reply_fn = fn _ -> :ok end

      map = %{
        1 => [],
        2 => [{1200, reply_fn, :data}],
        3 => []
      }

      time_fn = fn -> 1000 end

      timeout = WaitingList.next_timeout(map, time_fn)

      assert timeout == 200
    end
  end

  describe "next_timeout/1 (without time function)" do
    test "uses default time function" do
      reply_fn = fn _ -> :ok end
      future_time = :erlang.monotonic_time(:millisecond) + 1000
      map = %{1 => [{future_time, reply_fn, :future}]}

      timeout = WaitingList.next_timeout(map)

      assert is_integer(timeout)
      assert timeout > 0
    end

    test "returns infinity for empty map" do
      map = %{}

      timeout = WaitingList.next_timeout(map)

      assert timeout == :infinity
    end
  end

  describe "reply_to_expired/2" do
    test "calls reply function for each expired entry with default error" do
      test_pid = self()

      reply_fn1 = fn response -> send(test_pid, {:reply1, response}) end
      reply_fn2 = fn response -> send(test_pid, {:reply2, response}) end

      expired_entries = [
        {1000, reply_fn1, :data1},
        {1100, reply_fn2, :data2}
      ]

      WaitingList.reply_to_expired(expired_entries)

      assert_received {:reply1, {:error, :waiting_timeout}}
      assert_received {:reply2, {:error, :waiting_timeout}}
    end

    test "calls reply function for each expired entry with custom error" do
      test_pid = self()

      reply_fn = fn response -> send(test_pid, {:reply, response}) end
      expired_entries = [{1000, reply_fn, :data}]

      WaitingList.reply_to_expired(expired_entries, {:error, :custom_timeout})

      assert_received {:reply, {:error, :custom_timeout}}
    end

    test "handles empty expired entries list" do
      WaitingList.reply_to_expired([])
    end
  end

  describe "complex scenarios" do
    test "maintains deadline sorting across multiple operations" do
      reply_fn = fn _ -> :ok end
      time_fn = fn -> 1000 end

      map = %{}

      # Add entries in random deadline order to test sorting
      {map, _} = WaitingList.insert(map, 1, :data3, reply_fn, 300, time_fn)
      {map, _} = WaitingList.insert(map, 1, :data1, reply_fn, 100, time_fn)
      {map, _} = WaitingList.insert(map, 1, :data2, reply_fn, 200, time_fn)
      {map, _} = WaitingList.insert(map, 1, :data4, reply_fn, 400, time_fn)

      entries = Map.get(map, 1)
      deadlines = Enum.map(entries, fn {deadline, _, _} -> deadline end)

      assert deadlines == [1100, 1200, 1300, 1400]
    end

    test "expire and timeout work together correctly" do
      reply_fn = fn _ -> :ok end
      time_fn = fn -> 1000 end

      map = %{
        1 => [{900, reply_fn, :expired}, {1200, reply_fn, :future1}],
        2 => [{800, reply_fn, :expired}, {1100, reply_fn, :future2}]
      }

      {new_map, expired_entries} = WaitingList.expire(map, time_fn)
      timeout = WaitingList.next_timeout(new_map, time_fn)

      assert length(expired_entries) == 2
      assert timeout == 100
    end

    test "single waiter pattern (resolver-like usage)" do
      reply_fn1 = fn _ -> :ok end
      reply_fn2 = fn _ -> :ok end
      time_fn = fn -> 1000 end

      map = %{}

      {map, _} = WaitingList.insert(map, 1, :req1, reply_fn1, 500, time_fn)
      {map, _} = WaitingList.insert(map, 2, :req2, reply_fn2, 600, time_fn)

      # Fulfill version 1 (single waiter pattern)
      {map, removed} = WaitingList.remove(map, 1)
      assert removed == {1500, reply_fn1, :req1}

      assert Map.get(map, 1) == nil
      assert Map.get(map, 2) == [{1600, reply_fn2, :req2}]
    end

    test "multi waiter pattern (longpulls-like usage)" do
      reply_fn1 = fn _ -> :ok end
      reply_fn2 = fn _ -> :ok end
      reply_fn3 = fn _ -> :ok end
      time_fn = fn -> 1000 end

      map = %{}

      # Multiple waiters for same version
      {map, _} = WaitingList.insert(map, 1, :req1, reply_fn1, 500, time_fn)
      {map, _} = WaitingList.insert(map, 1, :req2, reply_fn2, 600, time_fn)
      {map, _} = WaitingList.insert(map, 1, :req3, reply_fn3, 400, time_fn)

      # Fulfill all waiters for version (multi waiter pattern)
      {map, removed_all} = WaitingList.remove_all(map, 1)

      assert length(removed_all) == 3
      assert Map.get(map, 1) == nil

      deadlines = Enum.map(removed_all, fn {deadline, _, _} -> deadline end)
      assert deadlines == [1400, 1500, 1600]
    end
  end
end
