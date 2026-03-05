defmodule Bedrock.Internal.WaitingListTest do
  use ExUnit.Case, async: true

  alias Bedrock.Internal.WaitingList

  # Test helpers
  defp simple_reply_fn, do: fn _ -> :ok end
  defp test_time_fn, do: fn -> 1000 end
  defp test_pid_reply_fn(pid, tag), do: fn response -> send(pid, {tag, response}) end

  describe "insert/5" do
    test "adds first entry to empty map" do
      map = %{}
      reply_fn = simple_reply_fn()
      time_fn = test_time_fn()

      assert {%{1 => [{1500, ^reply_fn, :data}]}, 500} =
               WaitingList.insert(map, 1, :data, reply_fn, 500, time_fn)
    end

    test "adds entry to existing version, maintaining deadline sort order" do
      reply_fn1 = simple_reply_fn()
      reply_fn2 = simple_reply_fn()
      reply_fn3 = simple_reply_fn()

      map = %{1 => [{1200, reply_fn1, :data1}]}
      time_fn = test_time_fn()

      assert {%{1 => [{1100, ^reply_fn2, :data2}, {1200, ^reply_fn1, :data1}]}, _} =
               WaitingList.insert(map, 1, :data2, reply_fn2, 100, time_fn)

      # Add entry with later deadline
      new_map = %{1 => [{1100, reply_fn2, :data2}, {1200, reply_fn1, :data1}]}

      assert {%{
                1 => [
                  {1100, ^reply_fn2, :data2},
                  {1200, ^reply_fn1, :data1},
                  {1300, ^reply_fn3, :data3}
                ]
              }, _} = WaitingList.insert(new_map, 1, :data3, reply_fn3, 300, time_fn)
    end

    test "adds entry to new version" do
      reply_fn1 = simple_reply_fn()
      reply_fn2 = simple_reply_fn()

      map = %{1 => [{1200, reply_fn1, :data1}]}
      time_fn = test_time_fn()

      assert {%{
                1 => [{1200, ^reply_fn1, :data1}],
                2 => [{1100, ^reply_fn2, :data2}]
              }, 100} = WaitingList.insert(map, 2, :data2, reply_fn2, 100, time_fn)
    end

    test "calculates correct timeout for next deadline" do
      reply_fn = simple_reply_fn()
      map = %{}
      time_fn = test_time_fn()

      assert {new_map, 500} = WaitingList.insert(map, 1, :data, reply_fn, 500, time_fn)
      assert {_, 200} = WaitingList.insert(new_map, 2, :data, reply_fn, 200, time_fn)
    end
  end

  describe "insert/5 (without time function)" do
    test "uses default time function" do
      map = %{}
      reply_fn = simple_reply_fn()

      assert {new_map, _timeout} = WaitingList.insert(map, 1, :data, reply_fn, 500)
      assert [{deadline, ^reply_fn, :data}] = Map.get(new_map, 1)
      assert is_integer(deadline)
    end
  end

  describe "remove/2" do
    test "removes first entry from version with single entry" do
      reply_fn = simple_reply_fn()
      map = %{1 => [{1500, reply_fn, :data}]}

      assert {%{}, {1500, ^reply_fn, :data}} = WaitingList.remove(map, 1)
    end

    test "removes first entry from version with multiple entries" do
      reply_fn1 = simple_reply_fn()
      reply_fn2 = simple_reply_fn()

      map = %{1 => [{1200, reply_fn1, :data1}, {1500, reply_fn2, :data2}]}

      assert {%{1 => [{1500, ^reply_fn2, :data2}]}, {1200, ^reply_fn1, :data1}} =
               WaitingList.remove(map, 1)
    end

    test "returns nil when version does not exist" do
      map = %{2 => [{1500, simple_reply_fn(), :data}]}

      assert {^map, nil} = WaitingList.remove(map, 1)
    end

    test "returns nil when version has empty list" do
      map = %{1 => []}

      assert {^map, nil} = WaitingList.remove(map, 1)
    end
  end

  describe "remove_all/2" do
    test "removes all entries for version" do
      reply_fn1 = simple_reply_fn()
      reply_fn2 = simple_reply_fn()

      map = %{
        1 => [{1200, reply_fn1, :data1}, {1500, reply_fn2, :data2}],
        2 => [{1300, reply_fn1, :other}]
      }

      assert {%{2 => [{1300, ^reply_fn1, :other}]}, [{1200, ^reply_fn1, :data1}, {1500, ^reply_fn2, :data2}]} =
               WaitingList.remove_all(map, 1)
    end

    test "returns empty list when version does not exist" do
      map = %{2 => [{1500, simple_reply_fn(), :data}]}

      assert {^map, []} = WaitingList.remove_all(map, 1)
    end
  end

  describe "remove_all_less_than/2" do
    test "removes all entries where version is less than threshold" do
      reply_fn1 = simple_reply_fn()
      reply_fn2 = simple_reply_fn()
      reply_fn3 = simple_reply_fn()

      map = %{
        5 => [{1200, reply_fn1, :data5}],
        10 => [{1300, reply_fn2, :data10}],
        15 => [{1400, reply_fn3, :data15}]
      }

      {new_map, removed} = WaitingList.remove_all_less_than(map, 12)

      # Only version 15 should remain
      assert Map.keys(new_map) == [15]
      assert new_map[15] == [{1400, reply_fn3, :data15}]

      # Versions 5 and 10 should be removed
      assert length(removed) == 2
      assert {1200, reply_fn1, :data5} in removed
      assert {1300, reply_fn2, :data10} in removed
    end

    test "removes nothing when all versions are >= threshold" do
      reply_fn = simple_reply_fn()

      map = %{
        10 => [{1200, reply_fn, :data10}],
        20 => [{1300, reply_fn, :data20}]
      }

      {new_map, removed} = WaitingList.remove_all_less_than(map, 5)

      assert new_map == map
      assert removed == []
    end

    test "removes all when all versions are < threshold" do
      reply_fn1 = simple_reply_fn()
      reply_fn2 = simple_reply_fn()

      map = %{
        5 => [{1200, reply_fn1, :data5}],
        10 => [{1300, reply_fn2, :data10}]
      }

      {new_map, removed} = WaitingList.remove_all_less_than(map, 100)

      assert new_map == %{}
      assert length(removed) == 2
    end

    test "handles empty map" do
      {new_map, removed} = WaitingList.remove_all_less_than(%{}, 100)

      assert new_map == %{}
      assert removed == []
    end

    test "handles exact boundary - excludes equal versions" do
      reply_fn1 = simple_reply_fn()
      reply_fn2 = simple_reply_fn()

      map = %{
        10 => [{1200, reply_fn1, :data10}],
        11 => [{1300, reply_fn2, :data11}]
      }

      # threshold=10 means remove versions < 10, keep versions >= 10
      {new_map, removed} = WaitingList.remove_all_less_than(map, 10)

      assert new_map |> Map.keys() |> Enum.sort() == [10, 11]
      assert removed == []
    end

    test "flattens entries from multiple versions" do
      reply_fn1 = simple_reply_fn()
      reply_fn2 = simple_reply_fn()
      reply_fn3 = simple_reply_fn()

      # Version 5 has multiple entries
      map = %{
        5 => [{1200, reply_fn1, :data5a}, {1250, reply_fn2, :data5b}],
        10 => [{1300, reply_fn3, :data10}]
      }

      {_new_map, removed} = WaitingList.remove_all_less_than(map, 8)

      # Should get both entries from version 5
      assert length(removed) == 2
      assert {1200, reply_fn1, :data5a} in removed
      assert {1250, reply_fn2, :data5b} in removed
    end
  end

  describe "find/2" do
    test "finds first entry for version" do
      reply_fn1 = simple_reply_fn()
      reply_fn2 = simple_reply_fn()

      map = %{1 => [{1200, reply_fn1, :data1}, {1500, reply_fn2, :data2}]}

      assert WaitingList.find(map, 1) == {1200, reply_fn1, :data1}
    end

    test "returns nil when version does not exist" do
      map = %{2 => [{1500, simple_reply_fn(), :data}]}

      assert WaitingList.find(map, 1) == nil
    end

    test "returns nil when version has empty list" do
      map = %{1 => []}

      assert WaitingList.find(map, 1) == nil
    end
  end

  describe "expire/2" do
    test "removes expired entries and keeps non-expired ones" do
      reply_fn1 = simple_reply_fn()
      reply_fn2 = simple_reply_fn()
      reply_fn3 = simple_reply_fn()

      map = %{
        1 => [{900, reply_fn1, :expired1}, {1100, reply_fn2, :future1}],
        2 => [{800, reply_fn3, :expired2}],
        3 => [{1200, reply_fn1, :future2}]
      }

      time_fn = test_time_fn()

      assert {%{
                1 => [{1100, ^reply_fn2, :future1}],
                3 => [{1200, ^reply_fn1, :future2}]
              }, expired_entries} = WaitingList.expire(map, time_fn)

      assert length(expired_entries) == 2
      assert {900, reply_fn1, :expired1} in expired_entries
      assert {800, reply_fn3, :expired2} in expired_entries
    end

    test "removes all entries when all are expired" do
      reply_fn1 = simple_reply_fn()
      reply_fn2 = simple_reply_fn()

      map = %{
        1 => [{900, reply_fn1, :expired1}],
        2 => [{800, reply_fn2, :expired2}]
      }

      time_fn = test_time_fn()

      assert {%{}, expired_entries} = WaitingList.expire(map, time_fn)
      assert length(expired_entries) == 2
    end

    test "keeps all entries when none are expired" do
      reply_fn1 = simple_reply_fn()
      reply_fn2 = simple_reply_fn()

      map = %{
        1 => [{1100, reply_fn1, :future1}],
        2 => [{1200, reply_fn2, :future2}]
      }

      time_fn = test_time_fn()

      assert {^map, []} = WaitingList.expire(map, time_fn)
    end

    test "handles empty map" do
      map = %{}
      time_fn = test_time_fn()

      assert {%{}, []} = WaitingList.expire(map, time_fn)
    end

    test "handles entries with exact deadline boundary" do
      reply_fn1 = simple_reply_fn()
      reply_fn2 = simple_reply_fn()

      map = %{
        1 => [{1000, reply_fn1, :exact}],
        2 => [{1001, reply_fn2, :future}]
      }

      time_fn = test_time_fn()

      assert {%{2 => [{1001, ^reply_fn2, :future}]}, [{1000, ^reply_fn1, :exact}]} =
               WaitingList.expire(map, time_fn)
    end
  end

  describe "expire/1 (without time function)" do
    test "uses default time function" do
      reply_fn = simple_reply_fn()
      past_time = :erlang.monotonic_time(:millisecond) - 1000
      map = %{1 => [{past_time, reply_fn, :expired}]}

      assert {%{}, [{^past_time, ^reply_fn, :expired}]} = WaitingList.expire(map)
    end
  end

  describe "next_timeout/2" do
    test "returns earliest deadline timeout" do
      reply_fn = simple_reply_fn()

      map = %{
        1 => [{1200, reply_fn, :data1}],
        2 => [{1100, reply_fn, :data2}],
        3 => [{1300, reply_fn, :data3}]
      }

      time_fn = test_time_fn()

      assert WaitingList.next_timeout(map, time_fn) == 100
    end

    test "returns zero when deadline has passed" do
      reply_fn = simple_reply_fn()
      map = %{1 => [{900, reply_fn, :data}]}
      time_fn = test_time_fn()

      assert WaitingList.next_timeout(map, time_fn) == 0
    end

    test "returns infinity for empty map" do
      map = %{}
      time_fn = test_time_fn()

      assert WaitingList.next_timeout(map, time_fn) == :infinity
    end

    test "returns infinity when all versions have empty lists" do
      map = %{1 => [], 2 => []}
      time_fn = test_time_fn()

      assert WaitingList.next_timeout(map, time_fn) == :infinity
    end

    test "ignores empty lists and finds earliest non-empty deadline" do
      reply_fn = simple_reply_fn()

      map = %{
        1 => [],
        2 => [{1200, reply_fn, :data}],
        3 => []
      }

      time_fn = test_time_fn()

      assert WaitingList.next_timeout(map, time_fn) == 200
    end
  end

  describe "next_timeout/1 (without time function)" do
    test "uses default time function" do
      reply_fn = simple_reply_fn()
      future_time = :erlang.monotonic_time(:millisecond) + 1000
      map = %{1 => [{future_time, reply_fn, :future}]}

      timeout = WaitingList.next_timeout(map)

      assert is_integer(timeout)
      assert timeout > 0
    end

    test "returns infinity for empty map" do
      map = %{}

      assert WaitingList.next_timeout(map) == :infinity
    end
  end

  describe "reply_to_expired/2" do
    test "calls reply function for each expired entry with default error" do
      test_pid = self()

      reply_fn1 = test_pid_reply_fn(test_pid, :reply1)
      reply_fn2 = test_pid_reply_fn(test_pid, :reply2)

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

      reply_fn = test_pid_reply_fn(test_pid, :reply)
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
      reply_fn = simple_reply_fn()
      time_fn = test_time_fn()

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
      reply_fn = simple_reply_fn()
      time_fn = test_time_fn()

      map = %{
        1 => [{900, reply_fn, :expired}, {1200, reply_fn, :future1}],
        2 => [{800, reply_fn, :expired}, {1100, reply_fn, :future2}]
      }

      assert {new_map, expired_entries} = WaitingList.expire(map, time_fn)
      assert length(expired_entries) == 2
      assert WaitingList.next_timeout(new_map, time_fn) == 100
    end

    test "single waiter pattern (resolver-like usage)" do
      reply_fn1 = simple_reply_fn()
      reply_fn2 = simple_reply_fn()
      time_fn = test_time_fn()

      map = %{}

      {map, _} = WaitingList.insert(map, 1, :req1, reply_fn1, 500, time_fn)
      {map, _} = WaitingList.insert(map, 2, :req2, reply_fn2, 600, time_fn)

      # Fulfill version 1 (single waiter pattern)
      assert {map, {1500, ^reply_fn1, :req1}} = WaitingList.remove(map, 1)

      assert Map.get(map, 1) == nil
      assert Map.get(map, 2) == [{1600, reply_fn2, :req2}]
    end

    test "multi waiter pattern (longpulls-like usage)" do
      reply_fn1 = simple_reply_fn()
      reply_fn2 = simple_reply_fn()
      reply_fn3 = simple_reply_fn()
      time_fn = test_time_fn()

      map = %{}

      # Multiple waiters for same version
      {map, _} = WaitingList.insert(map, 1, :req1, reply_fn1, 500, time_fn)
      {map, _} = WaitingList.insert(map, 1, :req2, reply_fn2, 600, time_fn)
      {map, _} = WaitingList.insert(map, 1, :req3, reply_fn3, 400, time_fn)

      # Fulfill all waiters for version (multi waiter pattern)
      assert {map, removed_all} = WaitingList.remove_all(map, 1)

      assert length(removed_all) == 3
      assert Map.get(map, 1) == nil

      deadlines = Enum.map(removed_all, fn {deadline, _, _} -> deadline end)
      assert deadlines == [1400, 1500, 1600]
    end
  end
end
