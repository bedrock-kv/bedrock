defmodule Bedrock.DataPlane.Materializer.Olivine.ReadingTest do
  @moduledoc """
  Behavioral tests for the Reading module's public API.

  Unlike ReadingTestHelpers (which duplicates fetch logic), these tests
  exercise the real Reading module: synchronous and asynchronous reads,
  waitlisting on :version_too_new, waitlist notification, active task
  tracking, and shutdown.
  """
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Bedrock.DataPlane.Materializer.Olivine.Database
  alias Bedrock.DataPlane.Materializer.Olivine.IndexManager
  alias Bedrock.DataPlane.Materializer.Olivine.Reading
  alias Bedrock.DataPlane.Materializer.Olivine.Reading.ReadingContext
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version
  alias Bedrock.KeySelector

  @moduletag :integration

  defp v1, do: Version.from_integer(1)
  defp v2, do: Version.from_integer(2)

  defp apply_mutations(index_manager, database, version, mutations) do
    transaction = Transaction.encode(%{commit_version: version, mutations: mutations})
    IndexManager.apply_transaction(index_manager, transaction, database)
  end

  defp open_database do
    tmp_dir = System.tmp_dir!()
    db_file = Path.join(tmp_dir, "reading_test_#{System.unique_integer([:positive])}.dets")
    table_name = String.to_atom("reading_test_#{System.unique_integer([:positive])}")
    {result, _logs} = with_log(fn -> Database.open(table_name, db_file, pool_size: 1) end)
    {:ok, database} = result

    on_exit(fn ->
      File.rm_rf(db_file)
    end)

    {database, db_file}
  end

  setup do
    {database, _db_file} = open_database()

    {index_manager, database} =
      apply_mutations(IndexManager.new(), database, v1(), [
        {:set, "key1", "value1"},
        {:set, "key2", "value2"},
        {:set, "key3", "value3"},
        {:set, "range:a", "range_a_value"},
        {:set, "range:b", "range_b_value"},
        {:set, "range:c", "range_c_value"},
        {:set, "range:d", "range_d_value"}
      ])

    on_exit(fn -> Database.close(database) end)

    context = ReadingContext.new(index_manager, database)

    {:ok, manager: Reading.new(), context: context, index_manager: index_manager, database: database}
  end

  # Advances the index to version 2 (adding one new key) and returns an
  # updated context. Used by waitlist notification tests.
  defp advance_to_v2(index_manager, database) do
    {index_manager, database} =
      apply_mutations(index_manager, database, v2(), [{:set, "key4", "value4"}])

    ReadingContext.new(index_manager, database)
  end

  describe "synchronous handle_get/5 (no reply_fn)" do
    test "returns the stored value for an existing binary key", %{manager: manager, context: context} do
      assert {^manager, {:ok, "value2"}} = Reading.handle_get(manager, context, "key2", v1(), [])
    end

    test "returns {:error, :not_found} for a missing key", %{manager: manager, context: context} do
      assert {^manager, {:error, :not_found}} = Reading.handle_get(manager, context, "key1a", v1(), [])
    end

    test "resolves a KeySelector and returns {resolved_key, value}", %{manager: manager, context: context} do
      selector = "key1" |> KeySelector.first_greater_or_equal() |> KeySelector.add(1)

      assert {^manager, {:ok, {"key2", "value2"}}} =
               Reading.handle_get(manager, context, selector, v1(), [])
    end
  end

  describe "synchronous handle_get_range/6 (no reply_fn)" do
    test "returns key-value pairs within the range", %{manager: manager, context: context} do
      assert {^manager, {:ok, {results, false}}} =
               Reading.handle_get_range(manager, context, "range:a", "range:z", v1(), [])

      assert results == [
               {"range:a", "range_a_value"},
               {"range:b", "range_b_value"},
               {"range:c", "range_c_value"},
               {"range:d", "range_d_value"}
             ]
    end

    test "resolves KeySelector boundaries", %{manager: manager, context: context} do
      start_selector = KeySelector.first_greater_or_equal("range:a")
      # Resolves to "range:d", which becomes the (exclusive) end of the range.
      end_selector = KeySelector.first_greater_or_equal("range:d")

      assert {^manager, {:ok, {results, false}}} =
               Reading.handle_get_range(manager, context, start_selector, end_selector, v1(), [])

      keys = Enum.map(results, fn {k, _v} -> k end)
      assert keys == ["range:a", "range:b", "range:c"]
    end

    test "truncates results and reports has_more when limit is smaller than the result count", %{
      manager: manager,
      context: context
    } do
      assert {^manager, {:ok, {results, true}}} =
               Reading.handle_get_range(manager, context, "range:a", "range:z", v1(), limit: 2)

      assert [{"range:a", "range_a_value"}, {"range:b", "range_b_value"}] = results
    end

    test "reports has_more: false when limit exceeds the result count", %{manager: manager, context: context} do
      assert {^manager, {:ok, {results, false}}} =
               Reading.handle_get_range(manager, context, "range:a", "range:z", v1(), limit: 100)

      assert length(results) == 4
    end
  end

  describe "asynchronous reads (reply_fn provided)" do
    test "handle_get tracks the task pid and delivers the result via reply_fn", %{
      manager: manager,
      context: context
    } do
      test_pid = self()
      reply_fn = fn result -> send(test_pid, {:async_get, result}) end

      assert {updated_manager, :ok} =
               Reading.handle_get(manager, context, "key1", v1(), reply_fn: reply_fn)

      active_tasks = Reading.get_active_tasks(updated_manager)
      assert MapSet.size(active_tasks) == 1
      [task_pid] = MapSet.to_list(active_tasks)

      assert_receive {:async_get, {:ok, "value1"}}

      # Timing info was stored for this task; removing it exercises the
      # telemetry-emitting branch and clears the process dictionary entry.
      assert Process.get({:read_timing, task_pid})
      cleaned = Reading.remove_active_task(updated_manager, task_pid)
      assert Reading.get_active_tasks(cleaned) == MapSet.new()
      assert Process.get({:read_timing, task_pid}) == nil
    end

    test "handle_get_range delivers range results via reply_fn", %{manager: manager, context: context} do
      test_pid = self()
      reply_fn = fn result -> send(test_pid, {:async_range, result}) end

      assert {updated_manager, :ok} =
               Reading.handle_get_range(manager, context, "range:a", "range:z", v1(),
                 reply_fn: reply_fn,
                 limit: 2
               )

      assert MapSet.size(Reading.get_active_tasks(updated_manager)) == 1
      assert_receive {:async_range, {:ok, {[{"range:a", _}, {"range:b", _}], true}}}
    end

    test "remove_active_task handles pids without stored timing info", %{manager: manager, context: context} do
      test_pid = self()
      reply_fn = fn result -> send(test_pid, {:async_get, result}) end

      {updated_manager, :ok} = Reading.handle_get(manager, context, "key1", v1(), reply_fn: reply_fn)
      [task_pid] = MapSet.to_list(Reading.get_active_tasks(updated_manager))
      assert_receive {:async_get, {:ok, "value1"}}

      # Simulate the timing entry being absent (nil fallback branch).
      Process.delete({:read_timing, task_pid})
      cleaned = Reading.remove_active_task(updated_manager, task_pid)
      assert Reading.get_active_tasks(cleaned) == MapSet.new()
    end
  end

  describe "waitlisting on :version_too_new" do
    test "get with a binary key and wait_ms queues the fetch", %{manager: manager, context: context} do
      reply_fn = fn _ -> :ok end

      assert {updated_manager, :ok} =
               Reading.handle_get(manager, context, "key1", v2(), wait_ms: 5_000, reply_fn: reply_fn)

      assert [{_deadline, ^reply_fn, {"key1", version}}] = updated_manager.waiting_fetches[v2()]
      assert version == v2()
    end

    test "get with a KeySelector and wait_ms queues the fetch", %{manager: manager, context: context} do
      reply_fn = fn _ -> :ok end
      selector = KeySelector.first_greater_or_equal("key1")

      assert {updated_manager, :ok} =
               Reading.handle_get(manager, context, selector, v2(), wait_ms: 5_000, reply_fn: reply_fn)

      assert [{_deadline, ^reply_fn, {%KeySelector{key: "key1"}, _version}}] =
               updated_manager.waiting_fetches[v2()]
    end

    test "get_range with binary keys and wait_ms queues the fetch", %{manager: manager, context: context} do
      reply_fn = fn _ -> :ok end

      assert {updated_manager, :ok} =
               Reading.handle_get_range(manager, context, "range:a", "range:z", v2(),
                 wait_ms: 5_000,
                 reply_fn: reply_fn
               )

      assert [{_deadline, ^reply_fn, {"range:a", "range:z", _version}}] =
               updated_manager.waiting_fetches[v2()]
    end

    test "get_range with KeySelectors and wait_ms queues the fetch", %{manager: manager, context: context} do
      reply_fn = fn _ -> :ok end
      start_selector = KeySelector.first_greater_or_equal("range:a")
      end_selector = KeySelector.first_greater_or_equal("range:d")

      assert {updated_manager, :ok} =
               Reading.handle_get_range(manager, context, start_selector, end_selector, v2(),
                 wait_ms: 5_000,
                 reply_fn: reply_fn
               )

      assert [{_deadline, ^reply_fn, {%KeySelector{}, %KeySelector{}, _version}}] =
               updated_manager.waiting_fetches[v2()]
    end

    # Regression test for bedrock-66h: waitlisting a request that has wait_ms
    # but no reply_fn used to queue a nil reply_fn. When the version was later
    # applied, notify_waiting_fetches ran the fetch synchronously (nil reply_fn
    # means "do now"), got {:ok, value} back, matched it against the {:ok, pid}
    # clause in execute_fetch_request, and crashed in Process.monitor/1 with an
    # ArgumentError. The fix rejects waitlisting without a reply_fn: the
    # waitlist's only delivery mechanism IS reply_fn, so a caller that supplied
    # none could never be notified anyway — it now gets the error immediately,
    # exactly as if wait_ms had not been set. (The sole production caller,
    # Olivine's Server, always supplies a reply_fn.)
    test "with wait_ms but no reply_fn the error is returned immediately and nothing is waitlisted", %{
      manager: manager,
      context: context,
      index_manager: index_manager,
      database: database
    } do
      {manager_after_get, get_result} =
        Reading.handle_get(manager, context, "key1", v2(), wait_ms: 5_000)

      {manager_after_range, range_result} =
        Reading.handle_get_range(manager_after_get, context, "range:a", "range:z", v2(), wait_ms: 5_000)

      # Pre-fix, both requests were waitlisted with a nil reply_fn and this
      # notification crashed with an ArgumentError in Process.monitor/1.
      new_context = advance_to_v2(index_manager, database)
      notified_manager = Reading.notify_waiting_fetches(manager_after_range, new_context, v2())

      assert notified_manager.waiting_fetches == %{}
      assert Reading.get_active_tasks(notified_manager) == MapSet.new()

      # Post-fix, the caller gets the error straight back and no state changes.
      assert get_result == {:error, :version_too_new}
      assert range_result == {:error, :version_too_new}
      assert manager_after_range.waiting_fetches == %{}
    end

    test "waitlisting does not leak :waitlist_timing process-dictionary entries", %{
      manager: manager,
      context: context
    } do
      {_manager, :ok} =
        Reading.handle_get(manager, context, "key1", v2(), wait_ms: 5_000, reply_fn: fn _ -> :ok end)

      timing_keys =
        Enum.filter(Process.get_keys(), fn
          {:waitlist_timing, _} -> true
          _ -> false
        end)

      assert timing_keys == []
    end

    test "without wait_ms the error is returned immediately", %{manager: manager, context: context} do
      assert {^manager, {:error, :version_too_new}} =
               Reading.handle_get(manager, context, "key1", v2(), [])

      assert {^manager, {:error, :version_too_new}} =
               Reading.handle_get_range(manager, context, "range:a", "range:z", v2(), [])
    end
  end

  describe "notify_waiting_fetches/3" do
    test "delivers results for all four waitlisted request shapes once the version is applied", %{
      manager: manager,
      context: context,
      index_manager: index_manager,
      database: database
    } do
      test_pid = self()
      start_selector = KeySelector.first_greater_or_equal("range:a")
      end_selector = KeySelector.first_greater_or_equal("range:d")

      {manager, :ok} =
        Reading.handle_get(manager, context, "key1", v2(),
          wait_ms: 5_000,
          reply_fn: fn result -> send(test_pid, {:get_binary, result}) end
        )

      {manager, :ok} =
        Reading.handle_get(manager, context, KeySelector.first_greater_or_equal("key4"), v2(),
          wait_ms: 5_000,
          reply_fn: fn result -> send(test_pid, {:get_selector, result}) end
        )

      {manager, :ok} =
        Reading.handle_get_range(manager, context, "range:a", "range:z", v2(),
          wait_ms: 5_000,
          reply_fn: fn result -> send(test_pid, {:range_binary, result}) end
        )

      {manager, :ok} =
        Reading.handle_get_range(manager, context, start_selector, end_selector, v2(),
          wait_ms: 5_000,
          reply_fn: fn result -> send(test_pid, {:range_selector, result}) end
        )

      assert length(manager.waiting_fetches[v2()]) == 4

      new_context = advance_to_v2(index_manager, database)
      notified_manager = Reading.notify_waiting_fetches(manager, new_context, v2())

      assert notified_manager.waiting_fetches == %{}
      # Each waitlisted fetch had a reply_fn, so each spawned an async task.
      assert MapSet.size(Reading.get_active_tasks(notified_manager)) == 4

      assert_receive {:get_binary, {:ok, "value1"}}
      assert_receive {:get_selector, {:ok, {"key4", "value4"}}}
      assert_receive {:range_binary, {:ok, {range_results, false}}}
      assert_receive {:range_selector, {:ok, {selector_results, false}}}

      assert length(range_results) == 4
      assert length(selector_results) == 3
    end

    test "delivers the error directly when a waitlisted fetch cannot be resolved", %{
      manager: manager,
      context: context
    } do
      test_pid = self()

      {manager, :ok} =
        Reading.handle_get(manager, context, "key1", v2(),
          wait_ms: 5_000,
          reply_fn: fn result -> send(test_pid, {:stale_get_binary, result}) end
        )

      {manager, :ok} =
        Reading.handle_get(manager, context, KeySelector.first_greater_or_equal("key1"), v2(),
          wait_ms: 5_000,
          reply_fn: fn result -> send(test_pid, {:stale_get_selector, result}) end
        )

      {manager, :ok} =
        Reading.handle_get_range(manager, context, "range:a", "range:z", v2(),
          wait_ms: 5_000,
          reply_fn: fn result -> send(test_pid, {:stale_range_binary, result}) end
        )

      {manager, :ok} =
        Reading.handle_get_range(
          manager,
          context,
          KeySelector.first_greater_or_equal("range:a"),
          KeySelector.first_greater_or_equal("range:d"),
          v2(),
          wait_ms: 5_000,
          reply_fn: fn result -> send(test_pid, {:stale_range_selector, result}) end
        )

      # Notify with a context that still doesn't have version 2: the fetch
      # resolution fails and the error goes straight to each reply_fn without
      # spawning a task.
      notified_manager = Reading.notify_waiting_fetches(manager, context, v2())

      assert notified_manager.waiting_fetches == %{}
      assert Reading.get_active_tasks(notified_manager) == MapSet.new()
      assert_receive {:stale_get_binary, {:error, :version_too_new}}
      assert_receive {:stale_get_selector, {:error, :version_too_new}}
      assert_receive {:stale_range_binary, {:error, :version_too_new}}
      assert_receive {:stale_range_selector, {:error, :version_too_new}}
    end
  end

  describe "shutdown/1" do
    test "notifies all waitlisted entries with {:error, :shutting_down}", %{manager: manager, context: context} do
      test_pid = self()

      {manager, :ok} =
        Reading.handle_get(manager, context, "key1", v2(),
          wait_ms: 5_000,
          reply_fn: fn result -> send(test_pid, {:waiter_one, result}) end
        )

      {manager, :ok} =
        Reading.handle_get_range(manager, context, "range:a", "range:z", v2(),
          wait_ms: 5_000,
          reply_fn: fn result -> send(test_pid, {:waiter_two, result}) end
        )

      assert :ok = Reading.shutdown(manager)
      assert_receive {:waiter_one, {:error, :shutting_down}}
      assert_receive {:waiter_two, {:error, :shutting_down}}
    end

    test "waits for active tasks to finish and emits shutdown telemetry", %{
      manager: manager,
      context: context
    } do
      test_pid = self()
      handler_id = "reading-shutdown-test-#{System.unique_integer([:positive])}"

      :telemetry.attach_many(
        handler_id,
        [[:bedrock, :materializer, :shutdown_waiting], [:bedrock, :materializer, :shutdown_timeout]],
        fn event, measurements, _metadata, _config -> send(test_pid, {:telemetry, event, measurements}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler_id) end)

      {manager, :ok} =
        Reading.handle_get(manager, context, "key1", v1(),
          reply_fn: fn result -> send(test_pid, {:task_one, result}) end
        )

      {manager, :ok} =
        Reading.handle_get(manager, context, "key2", v1(),
          reply_fn: fn result -> send(test_pid, {:task_two, result}) end
        )

      assert MapSet.size(Reading.get_active_tasks(manager)) == 2

      # The tasks are short-lived; shutdown drains their DOWN messages
      # (monitors were established in this process) and returns :ok.
      assert :ok = Reading.shutdown(manager)

      assert_receive {:task_one, {:ok, "value1"}}
      assert_receive {:task_two, {:ok, "value2"}}
      assert_receive {:telemetry, [:bedrock, :materializer, :shutdown_waiting], %{task_count: 2}}
      refute_receive {:telemetry, [:bedrock, :materializer, :shutdown_timeout], _}, 10
    end

    test "returns :ok immediately when there are no tasks or waiters", %{manager: manager} do
      assert :ok = Reading.shutdown(manager)
    end
  end

  describe "immediate errors other than :version_too_new" do
    test "returns the error and leaves the manager unchanged", %{manager: manager, context: context} do
      assert {^manager, {:error, error}} = Reading.handle_get(manager, context, "key1", Version.zero(), [])
      assert error in [:version_too_old, :not_found]

      # Even with wait_ms and a reply_fn, a non-:version_too_new error (here
      # an invalid range) is returned immediately without waitlisting.
      start_selector = KeySelector.first_greater_or_equal("z")
      end_selector = KeySelector.first_greater_or_equal("a")

      assert {^manager, {:error, range_error}} =
               Reading.handle_get_range(manager, context, start_selector, end_selector, v1(),
                 wait_ms: 5_000,
                 reply_fn: fn _ -> :ok end
               )

      assert range_error in [:invalid_range, :not_found]
    end
  end
end
