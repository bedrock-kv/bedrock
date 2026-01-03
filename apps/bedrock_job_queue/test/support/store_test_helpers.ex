defmodule Bedrock.JobQueue.Test.StoreHelpers do
  @moduledoc """
  Shared helper functions for job queue Store tests.
  Provides pipeable Mox expectations for common operations with parameter verification.

  ## Usage

      import Bedrock.JobQueue.Test.StoreHelpers

      setup do
        stub(MockRepo, :transact, fn callback -> callback.() end)
        :ok
      end

      test "obtain queue lease succeeds on empty queue" do
        MockRepo
        |> expect_queue_lease_get("tenant_1", nil)
        |> expect_queue_lease_put("tenant_1")

        assert {:ok, %QueueLease{}} = Store.obtain_queue_lease(MockRepo, root, "tenant_1", "holder", 5000)
      end

  """

  import ExUnit.Assertions
  import Mox

  alias Bedrock.JobQueue.Item
  alias Bedrock.JobQueue.Lease
  alias Bedrock.JobQueue.QueueLease
  alias Bedrock.Keyspace

  # ============================================================================
  # Queue Lease Operations
  # ============================================================================

  @doc """
  Expects a get on the queue_leases keyspace for a specific queue_id.
  Verifies the keyspace contains "queue_leases/" and the queue_id matches.
  """
  def expect_queue_lease_get(repo, queue_id, value) do
    expect(repo, :get, fn %Keyspace{} = ks, key ->
      assert String.contains?(Keyspace.prefix(ks), "queue_leases/"),
             "Expected queue_leases keyspace, got: #{Keyspace.prefix(ks)}"

      assert key == queue_id, "Expected queue_id #{inspect(queue_id)}, got: #{inspect(key)}"
      value
    end)
  end

  @doc """
  Expects a put on the queue_leases keyspace with verification.
  Verifies the keyspace, queue_id, and that value is a valid encoded QueueLease.
  """
  def expect_queue_lease_put(repo, queue_id) do
    expect(repo, :put, fn %Keyspace{} = ks, key, value ->
      assert String.contains?(Keyspace.prefix(ks), "queue_leases/"),
             "Expected queue_leases keyspace, got: #{Keyspace.prefix(ks)}"

      assert key == queue_id, "Expected queue_id #{inspect(queue_id)}, got: #{inspect(key)}"

      # Verify value is a valid encoded QueueLease
      lease = :erlang.binary_to_term(value)
      assert %QueueLease{queue_id: ^queue_id} = lease
      :ok
    end)
  end

  @doc """
  Expects a clear on the queue_leases keyspace.
  """
  def expect_queue_lease_clear(repo, queue_id) do
    expect(repo, :clear, fn %Keyspace{} = ks, key ->
      assert String.contains?(Keyspace.prefix(ks), "queue_leases/"),
             "Expected queue_leases keyspace, got: #{Keyspace.prefix(ks)}"

      assert key == queue_id, "Expected queue_id #{inspect(queue_id)}, got: #{inspect(key)}"
      :ok
    end)
  end

  # ============================================================================
  # Enqueue Operations
  # ============================================================================

  @doc """
  Expects the full sequence of calls for Store.enqueue/3 with verification.
  Verifies item key structure, keyspace prefixes, and stats increments.
  """
  def expect_enqueue(repo, %Item{} = item) do
    expected_key = Item.key(item)

    repo
    |> expect(:put, fn %Keyspace{} = ks, key, value ->
      assert String.contains?(Keyspace.prefix(ks), "items/"),
             "Expected items keyspace, got: #{Keyspace.prefix(ks)}"

      assert key == expected_key,
             "Expected item key #{inspect(expected_key)}, got: #{inspect(key)}"

      # Verify value decodes to matching item
      decoded = :erlang.binary_to_term(value)
      assert decoded.id == item.id
      assert decoded.topic == item.topic
      assert decoded.queue_id == item.queue_id
      :ok
    end)
    |> expect(:max, fn pointer_key, <<_timestamp::64-little>> ->
      assert is_binary(pointer_key), "Expected binary pointer key"
      assert String.contains?(pointer_key, "pointers/"), "Expected pointers keyspace in key"
      :ok
    end)
    |> expect(:add, fn stats_key, <<1::64-little>> ->
      assert is_binary(stats_key), "Expected binary stats key"
      assert String.contains?(stats_key, "stats/"), "Expected stats keyspace in key"
      assert String.contains?(stats_key, "pending"), "Expected pending stats key"
      :ok
    end)
  end

  # ============================================================================
  # Peek Operations
  # ============================================================================

  @doc """
  Expects a get_range on the items keyspace for a specific queue_id.
  Verifies the keyspace contains items path and returns the given items.

  Items should be a list of `{key, encoded_value}` tuples.
  """
  def expect_peek(repo, queue_id, items) do
    expect(repo, :get_range, fn %Keyspace{} = ks, opts ->
      prefix = Keyspace.prefix(ks)

      assert String.contains?(prefix, "queues/#{queue_id}/items/"),
             "Expected items keyspace for queue #{queue_id}, got: #{prefix}"

      assert is_list(opts), "Expected opts to be a list"
      items
    end)
  end

  # ============================================================================
  # Dequeue Operations (peek + obtain_lease)
  # ============================================================================

  @doc """
  Expects the full sequence for dequeue with no items visible.
  """
  def expect_dequeue_empty(repo, queue_id) do
    expect_peek(repo, queue_id, [])
  end

  @doc """
  Expects get on items keyspace for a specific item.
  Verifies keyspace contains items path and key structure.
  """
  def expect_item_get(repo, queue_id, %Item{} = item, return_value) do
    expected_key = Item.key(item)

    expect(repo, :get, fn %Keyspace{} = ks, key ->
      prefix = Keyspace.prefix(ks)

      assert String.contains?(prefix, "queues/#{queue_id}/items/"),
             "Expected items keyspace for queue #{queue_id}, got: #{prefix}"

      assert key == expected_key,
             "Expected item key #{inspect(expected_key)}, got: #{inspect(key)}"

      return_value
    end)
  end

  @doc """
  Expects the writes for obtaining a lease on an item.
  Verifies:
  - clear old item key
  - put new item (with updated vesting_time/lease_id)
  - put lease record
  - max pointer (last_active_time)
  - add stats (pending -1, processing +1)
  """
  def expect_obtain_lease_writes(repo, %Item{} = item) do
    old_item_key = Item.key(item)

    repo
    |> expect(:clear, fn %Keyspace{} = ks, key ->
      assert String.contains?(Keyspace.prefix(ks), "items/"),
             "Expected items keyspace, got: #{Keyspace.prefix(ks)}"

      assert key == old_item_key,
             "Expected old item key #{inspect(old_item_key)}, got: #{inspect(key)}"

      :ok
    end)
    |> expect(:put, fn %Keyspace{} = ks, {priority, vesting_time, id}, value ->
      assert String.contains?(Keyspace.prefix(ks), "items/"),
             "Expected items keyspace, got: #{Keyspace.prefix(ks)}"

      # Verify key structure: same id/priority, different vesting_time
      assert priority == item.priority, "Priority should match original item"
      assert id == item.id, "ID should match original item"
      assert vesting_time > item.vesting_time, "New vesting_time should be in future"

      # Verify item has lease_id set
      decoded = :erlang.binary_to_term(value)
      assert decoded.id == item.id
      assert decoded.lease_id != nil, "Item should have lease_id set"
      :ok
    end)
    |> expect(:put, fn %Keyspace{} = ks, lease_key, value ->
      assert String.contains?(Keyspace.prefix(ks), "leases/"),
             "Expected leases keyspace, got: #{Keyspace.prefix(ks)}"

      assert lease_key == item.id, "Lease key should be item.id"

      # Verify lease structure
      lease = :erlang.binary_to_term(value)
      assert %Lease{item_id: item_id} = lease
      assert item_id == item.id
      :ok
    end)
    |> expect(:max, fn pointer_key, <<_timestamp::64-little>> ->
      assert is_binary(pointer_key), "Expected binary pointer key"
      assert String.contains?(pointer_key, "pointers/"), "Expected pointers keyspace"
      :ok
    end)
    |> expect(:add, fn stats_key, <<-1::64-signed-little>> ->
      assert String.contains?(stats_key, "stats/"), "Expected stats keyspace"
      assert String.contains?(stats_key, "pending"), "Expected pending stats key"
      :ok
    end)
    |> expect(:add, fn stats_key, <<1::64-little>> ->
      assert String.contains?(stats_key, "stats/"), "Expected stats keyspace"
      assert String.contains?(stats_key, "processing"), "Expected processing stats key"
      :ok
    end)
  end

  # ============================================================================
  # Complete Operations
  # ============================================================================

  @doc """
  Expects a get on the leases keyspace for lease verification.
  Verifies keyspace contains leases path and item_id matches.
  """
  def expect_lease_get(repo, item_id, value) do
    expect(repo, :get, fn %Keyspace{} = ks, key ->
      assert String.contains?(Keyspace.prefix(ks), "leases/"),
             "Expected leases keyspace, got: #{Keyspace.prefix(ks)}"

      assert key == item_id, "Expected item_id #{inspect(item_id)}, got: #{inspect(key)}"
      value
    end)
  end

  # ============================================================================
  # Stateful Mock Store (for integration tests)
  # ============================================================================

  @doc """
  Creates a stateful mock store for integration tests.

  This enables cross-process mocking where Store operations read/write shared state.
  Returns an Agent that tracks the stored data. Call `setup_integration_stubs/2`
  to wire up the mock.
  """
  def start_mock_store do
    Agent.start_link(fn -> %{} end)
  end

  @doc """
  Sets up stubs for integration tests using the given mock store agent.

  The mock store tracks puts and returns them on gets, simulating real storage.
  Also stubs other operations (clear, min, add, get_range) appropriately.

  The `initial_items` parameter provides items that will be returned by peek/get_range.
  """
  def setup_integration_stubs(repo, store_agent, initial_items \\ []) do
    # Store initial items
    for {key, value} <- initial_items do
      Agent.update(store_agent, &Map.put(&1, key, value))
    end

    stub(repo, :put, fn %Keyspace{} = ks, key, value ->
      storage_key = {Keyspace.prefix(ks), key}
      Agent.update(store_agent, &Map.put(&1, storage_key, value))
      :ok
    end)

    stub(repo, :get, fn %Keyspace{} = ks, key ->
      storage_key = {Keyspace.prefix(ks), key}
      Agent.get(store_agent, &Map.get(&1, storage_key))
    end)

    stub(repo, :clear, fn %Keyspace{} = ks, key ->
      storage_key = {Keyspace.prefix(ks), key}
      Agent.update(store_agent, &Map.delete(&1, storage_key))
      :ok
    end)

    stub(repo, :max, fn _key, _value -> :ok end)
    stub(repo, :add, fn _key, _value -> :ok end)

    # Support Keyspace-based get_range and tuple-based raw key range
    stub(repo, :get_range, fn
      %Keyspace{} = ks, _opts ->
        prefix = Keyspace.prefix(ks)
        get_items_by_prefix(store_agent, prefix)

      # Support tuple-based raw key range {start_key, end_key}
      # Used by pointer cleanup and GC functions
      {start_key, end_key}, _opts when is_binary(start_key) and is_binary(end_key) ->
        get_items_by_range(store_agent, start_key, end_key)
    end)

    store_agent
  end

  defp get_items_by_prefix(store_agent, prefix) do
    Agent.get(store_agent, fn state ->
      state
      |> Enum.filter(fn {{p, _k}, _v} -> p == prefix end)
      |> Enum.map(fn {{_p, k}, v} -> {k, v} end)
      |> Enum.sort()
    end)
  end

  defp get_items_by_range(store_agent, start_key, end_key) do
    Agent.get(store_agent, fn state ->
      state
      |> Enum.filter(&key_in_range?(&1, start_key, end_key))
      |> Enum.map(&extract_key_value/1)
      |> Enum.sort()
    end)
  end

  defp key_in_range?({{prefix, _k}, _v}, start_key, end_key) when is_binary(prefix) do
    prefix >= start_key and prefix < end_key
  end

  defp key_in_range?({key, _v}, start_key, end_key) when is_binary(key) do
    key >= start_key and key < end_key
  end

  defp key_in_range?(_, _, _), do: false

  defp extract_key_value({{prefix, _k}, v}), do: {prefix, v}
  defp extract_key_value({k, v}), do: {k, v}

  @doc """
  Stores an item in the mock store.
  """
  def store_item(store_agent, keyspace, item) do
    key = {item.priority, item.vesting_time, item.id}
    value = :erlang.term_to_binary(item)
    storage_key = {Keyspace.prefix(keyspace), key}
    Agent.update(store_agent, &Map.put(&1, storage_key, value))
  end

  @doc """
  Expects the writes for completing a job.
  Verifies:
  - clear item with correct key
  - clear lease with correct item_id
  - add stats (processing -1)
  """
  def expect_complete_writes(repo, %Lease{} = lease) do
    repo
    |> expect(:clear, fn %Keyspace{} = ks, key ->
      assert String.contains?(Keyspace.prefix(ks), "items/"),
             "Expected items keyspace, got: #{Keyspace.prefix(ks)}"

      # Verify key matches lease's item_key
      assert key == lease.item_key,
             "Expected item_key #{inspect(lease.item_key)}, got: #{inspect(key)}"

      :ok
    end)
    |> expect(:clear, fn %Keyspace{} = ks, key ->
      assert String.contains?(Keyspace.prefix(ks), "leases/"),
             "Expected leases keyspace, got: #{Keyspace.prefix(ks)}"

      assert key == lease.item_id,
             "Expected item_id #{inspect(lease.item_id)}, got: #{inspect(key)}"

      :ok
    end)
    |> expect(:add, fn stats_key, <<-1::64-signed-little>> ->
      assert String.contains?(stats_key, "stats/"), "Expected stats keyspace"
      assert String.contains?(stats_key, "processing"), "Expected processing stats key"
      :ok
    end)
  end

  # ============================================================================
  # Requeue Operations
  # ============================================================================

  @doc """
  Expects the writes for requeuing a job.
  Verifies:
  - clear old item with correct key
  - put new item (with updated vesting_time, error_count)
  - max pointer (last_active_time)
  - clear lease with correct item_id
  - add stats (pending +1, processing -1)
  """
  def expect_requeue_writes(repo, %Lease{} = lease, %Item{} = original_item) do
    repo
    |> expect(:clear, fn %Keyspace{} = ks, key ->
      assert String.contains?(Keyspace.prefix(ks), "items/"),
             "Expected items keyspace, got: #{Keyspace.prefix(ks)}"

      assert key == lease.item_key,
             "Expected item_key #{inspect(lease.item_key)}, got: #{inspect(key)}"

      :ok
    end)
    |> expect(:put, fn %Keyspace{} = ks, {priority, vesting_time, id}, value ->
      assert String.contains?(Keyspace.prefix(ks), "items/"),
             "Expected items keyspace, got: #{Keyspace.prefix(ks)}"

      # Verify key structure: same id/priority, new vesting_time (in future due to backoff)
      assert priority == original_item.priority, "Priority should match original item"
      assert id == original_item.id, "ID should match original item"
      assert vesting_time > original_item.vesting_time, "New vesting_time should be in future"

      # Verify error_count incremented and lease cleared
      decoded = :erlang.binary_to_term(value)
      assert decoded.id == original_item.id
      assert decoded.error_count == original_item.error_count + 1
      assert decoded.lease_id == nil, "lease_id should be cleared"
      :ok
    end)
    |> expect(:max, fn pointer_key, <<_timestamp::64-little>> ->
      assert is_binary(pointer_key), "Expected binary pointer key"
      assert String.contains?(pointer_key, "pointers/"), "Expected pointers keyspace"
      :ok
    end)
    |> expect(:clear, fn %Keyspace{} = ks, key ->
      assert String.contains?(Keyspace.prefix(ks), "leases/"),
             "Expected leases keyspace, got: #{Keyspace.prefix(ks)}"

      assert key == lease.item_id,
             "Expected item_id #{inspect(lease.item_id)}, got: #{inspect(key)}"

      :ok
    end)
    |> expect(:add, fn stats_key, <<1::64-little>> ->
      assert String.contains?(stats_key, "stats/"), "Expected stats keyspace"
      assert String.contains?(stats_key, "pending"), "Expected pending stats key"
      :ok
    end)
    |> expect(:add, fn stats_key, <<-1::64-signed-little>> ->
      assert String.contains?(stats_key, "stats/"), "Expected stats keyspace"
      assert String.contains?(stats_key, "processing"), "Expected processing stats key"
      :ok
    end)
  end

  # ============================================================================
  # Extend Lease Operations
  # ============================================================================

  @doc """
  Expects the writes for extending a lease.
  Verifies:
  - clear old item with correct key
  - put new item (with updated vesting_time)
  - put updated lease
  - max pointer (last_active_time)
  """
  def expect_extend_lease_writes(repo, %Lease{} = lease, %Item{} = original_item) do
    repo
    |> expect(:clear, fn %Keyspace{} = ks, key ->
      assert String.contains?(Keyspace.prefix(ks), "items/"),
             "Expected items keyspace, got: #{Keyspace.prefix(ks)}"

      assert key == lease.item_key,
             "Expected item_key #{inspect(lease.item_key)}, got: #{inspect(key)}"

      :ok
    end)
    |> expect(:put, fn %Keyspace{} = ks, {priority, vesting_time, id}, value ->
      assert String.contains?(Keyspace.prefix(ks), "items/"),
             "Expected items keyspace, got: #{Keyspace.prefix(ks)}"

      # Verify key structure: same id/priority, extended vesting_time
      assert priority == original_item.priority, "Priority should match original item"
      assert id == original_item.id, "ID should match original item"
      assert vesting_time > lease.expires_at, "New vesting_time should be after old expiry"

      decoded = :erlang.binary_to_term(value)
      assert decoded.id == original_item.id
      :ok
    end)
    |> expect(:put, fn %Keyspace{} = ks, key, value ->
      assert String.contains?(Keyspace.prefix(ks), "leases/"),
             "Expected leases keyspace, got: #{Keyspace.prefix(ks)}"

      assert key == lease.item_id,
             "Expected item_id #{inspect(lease.item_id)}, got: #{inspect(key)}"

      # Verify lease has extended expiry
      updated_lease = :erlang.binary_to_term(value)
      assert updated_lease.id == lease.id
      assert updated_lease.expires_at > lease.expires_at, "Lease expiry should be extended"
      :ok
    end)
    |> expect(:max, fn pointer_key, <<_timestamp::64-little>> ->
      assert is_binary(pointer_key), "Expected binary pointer key"
      assert String.contains?(pointer_key, "pointers/"), "Expected pointers keyspace"
      :ok
    end)
  end

  # ============================================================================
  # Async Test Helpers
  # ============================================================================

  @doc """
  Waits until the given function returns a truthy value.

  Polls every `interval` ms for up to `timeout` ms total.
  Returns `:ok` when the condition is met, raises on timeout.

  ## Example

      assert_eventually(fn ->
        Agent.get(store, &Map.get(&1, key)) == nil
      end)
  """
  def assert_eventually(condition, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 200)
    interval = Keyword.get(opts, :interval, 10)
    deadline = System.monotonic_time(:millisecond) + timeout

    do_assert_eventually(condition, deadline, interval)
  end

  defp do_assert_eventually(condition, deadline, interval) do
    if condition.() do
      :ok
    else
      now = System.monotonic_time(:millisecond)

      if now >= deadline do
        raise ExUnit.AssertionError,
          message: "assert_eventually timed out after condition never became truthy"
      else
        Process.sleep(interval)
        do_assert_eventually(condition, deadline, interval)
      end
    end
  end
end
