defmodule Bedrock.JobQueue.Test.StoreHelpers do
  @moduledoc """
  Shared helper functions for job queue Store tests.
  Provides pipeable Mox expectations for common operations.

  ## Usage

      import Bedrock.JobQueue.Test.StoreHelpers

      setup do
        stub(MockRepo, :transact, fn callback -> callback.() end)
        :ok
      end

      test "obtain queue lease succeeds on empty queue" do
        MockRepo
        |> expect_queue_lease_get(nil)
        |> expect_queue_lease_put()

        assert {:ok, %QueueLease{}} = Store.obtain_queue_lease(MockRepo, root, "q", "holder", 5000)
      end
  """

  import Mox

  alias Bedrock.Keyspace

  # ============================================================================
  # Queue Lease Operations
  # ============================================================================

  @doc """
  Expects a get on the queue_leases keyspace, returning the given value.
  """
  def expect_queue_lease_get(repo, value) do
    expect(repo, :get, fn %Keyspace{}, _queue_id -> value end)
  end

  @doc """
  Expects a put on the queue_leases keyspace.
  """
  def expect_queue_lease_put(repo) do
    expect(repo, :put, fn %Keyspace{}, _queue_id, _value -> :ok end)
  end

  @doc """
  Expects a clear on the queue_leases keyspace.
  """
  def expect_queue_lease_clear(repo) do
    expect(repo, :clear, fn %Keyspace{}, _queue_id -> :ok end)
  end

  # ============================================================================
  # Enqueue Operations
  # ============================================================================

  @doc """
  Expects the full sequence of calls for Store.enqueue/3.
  """
  def expect_enqueue(repo) do
    repo
    |> expect(:put, fn %Keyspace{}, _item_key, _encoded_item -> :ok end)
    |> expect(:min, fn _pointer_key, _value -> :ok end)
    |> expect(:add, fn _stats_key, <<1::64-little>> -> :ok end)
  end

  # ============================================================================
  # Peek Operations
  # ============================================================================

  @doc """
  Expects a get_range on the items keyspace, returning the given items.

  Items should be a list of `{key, encoded_value}` tuples.
  """
  def expect_peek(repo, items) do
    expect(repo, :get_range, fn %Keyspace{}, _opts -> items end)
  end

  # ============================================================================
  # Dequeue Operations (peek + obtain_lease)
  # ============================================================================

  @doc """
  Expects the full sequence for dequeue with no items visible.
  """
  def expect_dequeue_empty(repo) do
    expect_peek(repo, [])
  end

  @doc """
  Expects get on items keyspace for lease verification.
  """
  def expect_item_get(repo, value) do
    expect(repo, :get, fn %Keyspace{}, _item_key -> value end)
  end

  @doc """
  Expects the writes for obtaining a lease on an item:
  - clear old item key
  - put new item (with updated vesting_time/lease_id)
  - put lease record
  - min pointer
  - add stats (pending -1, processing +1)
  """
  def expect_obtain_lease_writes(repo) do
    repo
    |> expect(:clear, fn %Keyspace{}, _old_item_key -> :ok end)
    |> expect(:put, fn %Keyspace{}, _new_item_key, _encoded_item -> :ok end)
    |> expect(:put, fn %Keyspace{}, _lease_key, _encoded_lease -> :ok end)
    |> expect(:min, fn _pointer_key, _value -> :ok end)
    |> expect(:add, fn _key, <<-1::64-signed-little>> -> :ok end)
    |> expect(:add, fn _key, <<1::64-little>> -> :ok end)
  end

  # ============================================================================
  # Complete Operations
  # ============================================================================

  @doc """
  Expects a get on the leases keyspace for lease verification.
  """
  def expect_lease_get(repo, value) do
    expect(repo, :get, fn %Keyspace{}, _item_id -> value end)
  end

  @doc """
  Creates a stateful mock store for integration tests.

  This is needed because Store.obtain_lease creates a lease with a fresh UUID,
  and Store.complete/requeue needs to find that same lease. Simple Mox expects
  can't capture the dynamically created lease.

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

    stub(repo, :min, fn _key, _value -> :ok end)
    stub(repo, :add, fn _key, _value -> :ok end)

    stub(repo, :get_range, fn %Keyspace{} = ks, _opts ->
      prefix = Keyspace.prefix(ks)
      get_items_by_prefix(store_agent, prefix)
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
  Expects the writes for completing a job:
  - clear item
  - clear lease
  - add stats (processing -1)
  """
  def expect_complete_writes(repo) do
    repo
    |> expect(:clear, fn %Keyspace{}, _item_key -> :ok end)
    |> expect(:clear, fn %Keyspace{}, _lease_key -> :ok end)
    |> expect(:add, fn _key, <<-1::64-signed-little>> -> :ok end)
  end

  # ============================================================================
  # Requeue Operations
  # ============================================================================

  @doc """
  Expects the writes for requeuing a job:
  - clear old item
  - put new item (with updated vesting_time, error_count)
  - min pointer
  - clear lease
  - add stats (pending +1, processing -1)
  """
  def expect_requeue_writes(repo) do
    repo
    |> expect(:clear, fn %Keyspace{}, _old_item_key -> :ok end)
    |> expect(:put, fn %Keyspace{}, _new_item_key, _encoded_item -> :ok end)
    |> expect(:min, fn _pointer_key, _value -> :ok end)
    |> expect(:clear, fn %Keyspace{}, _lease_key -> :ok end)
    |> expect(:add, fn _key, <<1::64-little>> -> :ok end)
    |> expect(:add, fn _key, <<-1::64-signed-little>> -> :ok end)
  end

  # ============================================================================
  # Extend Lease Operations
  # ============================================================================

  @doc """
  Expects the writes for extending a lease:
  - clear old item
  - put new item (with updated vesting_time)
  - put updated lease
  - min pointer
  """
  def expect_extend_lease_writes(repo) do
    repo
    |> expect(:clear, fn %Keyspace{}, _old_item_key -> :ok end)
    |> expect(:put, fn %Keyspace{}, _new_item_key, _encoded_item -> :ok end)
    |> expect(:put, fn %Keyspace{}, _lease_key, _encoded_lease -> :ok end)
    |> expect(:min, fn _pointer_key, _value -> :ok end)
  end
end
