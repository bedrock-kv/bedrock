defmodule Bedrock.JobQueue.StoreTest do
  # Not async due to MockRepo using global Agent
  use ExUnit.Case, async: false

  alias Bedrock.Encoding.Tuple, as: TupleEncoding
  alias Bedrock.JobQueue.Item
  alias Bedrock.JobQueue.Lease
  alias Bedrock.JobQueue.QueueLease
  alias Bedrock.JobQueue.Store
  alias Bedrock.Keyspace

  describe "queue_keyspaces/2" do
    test "creates keyspaces with proper structure" do
      root = Keyspace.new("job_queue/")
      keyspaces = Store.queue_keyspaces(root, "tenant_1")

      assert %{items: items, leases: leases, stats: stats} = keyspaces
      assert items.key_encoding == TupleEncoding
      assert leases.key_encoding == nil
      assert stats.key_encoding == nil

      # Verify prefix contains expected path components
      assert String.contains?(Keyspace.prefix(items), "items/")
      assert String.contains?(Keyspace.prefix(leases), "leases/")
      assert String.contains?(Keyspace.prefix(stats), "stats/")
    end
  end

  describe "pointer_keyspace/1" do
    test "creates pointer keyspace with tuple encoding" do
      root = Keyspace.new("job_queue/")
      pointers = Store.pointer_keyspace(root)

      assert pointers.key_encoding == TupleEncoding
      assert String.contains?(Keyspace.prefix(pointers), "pointers/")
    end
  end

  describe "tuple key ordering" do
    test "item keys preserve priority ordering" do
      root = Keyspace.new("job_queue/")
      keyspaces = Store.queue_keyspaces(root, "tenant_1")

      # Keys are packed with the keyspace prefix
      high_priority_key = Keyspace.pack(keyspaces.items, {10, 1000, <<1>>})
      low_priority_key = Keyspace.pack(keyspaces.items, {100, 1000, <<1>>})

      assert high_priority_key < low_priority_key
    end

    test "item keys with same priority sort by vesting_time" do
      root = Keyspace.new("job_queue/")
      keyspaces = Store.queue_keyspaces(root, "tenant_1")

      earlier = Keyspace.pack(keyspaces.items, {100, 1000, <<1>>})
      later = Keyspace.pack(keyspaces.items, {100, 2000, <<1>>})

      assert earlier < later
    end

    test "pointer keys sort by vesting_time" do
      root = Keyspace.new("job_queue/")
      pointers = Store.pointer_keyspace(root)

      earlier = Keyspace.pack(pointers, {1000, "tenant_a"})
      later = Keyspace.pack(pointers, {2000, "tenant_b"})

      assert earlier < later
    end
  end

  describe "Item visibility" do
    test "items with past vesting_time and no lease are visible" do
      past = System.system_time(:millisecond) - 1000
      item = Item.new("queue", "topic", %{}, vesting_time: past)

      assert Item.visible?(item)
    end

    test "items with future vesting_time are not visible" do
      future = System.system_time(:millisecond) + 10_000
      item = Item.new("queue", "topic", %{}, vesting_time: future)

      refute Item.visible?(item)
    end

    test "items with lease are not visible" do
      past = System.system_time(:millisecond) - 1000
      item = %{Item.new("queue", "topic", %{}, vesting_time: past) | lease_id: <<1, 2, 3>>}

      refute Item.visible?(item)
    end
  end

  describe "Lease creation" do
    test "creates lease with duration" do
      item = Item.new("queue", "topic", %{})
      lease = Lease.new(item, "holder", 5000)

      assert lease.item_id == item.id
      assert lease.queue_id == item.queue_id
      assert lease.holder == "holder"
      assert lease.expires_at > lease.obtained_at
      assert lease.expires_at - lease.obtained_at >= 5000
    end

    test "creates lease with default duration" do
      item = Item.new("queue", "topic", %{})
      lease = Lease.new(item, "holder")

      # Default is 30 seconds
      assert lease.expires_at - lease.obtained_at >= 30_000
    end

    test "stores item_key for O(1) lookup" do
      item = Item.new("queue", "topic", %{}, priority: 50)
      lease = Lease.new(item, "holder", 5000)

      # item_key should be {priority, new_vesting_time, id}
      {priority, vesting_time, id} = lease.item_key
      assert priority == 50
      assert vesting_time == lease.expires_at
      assert id == item.id
    end
  end

  describe "Lease expiration" do
    test "fresh lease is not expired" do
      item = Item.new("queue", "topic", %{})
      lease = Lease.new(item, "holder", 5000)

      refute Lease.expired?(lease)
      assert Lease.remaining_ms(lease) > 0
    end

    test "expired lease reports expired" do
      item = Item.new("queue", "topic", %{})
      # Create a lease that's already expired
      lease = %{Lease.new(item, "holder", 5000) | expires_at: System.system_time(:millisecond) - 1000}

      assert Lease.expired?(lease)
      assert Lease.remaining_ms(lease) == 0
    end
  end

  describe "QueueLease creation" do
    test "creates queue lease with duration" do
      lease = QueueLease.new("tenant_1", "holder_123", 5000)

      assert lease.queue_id == "tenant_1"
      assert lease.holder == "holder_123"
      assert lease.expires_at > lease.obtained_at
      assert lease.expires_at - lease.obtained_at >= 5000
      assert is_binary(lease.id) and byte_size(lease.id) == 16
    end

    test "creates queue lease with default duration" do
      lease = QueueLease.new("tenant_1", "holder")

      # Default is 5 seconds
      assert lease.expires_at - lease.obtained_at >= 5000
    end
  end

  describe "QueueLease expiration" do
    test "fresh queue lease is not expired" do
      lease = QueueLease.new("tenant_1", "holder", 5000)

      refute QueueLease.expired?(lease)
      assert QueueLease.remaining_ms(lease) > 0
    end

    test "expired queue lease reports expired" do
      lease = %{QueueLease.new("tenant_1", "holder", 5000) | expires_at: System.system_time(:millisecond) - 1000}

      assert QueueLease.expired?(lease)
      assert QueueLease.remaining_ms(lease) == 0
    end
  end

  describe "queue_lease_keyspace/1" do
    test "creates queue lease keyspace" do
      root = Keyspace.new("job_queue/")
      ks = Store.queue_lease_keyspace(root)

      assert String.contains?(Keyspace.prefix(ks), "queue_leases/")
    end
  end

  # Integration tests using MockRepo for two-tier leasing
  defmodule MockRepo do
    @moduledoc false
    # Simple in-memory mock for testing Store operations

    def start_link do
      Agent.start_link(fn -> %{} end, name: __MODULE__)
    end

    def stop do
      case Process.whereis(__MODULE__) do
        nil -> :ok
        pid -> Agent.stop(pid)
      end
    catch
      :exit, _ -> :ok
    end

    def reset do
      Agent.update(__MODULE__, fn _ -> %{} end)
    end

    def transact(fun) do
      fun.()
    end

    def put(keyspace, key, value) when is_struct(keyspace) do
      full_key = Keyspace.pack(keyspace, key)
      Agent.update(__MODULE__, &Map.put(&1, full_key, value))
    end

    def put(key, value) when is_binary(key) do
      Agent.update(__MODULE__, &Map.put(&1, key, value))
    end

    def get(keyspace, key) when is_struct(keyspace) do
      full_key = Keyspace.pack(keyspace, key)
      Agent.get(__MODULE__, &Map.get(&1, full_key))
    end

    def get(key) when is_binary(key) do
      Agent.get(__MODULE__, &Map.get(&1, key))
    end

    def clear(keyspace, key) when is_struct(keyspace) do
      full_key = Keyspace.pack(keyspace, key)
      Agent.update(__MODULE__, &Map.delete(&1, full_key))
    end

    def clear(key) when is_binary(key) do
      Agent.update(__MODULE__, &Map.delete(&1, key))
    end

    def get_range(key_or_keyspace, opts \\ [])

    def get_range(keyspace, opts) when is_struct(keyspace) do
      prefix = Keyspace.prefix(keyspace)
      limit = Keyword.get(opts, :limit, 100)

      Agent.get(__MODULE__, fn state ->
        state
        |> Enum.filter(fn {k, _v} -> String.starts_with?(k, prefix) end)
        |> Enum.sort_by(fn {k, _v} -> k end)
        |> Enum.take(limit)
      end)
    end

    def get_range({start_key, end_key}, opts) do
      limit = Keyword.get(opts, :limit, 100)

      Agent.get(__MODULE__, fn state ->
        state
        |> Enum.filter(fn {k, _v} -> k >= start_key and k < end_key end)
        |> Enum.sort_by(fn {k, _v} -> k end)
        |> Enum.take(limit)
      end)
    end

    def min(key, value) do
      Agent.update(__MODULE__, fn state ->
        case Map.get(state, key) do
          nil -> Map.put(state, key, value)
          existing when existing <= value -> state
          _ -> Map.put(state, key, value)
        end
      end)
    end

    def add(key, <<delta::64-signed-little>>) do
      Agent.update(__MODULE__, fn state ->
        current = Map.get(state, key, <<0::64-little>>)
        <<current_val::64-signed-little>> = current
        new_val = current_val + delta
        Map.put(state, key, <<new_val::64-signed-little>>)
      end)
    end
  end

  describe "two-tier leasing with MockRepo" do
    @describetag :capture_log

    setup do
      # Ensure clean state before each test
      MockRepo.stop()
      {:ok, _} = MockRepo.start_link()
      :ok
    end

    test "obtain_queue_lease succeeds on empty queue" do
      root = Keyspace.new("job_queue/")

      result = Store.obtain_queue_lease(MockRepo, root, "tenant_1", "holder", 5000)

      assert {:ok, %QueueLease{queue_id: "tenant_1"}} = result
    end

    test "obtain_queue_lease fails when queue already leased" do
      root = Keyspace.new("job_queue/")

      {:ok, _lease1} = Store.obtain_queue_lease(MockRepo, root, "tenant_1", "holder1", 5000)
      result = Store.obtain_queue_lease(MockRepo, root, "tenant_1", "holder2", 5000)

      assert {:error, :queue_leased} = result
    end

    test "obtain_queue_lease succeeds after previous lease expires" do
      root = Keyspace.new("job_queue/")

      # Create an already-expired lease directly
      ks = Store.queue_lease_keyspace(root)

      expired_lease = %{
        QueueLease.new("tenant_1", "holder1", 5000)
        | expires_at: System.system_time(:millisecond) - 1000
      }

      MockRepo.put(ks, "tenant_1", :erlang.term_to_binary(expired_lease))

      # New lease should succeed
      result = Store.obtain_queue_lease(MockRepo, root, "tenant_1", "holder2", 5000)

      assert {:ok, %QueueLease{holder: "holder2"}} = result
    end

    test "release_queue_lease removes the lease" do
      root = Keyspace.new("job_queue/")

      {:ok, lease} = Store.obtain_queue_lease(MockRepo, root, "tenant_1", "holder", 5000)
      :ok = Store.release_queue_lease(MockRepo, root, lease)

      # Should be able to obtain again
      {:ok, new_lease} = Store.obtain_queue_lease(MockRepo, root, "tenant_1", "holder2", 5000)
      assert new_lease.holder == "holder2"
    end

    test "release_queue_lease fails with mismatched lease" do
      root = Keyspace.new("job_queue/")

      {:ok, _lease} = Store.obtain_queue_lease(MockRepo, root, "tenant_1", "holder", 5000)

      # Create a fake lease with different ID
      fake_lease = QueueLease.new("tenant_1", "attacker", 5000)
      result = Store.release_queue_lease(MockRepo, root, fake_lease)

      assert {:error, :lease_mismatch} = result
    end

    test "dequeue returns empty list when no visible items" do
      root = Keyspace.new("job_queue/")

      result = Store.dequeue(MockRepo, root, "tenant_1", "holder", limit: 5, lease_duration: 5000)

      assert {:ok, []} = result
    end

    test "dequeue returns items with leases" do
      root = Keyspace.new("job_queue/")

      # Enqueue some items
      item1 = Item.new("tenant_1", "topic", %{n: 1})
      item2 = Item.new("tenant_1", "topic", %{n: 2})
      Store.enqueue(MockRepo, root, item1)
      Store.enqueue(MockRepo, root, item2)

      result = Store.dequeue(MockRepo, root, "tenant_1", "holder", limit: 5, lease_duration: 5000)

      assert {:ok, leases} = result
      assert length(leases) == 2
      assert Enum.all?(leases, &match?(%Lease{}, &1))
    end

    test "dequeue respects limit" do
      root = Keyspace.new("job_queue/")

      # Enqueue 5 items
      for i <- 1..5 do
        item = Item.new("tenant_1", "topic", %{n: i})
        Store.enqueue(MockRepo, root, item)
      end

      result = Store.dequeue(MockRepo, root, "tenant_1", "holder", limit: 2, lease_duration: 5000)

      assert {:ok, leases} = result
      assert length(leases) == 2
    end
  end
end
