defmodule Bedrock.JobQueue.StoreTest do
  use ExUnit.Case, async: true

  import Bedrock.JobQueue.Test.StoreHelpers
  import Mox

  alias Bedrock.Encoding.Tuple, as: TupleEncoding
  alias Bedrock.JobQueue.Item
  alias Bedrock.JobQueue.Lease
  alias Bedrock.JobQueue.QueueLease
  alias Bedrock.JobQueue.Store
  alias Bedrock.Keyspace

  setup :verify_on_exit!

  # Stub transact to execute callbacks immediately
  setup do
    stub(MockRepo, :transact, fn callback -> callback.() end)
    :ok
  end

  defp root, do: Keyspace.new("job_queue/")

  # ============================================================================
  # Pure tests (no repo needed)
  # ============================================================================

  describe "queue_keyspaces/2" do
    test "creates keyspaces with proper structure" do
      keyspaces = Store.queue_keyspaces(root(), "tenant_1")

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
      pointers = Store.pointer_keyspace(root())

      assert pointers.key_encoding == TupleEncoding
      assert String.contains?(Keyspace.prefix(pointers), "pointers/")
    end
  end

  describe "tuple key ordering" do
    test "item keys preserve priority ordering" do
      keyspaces = Store.queue_keyspaces(root(), "tenant_1")

      # Keys are packed with the keyspace prefix
      high_priority_key = Keyspace.pack(keyspaces.items, {10, 1000, <<1>>})
      low_priority_key = Keyspace.pack(keyspaces.items, {100, 1000, <<1>>})

      assert high_priority_key < low_priority_key
    end

    test "item keys with same priority sort by vesting_time" do
      keyspaces = Store.queue_keyspaces(root(), "tenant_1")

      earlier = Keyspace.pack(keyspaces.items, {100, 1000, <<1>>})
      later = Keyspace.pack(keyspaces.items, {100, 2000, <<1>>})

      assert earlier < later
    end

    test "pointer keys sort by vesting_time" do
      pointers = Store.pointer_keyspace(root())

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
      ks = Store.queue_lease_keyspace(root())

      assert String.contains?(Keyspace.prefix(ks), "queue_leases/")
    end
  end

  # ============================================================================
  # Mox-based Store operation tests
  # ============================================================================

  describe "obtain_queue_lease/5" do
    test "succeeds on empty queue" do
      MockRepo
      |> expect_queue_lease_get(nil)
      |> expect_queue_lease_put()

      result = Store.obtain_queue_lease(MockRepo, root(), "tenant_1", "holder", 5000)

      assert {:ok, %QueueLease{queue_id: "tenant_1", holder: "holder"}} = result
    end

    test "fails when queue already leased" do
      # Create a non-expired lease
      existing = QueueLease.new("tenant_1", "holder1", 5000)
      encoded = :erlang.term_to_binary(existing)

      expect_queue_lease_get(MockRepo, encoded)
      result = Store.obtain_queue_lease(MockRepo, root(), "tenant_1", "holder2", 5000)

      assert {:error, :queue_leased} = result
    end

    test "succeeds after previous lease expires" do
      # Create an expired lease
      expired = %{QueueLease.new("tenant_1", "holder1", 5000) | expires_at: System.system_time(:millisecond) - 1000}
      encoded = :erlang.term_to_binary(expired)

      MockRepo
      |> expect_queue_lease_get(encoded)
      |> expect_queue_lease_put()

      result = Store.obtain_queue_lease(MockRepo, root(), "tenant_1", "holder2", 5000)

      assert {:ok, %QueueLease{holder: "holder2"}} = result
    end
  end

  describe "release_queue_lease/3" do
    test "removes the lease" do
      lease = QueueLease.new("tenant_1", "holder", 5000)
      encoded = :erlang.term_to_binary(lease)

      MockRepo
      |> expect_queue_lease_get(encoded)
      |> expect_queue_lease_clear()

      result = Store.release_queue_lease(MockRepo, root(), lease)

      assert :ok = result
    end

    test "fails with mismatched lease" do
      stored = QueueLease.new("tenant_1", "holder1", 5000)
      encoded = :erlang.term_to_binary(stored)

      # Try to release with different lease ID
      fake = QueueLease.new("tenant_1", "attacker", 5000)

      expect_queue_lease_get(MockRepo, encoded)
      result = Store.release_queue_lease(MockRepo, root(), fake)

      assert {:error, :lease_mismatch} = result
    end

    test "fails when lease not found" do
      lease = QueueLease.new("tenant_1", "holder", 5000)

      expect_queue_lease_get(MockRepo, nil)
      result = Store.release_queue_lease(MockRepo, root(), lease)

      assert {:error, :lease_not_found} = result
    end
  end

  describe "enqueue/3" do
    test "writes item, updates pointer, increments stats" do
      item = Item.new("tenant_1", "topic", %{n: 1})

      expect_enqueue(MockRepo)
      result = Store.enqueue(MockRepo, root(), item)

      assert :ok = result
    end
  end

  describe "dequeue/5" do
    test "returns empty list when no visible items" do
      expect_dequeue_empty(MockRepo)
      result = Store.dequeue(MockRepo, root(), "tenant_1", "holder", limit: 5, lease_duration: 5000)

      assert {:ok, []} = result
    end
  end
end
