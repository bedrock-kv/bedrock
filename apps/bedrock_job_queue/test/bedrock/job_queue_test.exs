defmodule Bedrock.JobQueueTest do
  use ExUnit.Case, async: true

  alias Bedrock.JobQueue.Item
  alias Bedrock.JobQueue.Lease

  describe "Item" do
    test "creates a new item with defaults" do
      item = Item.new("tenant_1", "user:created", %{user_id: 123})

      assert item.queue_id == "tenant_1"
      assert item.topic == "user:created"
      assert item.priority == 100
      assert item.error_count == 0
      assert item.max_retries == 3
      assert item.lease_id == nil
      assert is_binary(item.id)
      assert is_binary(item.payload)
    end

    test "creates an item with custom options" do
      item = Item.new("tenant_1", "urgent", %{}, priority: 0, max_retries: 5)

      assert item.priority == 0
      assert item.max_retries == 5
    end

    test "visible? returns true when vesting_time has passed" do
      now = 10_000
      item = Item.new("tenant_1", "test", %{}, vesting_time: 9_000)

      assert Item.visible?(item, now)
    end

    test "visible? returns false when vesting_time is in future" do
      now = 10_000
      item = Item.new("tenant_1", "test", %{}, vesting_time: 20_000)

      refute Item.visible?(item, now)
    end

    test "leased? returns false when no lease" do
      item = Item.new("tenant_1", "test", %{})
      refute Item.leased?(item)
    end

    test "exhausted? returns true when error_count >= max_retries" do
      item = %{Item.new("tenant_1", "test", %{}) | error_count: 3}
      assert Item.exhausted?(item)
    end
  end

  describe "Lease" do
    test "creates a new lease" do
      item = Item.new("tenant_1", "test", %{})
      holder = :crypto.strong_rand_bytes(16)
      lease = Lease.new(item, holder)

      assert lease.item_id == item.id
      assert lease.queue_id == item.queue_id
      assert lease.holder == holder
      assert lease.expires_at > lease.obtained_at
    end

    test "expired? returns false for fresh lease" do
      now = 10_000
      item = Item.new("tenant_1", "test", %{})
      lease = Lease.new(item, "holder", now: now)

      refute Lease.expired?(lease, now: now)
    end

    test "expired? returns true for old lease" do
      item = Item.new("tenant_1", "test", %{})
      # Created at 4000, expires at 34000 (default 30s), but we'll set duration
      lease = Lease.new(item, "holder", duration_ms: 5000, now: 4_000)
      now = 10_000

      assert Lease.expired?(lease, now: now)
    end
  end

  describe "Job behaviour" do
    defmodule TestJob do
      @moduledoc false
      use Bedrock.JobQueue.Job,
        topic: "test:job",
        priority: 50,
        max_retries: 5

      @impl true
      def perform(%{action: action}, _meta) do
        case action do
          "succeed" -> :ok
          "fail" -> {:error, :failed}
          "discard" -> {:discard, :invalid}
        end
      end
    end

    test "job module has config" do
      config = TestJob.__job_config__()

      assert config.topic == "test:job"
      assert config.priority == 50
      assert config.max_retries == 5
    end

    test "job module has timeout" do
      assert TestJob.timeout() == 30_000
    end
  end
end
