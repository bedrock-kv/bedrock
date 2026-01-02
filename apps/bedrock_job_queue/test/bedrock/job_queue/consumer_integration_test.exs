defmodule Bedrock.JobQueue.ConsumerIntegrationTest do
  @moduledoc """
  Integration tests for the consumer flow: Scanner → Manager → Worker.

  Uses a stateful mock store to simulate real storage behavior.
  This allows Store.obtain_lease to create a lease that Store.complete can find.
  """

  # Not async because we need global Mox mode for cross-process mocking
  use ExUnit.Case, async: false

  import Bedrock.JobQueue.Test.StoreHelpers
  import ExUnit.CaptureLog
  import Mox

  alias Bedrock.JobQueue.Consumer.Manager
  alias Bedrock.JobQueue.Item
  alias Bedrock.JobQueue.Store
  alias Bedrock.JobQueue.Test.Jobs
  alias Bedrock.Keyspace

  # Use global mode so spawned processes can access the mock
  setup :set_mox_global

  @concurrency 5

  setup do
    # Start a test registry for job handlers
    registry_name = :"TestRegistry_#{System.unique_integer()}"
    {:ok, _} = Registry.start_link(keys: :duplicate, name: registry_name)

    # Start worker pool (Task.Supervisor)
    pool_name = :"TestPool_#{System.unique_integer()}"
    {:ok, pool} = Task.Supervisor.start_link(name: pool_name, max_children: @concurrency)

    # Start mock store for stateful storage simulation
    {:ok, store_agent} = start_mock_store()

    # Stub transact to execute callbacks immediately
    stub(MockRepo, :transact, fn callback -> callback.() end)

    # Set up the stateful mock store
    setup_integration_stubs(MockRepo, store_agent)

    %{
      registry: registry_name,
      pool: pool,
      pool_name: pool_name,
      concurrency: @concurrency,
      store: store_agent,
      root: Keyspace.new("job_queue/")
    }
  end

  defp start_manager(ctx, opts \\ []) do
    name = :"TestManager_#{System.unique_integer()}"

    {:ok, manager} =
      Manager.start_link(
        Keyword.merge(
          [
            name: name,
            repo: MockRepo,
            root: ctx.root,
            registry: ctx.registry,
            worker_pool: ctx.pool_name,
            concurrency: ctx.concurrency
          ],
          opts
        )
      )

    manager
  end

  defp register_job(ctx, pattern, job_module) do
    Registry.register(ctx.registry, pattern, job_module)
  end

  defp enqueue_item(ctx, topic, payload \\ %{}) do
    item = Item.new("tenant_1", topic, payload)
    keyspaces = Store.queue_keyspaces(ctx.root, "tenant_1")
    store_item(ctx.store, keyspaces.items, item)
    item
  end

  describe "happy path - job succeeds" do
    test "completes job when handler returns :ok", ctx do
      register_job(ctx, "test:*", Jobs.SuccessJob)
      item = enqueue_item(ctx, "test:success")
      manager = start_manager(ctx)

      send(manager, {:queue_ready, "tenant_1"})

      # Wait for item to be removed (completed)
      keyspaces = Store.queue_keyspaces(ctx.root, "tenant_1")
      item_key = {item.priority, item.vesting_time, item.id}
      storage_key = {Keyspace.prefix(keyspaces.items), item_key}

      assert_eventually(fn ->
        Agent.get(ctx.store, &Map.get(&1, storage_key)) == nil
      end)
    end

    test "completes job when handler returns {:ok, result}", ctx do
      register_job(ctx, "test:*", Jobs.SuccessWithResultJob)
      item = enqueue_item(ctx, "test:success_with_result", %{key: "value"})
      manager = start_manager(ctx)

      send(manager, {:queue_ready, "tenant_1"})

      # Wait for item to be removed
      keyspaces = Store.queue_keyspaces(ctx.root, "tenant_1")
      item_key = {item.priority, item.vesting_time, item.id}
      storage_key = {Keyspace.prefix(keyspaces.items), item_key}

      assert_eventually(fn ->
        Agent.get(ctx.store, &Map.get(&1, storage_key)) == nil
      end)
    end
  end

  describe "error handling - job fails" do
    test "requeues job when handler returns {:error, reason}", ctx do
      register_job(ctx, "test:*", Jobs.FailingJob)
      item = enqueue_item(ctx, "test:fail")
      manager = start_manager(ctx)

      send(manager, {:queue_ready, "tenant_1"})

      # Wait for requeue - item should have error_count = 1
      keyspaces = Store.queue_keyspaces(ctx.root, "tenant_1")
      items_prefix = Keyspace.prefix(keyspaces.items)

      assert_eventually(fn ->
        stored_items =
          Agent.get(ctx.store, fn state ->
            state
            |> Enum.filter(fn {{p, _k}, _v} -> p == items_prefix end)
            |> Enum.map(fn {_k, v} -> :erlang.binary_to_term(v) end)
          end)

        case stored_items do
          [requeued] -> requeued.id == item.id and requeued.error_count == 1
          _ -> false
        end
      end)
    end
  end

  describe "discard handling" do
    test "completes job when handler returns {:discard, reason}", ctx do
      register_job(ctx, "test:*", Jobs.DiscardJob)
      item = enqueue_item(ctx, "test:discard")
      manager = start_manager(ctx)

      log =
        capture_log(fn ->
          send(manager, {:queue_ready, "tenant_1"})

          # Wait for item to be removed (discarded = completed)
          keyspaces = Store.queue_keyspaces(ctx.root, "tenant_1")
          item_key = {item.priority, item.vesting_time, item.id}
          storage_key = {Keyspace.prefix(keyspaces.items), item_key}

          assert_eventually(fn ->
            Agent.get(ctx.store, &Map.get(&1, storage_key)) == nil
          end)
        end)

      assert log =~ "Discarding job"
      assert log =~ ":invalid_data"
    end
  end

  describe "snooze handling" do
    test "requeues job with delay when handler returns {:snooze, delay}", ctx do
      register_job(ctx, "test:*", Jobs.SnoozeJob)
      item = enqueue_item(ctx, "test:snooze", %{delay: 5000})
      manager = start_manager(ctx)

      send(manager, {:queue_ready, "tenant_1"})

      # Wait for requeue - snooze increments error_count
      keyspaces = Store.queue_keyspaces(ctx.root, "tenant_1")
      items_prefix = Keyspace.prefix(keyspaces.items)

      assert_eventually(fn ->
        stored_items =
          Agent.get(ctx.store, fn state ->
            state
            |> Enum.filter(fn {{p, _k}, _v} -> p == items_prefix end)
            |> Enum.map(fn {_k, v} -> :erlang.binary_to_term(v) end)
          end)

        case stored_items do
          [requeued] -> requeued.id == item.id and requeued.error_count == 1
          _ -> false
        end
      end)
    end
  end

  describe "missing handler" do
    test "discards job when no handler registered for topic", ctx do
      # Don't register any handler
      item = enqueue_item(ctx, "unknown:topic")
      manager = start_manager(ctx)

      log =
        capture_log(fn ->
          send(manager, {:queue_ready, "tenant_1"})

          # Wait for item to be removed (discarded)
          keyspaces = Store.queue_keyspaces(ctx.root, "tenant_1")
          item_key = {item.priority, item.vesting_time, item.id}
          storage_key = {Keyspace.prefix(keyspaces.items), item_key}

          assert_eventually(fn ->
            Agent.get(ctx.store, &Map.get(&1, storage_key)) == nil
          end)
        end)

      assert log =~ "Discarding job"
      assert log =~ ":no_handler"
    end
  end

  describe "empty queue" do
    test "does nothing when queue has no visible items", ctx do
      manager = start_manager(ctx)

      # No items enqueued - just verify manager handles message without crashing
      send(manager, {:queue_ready, "tenant_1"})

      # Give it a moment to process, then check it's still alive
      assert_eventually(fn -> Process.alive?(manager) end, timeout: 50)
    end
  end
end
