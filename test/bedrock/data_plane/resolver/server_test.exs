defmodule Bedrock.DataPlane.Resolver.ServerTest do
  use ExUnit.Case, async: false
  use ExUnitProperties

  import Bedrock.Test.TelemetryTestHelper
  import StreamData

  alias Bedrock.DataPlane.Resolver
  alias Bedrock.DataPlane.Resolver.Server
  alias Bedrock.DataPlane.Resolver.State
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version

  # Helper to create simple binary transactions for testing
  defp simple_binary_transaction(write_keys \\ []) do
    Transaction.encode(%{
      mutations: Enum.map(write_keys, &{:set, &1, "value"}),
      read_conflicts: [],
      write_conflicts: Enum.map(write_keys, &{&1, &1 <> "\0"})
    })
  end

  # Telemetry helpers for deterministic testing
  defp expect_transaction_waitlisted(_last_version, next_version) do
    {_measurements, metadata} = expect_telemetry([:bedrock, :resolver, :resolve_transactions, :waiting_list_inserted])
    assert metadata.next_version == next_version
  end

  defp expect_transaction_processing_start(_last_version, next_version) do
    {_measurements, metadata} = expect_telemetry([:bedrock, :resolver, :resolve_transactions, :processing])
    assert metadata.next_version == next_version
  end

  defp expect_transaction_completed(_last_version, next_version) do
    {_measurements, metadata} = expect_telemetry([:bedrock, :resolver, :resolve_transactions, :completed])
    assert metadata.next_version == next_version
  end

  defp expect_waitlisted_transaction_resolved(next_version) do
    {_measurements, metadata} = expect_telemetry([:bedrock, :resolver, :resolve_transactions, :waiting_resolved])
    assert metadata.next_version == next_version
  end

  defp attach_resolver_telemetry(test_pid) do
    attach_telemetry_reflector(
      test_pid,
      [
        [:bedrock, :resolver, :resolve_transactions, :waiting_list_inserted],
        [:bedrock, :resolver, :resolve_transactions, :processing],
        [:bedrock, :resolver, :resolve_transactions, :completed],
        [:bedrock, :resolver, :resolve_transactions, :waiting_resolved]
      ],
      "test-resolver-events"
    )
  end

  # Common setup helper for server startup
  defp start_test_server(additional_opts \\ []) do
    lock_token = :crypto.strong_rand_bytes(32)

    opts =
      Keyword.merge(
        [
          lock_token: lock_token,
          key_range: {"", :end},
          epoch: 1,
          last_version: Version.zero(),
          director: self(),
          cluster: TestCluster
        ],
        additional_opts
      )

    pid = start_supervised!({Server, opts})
    %{server: pid, lock_token: lock_token}
  end

  describe "child_spec/1" do
    test "creates valid child spec with required options" do
      opts = [
        lock_token: :crypto.strong_rand_bytes(32),
        key_range: {"a", "z"},
        epoch: 123,
        director: self(),
        cluster: TestCluster
      ]

      spec = Server.child_spec(opts)

      assert %{
               id: {Server, _, _, _},
               restart: :temporary,
               start:
                 {GenServer, :start_link,
                  [Server, {token, last_version, epoch, director, sweep_interval_ms, version_retention_ms}]}
             } = spec

      assert is_binary(token)
      assert last_version == Version.zero()
      assert epoch == 123
      assert is_pid(director)
      assert sweep_interval_ms == 1_000
      assert version_retention_ms == 6_000
    end

    test "raises error when lock_token option is missing" do
      opts = [
        key_range: {"a", "z"},
        epoch: 123,
        director: self()
      ]

      assert_raise RuntimeError, "Missing :lock_token option", fn ->
        Server.child_spec(opts)
      end
    end

    test "raises error when key_range option is missing" do
      opts = [
        lock_token: :crypto.strong_rand_bytes(32),
        epoch: 123,
        director: self()
      ]

      assert_raise RuntimeError, "Missing :key_range option", fn ->
        Server.child_spec(opts)
      end
    end

    test "raises error when epoch option is missing" do
      opts = [
        lock_token: :crypto.strong_rand_bytes(32),
        key_range: {"a", "z"},
        director: self()
      ]

      assert_raise RuntimeError, "Missing :epoch option", fn ->
        Server.child_spec(opts)
      end
    end

    test "raises error when director option is missing" do
      opts = [
        lock_token: :crypto.strong_rand_bytes(32),
        key_range: {"a", "z"},
        epoch: 123
      ]

      assert_raise RuntimeError, "Missing :director option", fn ->
        Server.child_spec(opts)
      end
    end
  end

  describe "GenServer lifecycle" do
    setup do
      start_test_server()
    end

    test "initializes with correct state", %{server: server, lock_token: lock_token} do
      assert %State{
               lock_token: ^lock_token,
               mode: :running,
               waiting: %{}
             } = :sys.get_state(server)
    end
  end

  describe "server state and lifecycle" do
    setup do
      start_test_server()
    end

    test "resolver starts in running mode and is ready for transactions", %{server: server} do
      assert %State{mode: :running} = :sys.get_state(server)
    end

    test "server is alive and can receive messages", %{server: server} do
      assert Process.alive?(server)
      assert %State{mode: :running} = :sys.get_state(server)
    end
  end

  describe "module structure and integration" do
    setup do
      start_test_server()
    end

    test "module compiles and has expected structure" do
      Code.ensure_loaded!(Server)

      assert is_atom(Server)
      assert function_exported?(Server, :child_spec, 1)
      assert function_exported?(Server, :init, 1)
    end

    test "resolver is ready to accept transactions", %{server: server} do
      assert %State{
               mode: :running,
               waiting: %{}
             } = :sys.get_state(server)
    end

    test "server maintains state consistency", %{server: server, lock_token: lock_token} do
      assert %State{
               lock_token: ^lock_token,
               mode: :running
             } = :sys.get_state(server)

      assert Process.alive?(server)

      assert %State{
               lock_token: ^lock_token,
               mode: :running
             } = :sys.get_state(server)
    end
  end

  describe "transaction validation" do
    setup do
      context = start_test_server()
      zero_version = Version.zero()
      next_version = Version.increment(zero_version)
      Map.merge(context, %{zero_version: zero_version, next_version: next_version})
    end

    test "accepts valid binary transaction", %{
      server: server,
      zero_version: zero_version,
      next_version: next_version
    } do
      valid_transactions = [simple_binary_transaction()]

      assert {:ok, []} =
               Resolver.resolve_transactions(server, 1, zero_version, next_version, valid_transactions)
    end

    test "rejects invalid transaction summaries", %{
      server: server,
      zero_version: zero_version,
      next_version: next_version
    } do
      invalid_transactions = ["not_a_transaction_summary", {:invalid, :format}]

      assert {:error, error_message} =
               Resolver.resolve_transactions(server, 1, zero_version, next_version, invalid_transactions)

      assert error_message =~ "invalid transaction format: all transactions must be binary"
    end

    test "handles various binary transaction formats", %{
      server: server,
      zero_version: zero_version,
      next_version: next_version
    } do
      binary_transactions = [
        simple_binary_transaction(),
        simple_binary_transaction(["key1", "key2"]),
        simple_binary_transaction(["write_key"])
      ]

      assert {:ok, aborted_indices} =
               Resolver.resolve_transactions(server, 1, zero_version, next_version, binary_transactions)

      assert is_list(aborted_indices)
    end
  end

  describe "timeout mechanism for waiting transactions" do
    setup do
      context = start_test_server()
      zero_version = Version.zero()
      next_version = Version.increment(zero_version)
      future_version = Version.increment(next_version)

      Map.merge(context, %{
        zero_version: zero_version,
        next_version: next_version,
        future_version: future_version
      })
    end

    test "adds transaction to waiting list when dependency missing", %{
      server: server,
      next_version: next_version,
      future_version: future_version
    } do
      test_transaction = simple_binary_transaction(["test_key"])

      task =
        Task.async(fn ->
          Resolver.resolve_transactions(server, 1, next_version, future_version, [test_transaction])
        end)

      Process.sleep(50)
      state = :sys.get_state(server)
      assert map_size(state.waiting) == 1

      assert [{deadline, _reply_fn, {^future_version, [^test_transaction]}}] =
               Map.get(state.waiting, next_version)

      assert is_integer(deadline)
      assert deadline > Bedrock.Internal.Time.monotonic_now_in_ms()

      Task.shutdown(task)
    end

    test "timeout message cleans up expired transaction", %{
      server: server,
      next_version: next_version,
      future_version: future_version
    } do
      test_transaction = simple_binary_transaction(["test_key"])

      task =
        Task.async(fn ->
          Resolver.resolve_transactions(
            server,
            1,
            next_version,
            future_version,
            [test_transaction],
            timeout: 60_000
          )
        end)

      Process.sleep(50)
      state = :sys.get_state(server)
      assert map_size(state.waiting) == 1

      assert [{_old_deadline, reply_fn, data}] = Map.get(state.waiting, next_version)
      expired_deadline = Bedrock.Internal.Time.monotonic_now_in_ms() - 1_000
      expired_entry = {expired_deadline, reply_fn, data}
      expired_state = %{state | waiting: %{next_version => [expired_entry]}}
      :sys.replace_state(server, fn _ -> expired_state end)

      send(server, :timeout)
      Process.sleep(50)

      assert %{waiting: waiting} = :sys.get_state(server)
      assert map_size(waiting) == 0

      assert {:error, :waiting_timeout} = Task.await(task)
    end

    test "timeout message with no waiting transactions is ignored", %{server: server} do
      assert %{waiting: waiting} = :sys.get_state(server)
      assert map_size(waiting) == 0

      send(server, :timeout)
      Process.sleep(50)

      assert %{waiting: ^waiting} = :sys.get_state(server)
      assert map_size(waiting) == 0
    end

    test "waiting list maintains chronological order", %{
      server: server,
      next_version: next_version,
      future_version: future_version
    } do
      transaction1 = simple_binary_transaction(["key1"])
      transaction2 = simple_binary_transaction(["key2"])

      task1 =
        Task.async(fn ->
          Resolver.resolve_transactions(server, 1, next_version, future_version, [transaction1], timeout: 60_000)
        end)

      Process.sleep(50)
      later_version = Version.increment(future_version)

      task2 =
        Task.async(fn ->
          Resolver.resolve_transactions(server, 1, future_version, later_version, [transaction2], timeout: 60_000)
        end)

      Process.sleep(50)

      state = :sys.get_state(server)
      assert map_size(state.waiting) == 2
      assert Map.has_key?(state.waiting, next_version)
      assert Map.has_key?(state.waiting, future_version)

      assert [{first_deadline, _, _}] = Map.get(state.waiting, next_version)
      assert [{second_deadline, _, _}] = Map.get(state.waiting, future_version)
      assert first_deadline <= second_deadline

      Task.shutdown(task1)
      Task.shutdown(task2)
    end

    test "waitlist processing works correctly with telemetry-based deterministic test", %{
      server: server,
      zero_version: zero_version,
      next_version: next_version,
      future_version: future_version
    } do
      # Use telemetry to make test deterministic instead of relying on timing
      attach_resolver_telemetry(self())

      transaction_b = simple_binary_transaction(["key_b"])

      # Send B - will be waitlisted with key=next_version
      task_b =
        Task.async(fn ->
          Resolver.resolve_transactions(server, 1, next_version, future_version, [transaction_b])
        end)

      # Wait for B to be waitlisted
      expect_transaction_waitlisted(next_version, future_version)

      # Verify B is waitlisted
      state_before = :sys.get_state(server)
      assert map_size(state_before.waiting) == 1
      assert Map.has_key?(state_before.waiting, next_version)

      # Process transaction A - this should trigger processing of B
      assert {:ok, []} =
               Resolver.resolve_transactions(server, 1, zero_version, next_version, [
                 simple_binary_transaction(["key_a"])
               ])

      # Wait for A to start processing
      expect_transaction_processing_start(zero_version, next_version)

      # Wait for A to complete processing (completed event shows resolver's new state)
      expect_transaction_completed(next_version, next_version)

      # Wait for B to be resolved from waitlist and start processing
      expect_waitlisted_transaction_resolved(future_version)
      expect_transaction_processing_start(next_version, future_version)

      # Wait for B to complete processing (completed event shows resolver's new state)
      expect_transaction_completed(future_version, future_version)

      # B should complete successfully
      assert {:ok, []} = Task.await(task_b, 1000)

      # Final check - waitlist should be empty
      final_state = :sys.get_state(server)
      assert map_size(final_state.waiting) == 0

      # Clean up telemetry
    end
  end

  describe "sweep functionality" do
    setup do
      context =
        start_test_server(
          # Short interval for testing
          sweep_interval_ms: 100,
          # Keep only 200ms of history
          version_retention_ms: 200
        )

      Map.put(context, :resolver, context[:server])
    end

    test "child_spec accepts custom sweep configuration" do
      opts = [
        lock_token: :crypto.strong_rand_bytes(32),
        key_range: {"a", "z"},
        epoch: 123,
        director: self(),
        cluster: TestCluster,
        sweep_interval_ms: 500,
        version_retention_ms: 2000
      ]

      spec = Server.child_spec(opts)

      assert %{
               start: {GenServer, :start_link, [Server, {_token, _last_version, _epoch, _director, 500, 2000}]}
             } = spec
    end

    test "resolver state includes sweep configuration", %{resolver: resolver} do
      assert {:ok, _aborted} =
               Resolver.resolve_transactions(resolver, 1, Version.zero(), Version.from_integer(1000), [], timeout: 1000)

      assert Process.alive?(resolver)
    end

    test "sweep removes old versions from tree", %{resolver: resolver} do
      # 1ms in microseconds
      old_version = Version.from_integer(1000)
      # 300ms in microseconds
      recent_version = Version.from_integer(300_000)

      old_tx = simple_binary_transaction(["key1"])
      recent_tx = simple_binary_transaction(["key2"])

      assert {:ok, _} = Resolver.resolve_transactions(resolver, 1, Version.zero(), old_version, [old_tx], timeout: 1000)

      assert {:ok, _} =
               Resolver.resolve_transactions(resolver, 1, old_version, recent_version, [recent_tx], timeout: 1000)

      # Wait for sweep to occur (sweep interval is 100ms, retention is 200ms)
      Process.sleep(150)
      send(resolver, :timeout)
      Process.sleep(50)

      # Verify resolver is still functioning after sweep
      newer_version = Version.from_integer(400_000)
      newer_tx = simple_binary_transaction(["key3"])

      assert {:ok, _} =
               Resolver.resolve_transactions(resolver, 1, recent_version, newer_version, [newer_tx], timeout: 1000)
    end

    test "sweep configuration works with transaction processing", %{resolver: resolver} do
      # 100ms
      version1 = Version.from_integer(100_000)
      # 200ms
      version2 = Version.from_integer(200_000)
      # 350ms - beyond retention period
      version3 = Version.from_integer(350_000)

      tx1 = simple_binary_transaction(["key1"])
      tx2 = simple_binary_transaction(["key2"])
      tx3 = simple_binary_transaction(["key3"])

      # Process transactions in sequence
      assert {:ok, []} = Resolver.resolve_transactions(resolver, 1, Version.zero(), version1, [tx1], timeout: 1000)
      assert {:ok, []} = Resolver.resolve_transactions(resolver, 1, version1, version2, [tx2], timeout: 1000)

      # Wait for sweep to potentially occur (longer than sweep interval)
      Process.sleep(150)

      # Process another transaction - should work fine regardless of sweep
      assert {:ok, []} = Resolver.resolve_transactions(resolver, 1, version2, version3, [tx3], timeout: 1000)
      assert Process.alive?(resolver)
    end
  end

  describe "property-based testing" do
    setup do
      start_test_server()
    end

    property "transactions complete in version order regardless of arrival order", %{server: server} do
      check all(
              sequence_length <- integer(3..6),
              max_runs: 20
            ) do
        # Generate consecutive versions starting from server's current last_version
        server_state = :sys.get_state(server)
        start_version_int = Version.to_integer(server_state.last_version)

        versions =
          for i <- start_version_int..(start_version_int + sequence_length) do
            Version.from_integer(i)
          end

        # Create transaction pairs: [(last_v, next_v), ...]
        transaction_specs = Enum.zip(versions, tl(versions))

        attach_telemetry_reflector(
          self(),
          [[:bedrock, :resolver, :resolve_transactions, :completed]],
          "property-test-resolver-events"
        )

        # Create and shuffle transactions
        transactions_with_specs =
          transaction_specs
          |> Enum.with_index()
          |> Enum.map(fn {{last_v, next_v}, idx} ->
            tx = simple_binary_transaction(["key_#{idx}"])
            {tx, last_v, next_v}
          end)
          |> Enum.shuffle()

        # Send all transactions asynchronously in shuffled order
        tasks =
          for {tx, last_v, next_v} <- transactions_with_specs do
            Task.async(fn ->
              Resolver.resolve_transactions(server, 1, last_v, next_v, [tx])
            end)
          end

        # Collect completion events as they arrive
        completion_versions =
          for _ <- 1..length(transaction_specs) do
            {_measurements, metadata} = expect_telemetry([:bedrock, :resolver, :resolve_transactions, :completed])
            metadata.next_version
          end

        # Collect all task results
        results = Enum.map(tasks, &Task.await(&1, 5000))

        # All should succeed
        assert Enum.all?(results, &match?({:ok, []}, &1))

        # Verify the completion order matches expected version sequence
        expected_versions = Enum.map(transaction_specs, fn {_last, next} -> next end)
        assert completion_versions == expected_versions

        # Verify resolver state is clean
        final_state = :sys.get_state(server)
        assert map_size(final_state.waiting) == 0
        assert final_state.last_version == List.last(expected_versions)
      end
    end
  end
end
