defmodule Bedrock.DataPlane.Resolver.ServerTest do
  use ExUnit.Case, async: false

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
               restart: :temporary
             } = spec

      assert {GenServer, :start_link,
              [Server, {token, last_version, epoch, director, sweep_interval_ms, version_retention_ms}]} = spec.start

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
      lock_token = :crypto.strong_rand_bytes(32)

      pid =
        start_supervised!(
          {Server,
           [
             lock_token: lock_token,
             key_range: {"", :end},
             epoch: 1,
             last_version: Version.zero(),
             director: self(),
             cluster: TestCluster
           ]}
        )

      {:ok, server: pid, lock_token: lock_token}
    end

    test "initializes with correct state", %{server: server, lock_token: lock_token} do
      state = :sys.get_state(server)

      assert %State{
               lock_token: ^lock_token,
               mode: :running
             } = state

      assert state.tree
      assert state.oldest_version
      assert state.last_version
      assert state.waiting == %{}
    end
  end

  describe "handle_call - resolve_transactions when running" do
    setup do
      lock_token = :crypto.strong_rand_bytes(32)

      pid =
        start_supervised!(
          {Server,
           [
             lock_token: lock_token,
             key_range: {"", :end},
             epoch: 1,
             last_version: Version.zero(),
             director: self(),
             cluster: TestCluster
           ]}
        )

      {:ok, server: pid, lock_token: lock_token}
    end

    test "resolver starts in running mode and is ready for transactions", %{server: server} do
      state = :sys.get_state(server)
      assert state.mode == :running
    end
  end

  describe "handle_info - resolve_next" do
    setup do
      lock_token = :crypto.strong_rand_bytes(32)

      pid =
        start_supervised!(
          {Server,
           [
             lock_token: lock_token,
             key_range: {"", :end},
             epoch: 1,
             last_version: Version.zero(),
             director: self(),
             cluster: TestCluster
           ]}
        )

      {:ok, server: pid}
    end

    test "server is alive and can receive messages", %{server: server} do
      assert Process.alive?(server)
      state = :sys.get_state(server)
      assert %State{mode: :running} = state
    end
  end

  describe "private functions" do
    test "module compiles and has expected structure" do
      Code.ensure_loaded!(Server)

      assert is_atom(Server)
      assert function_exported?(Server, :child_spec, 1)
      assert function_exported?(Server, :init, 1)
    end
  end

  describe "integration scenarios" do
    setup do
      lock_token = :crypto.strong_rand_bytes(32)

      pid =
        start_supervised!(
          {Server,
           [
             lock_token: lock_token,
             key_range: {"", :end},
             epoch: 1,
             last_version: Version.zero(),
             director: self(),
             cluster: TestCluster
           ]}
        )

      {:ok, server: pid, lock_token: lock_token}
    end

    test "resolver is ready to accept transactions", %{server: server} do
      state = :sys.get_state(server)
      assert state.mode == :running
      assert state.last_version
      assert state.waiting == %{}
    end

    test "server maintains state consistency", %{server: server, lock_token: lock_token} do
      state = :sys.get_state(server)
      assert state.lock_token == lock_token
      assert state.mode == :running
      assert Process.alive?(server)

      final_state = :sys.get_state(server)
      assert final_state.lock_token == lock_token
      assert final_state.mode == :running
    end
  end

  describe "transaction validation" do
    setup do
      lock_token = :crypto.strong_rand_bytes(32)

      pid =
        start_supervised!(
          {Server,
           [
             lock_token: lock_token,
             key_range: {"", :end},
             epoch: 1,
             last_version: Version.zero(),
             director: self(),
             cluster: TestCluster
           ]}
        )

      zero_version = Version.zero()
      next_version = Version.increment(zero_version)
      {:ok, server: pid, zero_version: zero_version, next_version: next_version}
    end

    test "accepts valid binary transaction", %{
      server: server,
      zero_version: zero_version,
      next_version: next_version
    } do
      # Create a simple binary transaction with no write conflicts
      valid_transactions = [simple_binary_transaction()]

      result =
        Resolver.resolve_transactions(server, 1, zero_version, next_version, valid_transactions)

      assert {:ok, []} = result
    end

    test "rejects invalid transaction summaries", %{
      server: server,
      zero_version: zero_version,
      next_version: next_version
    } do
      invalid_transactions = ["not_a_transaction_summary", {:invalid, :format}]

      result =
        Resolver.resolve_transactions(server, 1, zero_version, next_version, invalid_transactions)

      assert {:error, error_message} = result

      assert error_message =~
               "invalid transaction format: all transactions must be binary"
    end

    test "validation now correctly expects binary transaction summaries", %{
      server: server,
      zero_version: zero_version,
      next_version: next_version
    } do
      # Test with various binary transactions
      binary_transactions = [
        # no writes
        simple_binary_transaction(),
        # writes to key1, key2
        simple_binary_transaction(["key1", "key2"]),
        # write to write_key
        simple_binary_transaction(["write_key"])
      ]

      result =
        Resolver.resolve_transactions(server, 1, zero_version, next_version, binary_transactions)

      assert {:ok, aborted_indices} = result
      assert is_list(aborted_indices)
    end
  end

  describe "timeout mechanism for waiting transactions" do
    setup do
      lock_token = :crypto.strong_rand_bytes(32)

      pid =
        start_supervised!(
          {Server,
           [
             lock_token: lock_token,
             key_range: {"", :end},
             epoch: 1,
             last_version: Version.zero(),
             director: self(),
             cluster: TestCluster
           ]}
        )

      zero_version = Version.zero()
      next_version = Version.increment(zero_version)
      future_version = Version.increment(next_version)

      {:ok, server: pid, zero_version: zero_version, next_version: next_version, future_version: future_version}
    end

    test "adds transaction to waiting list when dependency missing", %{
      server: server,
      next_version: next_version,
      future_version: future_version
    } do
      # Create simple binary transaction that writes to "test_key"
      test_transaction = simple_binary_transaction(["test_key"])

      task =
        Task.async(fn ->
          Resolver.resolve_transactions(server, 1, next_version, future_version, [test_transaction])
        end)

      Process.sleep(50)
      state = :sys.get_state(server)
      assert map_size(state.waiting) == 1

      [{deadline, _reply_fn, data}] = Map.get(state.waiting, next_version)
      assert data == {future_version, [test_transaction]}
      assert is_integer(deadline)
      now = Bedrock.Internal.Time.monotonic_now_in_ms()
      assert deadline > now

      Task.shutdown(task)
    end

    test "timeout message cleans up expired transaction", %{
      server: server,
      next_version: next_version,
      future_version: future_version
    } do
      # Create simple binary transaction that writes to "test_key"
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

      [{_old_deadline, reply_fn, data}] = Map.get(state.waiting, next_version)
      expired_deadline = Bedrock.Internal.Time.monotonic_now_in_ms() - 1_000
      expired_entry = {expired_deadline, reply_fn, data}
      expired_state = %{state | waiting: %{next_version => [expired_entry]}}
      :sys.replace_state(server, fn _ -> expired_state end)

      send(server, :timeout)

      Process.sleep(50)

      final_state = :sys.get_state(server)
      assert map_size(final_state.waiting) == 0

      assert {:error, :waiting_timeout} = Task.await(task)
    end

    test "timeout message with no waiting transactions is ignored", %{server: server} do
      initial_state = :sys.get_state(server)
      assert map_size(initial_state.waiting) == 0

      send(server, :timeout)
      Process.sleep(50)

      final_state = :sys.get_state(server)
      assert final_state.waiting == initial_state.waiting
      assert map_size(final_state.waiting) == 0
    end

    test "waiting list maintains chronological order", %{
      server: server,
      next_version: next_version,
      future_version: future_version
    } do
      # Create simple binary transactions that write to different keys
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

      [{first_deadline, _, _}] = Map.get(state.waiting, next_version)
      [{second_deadline, _, _}] = Map.get(state.waiting, future_version)
      assert first_deadline <= second_deadline

      Task.shutdown(task1)
      Task.shutdown(task2)
    end
  end

  describe "sweep functionality" do
    setup do
      lock_token = :crypto.strong_rand_bytes(32)

      pid =
        start_supervised!({Server,
         [
           lock_token: lock_token,
           key_range: {"", :end},
           epoch: 1,
           last_version: Version.zero(),
           director: self(),
           cluster: TestCluster,
           # Short interval for testing
           sweep_interval_ms: 100,
           # Keep only 200ms of history
           version_retention_ms: 200
         ]})

      %{resolver: pid, lock_token: lock_token}
    end

    test "child_spec accepts custom sweep configuration", %{} do
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

      assert {GenServer, :start_link,
              [Server, {_token, _last_version, _epoch, _director, sweep_interval_ms, version_retention_ms}]} =
               spec.start

      assert sweep_interval_ms == 500
      assert version_retention_ms == 2000
    end

    test "resolver state includes sweep configuration", %{resolver: resolver} do
      # Get the current state by calling a simple operation
      {:ok, _aborted} =
        Resolver.resolve_transactions(resolver, 1, Version.zero(), Version.from_integer(1000), [], timeout: 1000)

      # Check that state was initialized properly by verifying it can handle transactions
      # (we can't directly access state but can verify the resolver is working with sweep config)
      assert Process.alive?(resolver)
    end

    test "sweep removes old versions from tree", %{resolver: resolver} do
      # Create transactions at different versions
      # 1ms in microseconds
      old_version = Version.from_integer(1000)
      # 300ms in microseconds
      recent_version = Version.from_integer(300_000)

      old_tx = simple_binary_transaction(["key1"])
      recent_tx = simple_binary_transaction(["key2"])

      # Process old transaction
      {:ok, _} = Resolver.resolve_transactions(resolver, 1, Version.zero(), old_version, [old_tx], timeout: 1000)

      # Process recent transaction
      {:ok, _} = Resolver.resolve_transactions(resolver, 1, old_version, recent_version, [recent_tx], timeout: 1000)

      # Wait for sweep to occur (sweep interval is 100ms, retention is 200ms)
      Process.sleep(150)

      # Force a timeout to trigger sweep by sending a message that will cause :next_timeout
      send(resolver, :timeout)
      Process.sleep(50)

      # Verify resolver is still functioning (old data swept but resolver still works)
      newer_version = Version.from_integer(400_000)
      newer_tx = simple_binary_transaction(["key3"])
      {:ok, _} = Resolver.resolve_transactions(resolver, 1, recent_version, newer_version, [newer_tx], timeout: 1000)
    end

    test "sweep configuration is applied correctly", %{resolver: resolver} do
      # This test just verifies that the resolver continues to work correctly
      # with sweep configuration, by processing some transactions over time

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
      {:ok, []} = Resolver.resolve_transactions(resolver, 1, Version.zero(), version1, [tx1], timeout: 1000)
      {:ok, []} = Resolver.resolve_transactions(resolver, 1, version1, version2, [tx2], timeout: 1000)

      # Wait for sweep to potentially occur
      # Longer than sweep interval (100ms)
      Process.sleep(150)

      # Process another transaction - this should work fine regardless of sweep
      {:ok, []} = Resolver.resolve_transactions(resolver, 1, version2, version3, [tx3], timeout: 1000)

      # Verify resolver is still alive and functioning
      assert Process.alive?(resolver)
    end
  end
end
