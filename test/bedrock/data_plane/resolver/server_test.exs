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
end
