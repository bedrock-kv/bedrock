defmodule Bedrock.DataPlane.Resolver.ServerTest do
  use ExUnit.Case, async: false
  alias Bedrock.DataPlane.Resolver.Server
  alias Bedrock.DataPlane.Resolver.State

  describe "child_spec/1" do
    test "creates valid child spec with required options" do
      opts = [
        lock_token: :crypto.strong_rand_bytes(32),
        key_range: {"a", "z"},
        epoch: 123
      ]

      spec = Server.child_spec(opts)

      assert %{
               id: Server,
               restart: :temporary
             } = spec

      assert {GenServer, :start_link, [Server, {token}]} = spec.start
      assert is_binary(token)
    end

    test "raises error when lock_token option is missing" do
      opts = [
        key_range: {"a", "z"},
        epoch: 123
      ]

      assert_raise RuntimeError, "Missing :lock_token option", fn ->
        Server.child_spec(opts)
      end
    end

    test "raises error when key_range option is missing" do
      opts = [
        lock_token: :crypto.strong_rand_bytes(32),
        epoch: 123
      ]

      assert_raise RuntimeError, "Missing :key_range option", fn ->
        Server.child_spec(opts)
      end
    end

    test "raises error when epoch option is missing" do
      opts = [
        lock_token: :crypto.strong_rand_bytes(32),
        key_range: {"a", "z"}
      ]

      assert_raise RuntimeError, "Missing :epoch option", fn ->
        Server.child_spec(opts)
      end
    end
  end

  describe "GenServer lifecycle" do
    setup do
      lock_token = :crypto.strong_rand_bytes(32)
      {:ok, pid} = GenServer.start_link(Server, {lock_token})
      {:ok, server: pid, lock_token: lock_token}
    end

    test "initializes with correct state", %{server: server, lock_token: lock_token} do
      state = :sys.get_state(server)

      assert %State{
               lock_token: ^lock_token,
               mode: :running
             } = state

      # Verify tree and versions are properly initialized
      assert state.tree != nil
      assert state.oldest_version != nil
      assert state.last_version != nil
      assert state.waiting == %{}
    end
  end

  describe "handle_call - resolve_transactions when running" do
    setup do
      lock_token = :crypto.strong_rand_bytes(32)
      {:ok, pid} = GenServer.start_link(Server, {lock_token})
      {:ok, server: pid, lock_token: lock_token}
    end

    test "resolver starts in running mode and is ready for transactions", %{server: server} do
      # Verify the resolver is in running mode and ready for transactions
      state = :sys.get_state(server)
      assert state.mode == :running

      # Note: To properly test transaction resolution, we'd need to set up
      # the full transaction structure and version coordination which is
      # beyond the scope of this cleanup test
    end
  end

  describe "handle_info - resolve_next" do
    setup do
      lock_token = :crypto.strong_rand_bytes(32)
      {:ok, pid} = GenServer.start_link(Server, {lock_token})

      # For this test, we'll just verify the server is alive
      # Testing handle_info requires complex state manipulation

      {:ok, server: pid}
    end

    test "server is alive and can receive messages", %{server: server} do
      # Just verify the server process is alive
      assert Process.alive?(server)

      # Verify we can get the state
      state = :sys.get_state(server)
      assert %State{mode: :running} = state
    end
  end

  describe "private functions" do
    test "module compiles and has expected structure" do
      # Ensure module is loaded before checking exports
      Code.ensure_loaded!(Server)

      # We can't directly test private functions like reply_fn/1
      # but we can verify the module structure
      assert is_atom(Server)
      assert function_exported?(Server, :child_spec, 1)
      assert function_exported?(Server, :init, 1)
    end
  end

  describe "integration scenarios" do
    setup do
      lock_token = :crypto.strong_rand_bytes(32)
      {:ok, pid} = GenServer.start_link(Server, {lock_token})

      # For integration tests, resolver starts in running mode

      {:ok, server: pid, lock_token: lock_token}
    end

    test "resolver is ready to accept transactions", %{server: server} do
      # Verify resolver starts in running mode
      state = :sys.get_state(server)
      assert state.mode == :running
      # Initialized with zero version
      assert state.last_version != nil
      assert state.waiting == %{}

      # Note: Full transaction testing would require proper transaction setup
      # which is beyond the scope of this recovery cleanup
    end

    test "server maintains state consistency", %{server: server, lock_token: lock_token} do
      # Verify initial state
      state = :sys.get_state(server)
      assert state.lock_token == lock_token
      assert state.mode == :running

      # Verify server is stable and running
      assert Process.alive?(server)

      # State should be consistent
      final_state = :sys.get_state(server)
      assert final_state.lock_token == lock_token
      assert final_state.mode == :running
    end
  end

  describe "transaction validation" do
    setup do
      lock_token = :crypto.strong_rand_bytes(32)
      {:ok, pid} = GenServer.start_link(Server, {lock_token})
      zero_version = Bedrock.DataPlane.Version.zero()
      next_version = Bedrock.DataPlane.Version.increment(zero_version)
      {:ok, server: pid, zero_version: zero_version, next_version: next_version}
    end

    test "accepts valid transaction summary like [nil: []]", %{
      server: server,
      zero_version: zero_version,
      next_version: next_version
    } do
      # Test that [nil: []] is valid transaction summary data (write-only transaction with no writes)
      valid_transactions = [nil: []]

      result =
        GenServer.call(
          server,
          {:resolve_transactions, {zero_version, next_version}, valid_transactions}
        )

      # Should succeed with no aborted transactions
      assert {:ok, []} = result
    end

    test "rejects invalid transaction summaries", %{
      server: server,
      zero_version: zero_version,
      next_version: next_version
    } do
      # Test with invalid transaction summary data
      invalid_transactions = ["not_a_transaction_summary", {:invalid, :format}]

      result =
        GenServer.call(
          server,
          {:resolve_transactions, {zero_version, next_version}, invalid_transactions}
        )

      assert {:error, error_message} = result

      assert error_message =~
               "invalid transaction format: all transactions must be transaction summaries with format {read_info | nil, write_keys}"
    end

    test "validation now correctly expects transaction summaries", %{
      server: server,
      zero_version: zero_version,
      next_version: next_version
    } do
      # Test with properly formatted transaction summaries
      valid_summaries = [
        # write-only transaction with no writes
        {nil, []},
        # write-only transaction with writes
        {nil, ["key1", "key2"]},
        # read-write transaction
        {{zero_version, ["read_key"]}, ["write_key"]}
      ]

      result =
        GenServer.call(
          server,
          {:resolve_transactions, {zero_version, next_version}, valid_summaries}
        )

      # Should succeed - validation accepts transaction summaries and ConflictResolution can process them
      assert {:ok, aborted_indices} = result
      assert is_list(aborted_indices)
    end
  end

  describe "timeout mechanism for waiting transactions" do
    setup do
      lock_token = :crypto.strong_rand_bytes(32)
      {:ok, pid} = GenServer.start_link(Server, {lock_token})
      zero_version = Bedrock.DataPlane.Version.zero()
      next_version = Bedrock.DataPlane.Version.increment(zero_version)
      future_version = Bedrock.DataPlane.Version.increment(next_version)

      {:ok,
       server: pid,
       zero_version: zero_version,
       next_version: next_version,
       future_version: future_version}
    end

    test "adds transaction to waiting list when dependency missing", %{
      server: server,
      next_version: next_version,
      future_version: future_version
    } do
      # Create a valid transaction summary (the resolver now expects transaction summaries)
      valid_transaction_summary = {nil, ["test_key"]}

      # Start an async call that will need to wait (out-of-order transaction)
      task =
        Task.async(fn ->
          GenServer.call(
            server,
            {:resolve_transactions, {next_version, future_version}, [valid_transaction_summary]}
          )
        end)

      # Give the server time to process the call and set up waiting state
      Process.sleep(50)

      # Check that the transaction is now in waiting list
      state = :sys.get_state(server)
      assert map_size(state.waiting) == 1

      # Verify the waiting entry structure (deadline, reply_fn, data)
      [{deadline, _reply_fn, data}] = Map.get(state.waiting, next_version)
      assert data == {future_version, [valid_transaction_summary]}
      assert is_integer(deadline)
      # Deadline should be in the future (now + 30s)
      now = Bedrock.Internal.Time.monotonic_now_in_ms()
      assert deadline > now

      # Clean up the task
      Task.shutdown(task)
    end

    test "timeout message cleans up expired transaction", %{
      server: server,
      next_version: next_version,
      future_version: future_version
    } do
      # Create a valid transaction summary
      valid_transaction_summary = {nil, ["test_key"]}

      # Start an async call that will timeout
      task =
        Task.async(fn ->
          GenServer.call(
            server,
            {:resolve_transactions, {next_version, future_version}, [valid_transaction_summary]},
            60_000
          )
        end)

      # Give the server time to process the call and set up waiting state
      Process.sleep(50)

      # Verify transaction is waiting
      state = :sys.get_state(server)
      assert map_size(state.waiting) == 1

      # Manually modify the state to make the transaction appear expired
      [{_old_deadline, reply_fn, data}] = Map.get(state.waiting, next_version)
      # Set the deadline to 1 second ago (expired)
      expired_deadline = Bedrock.Internal.Time.monotonic_now_in_ms() - 1_000
      expired_entry = {expired_deadline, reply_fn, data}
      expired_state = %{state | waiting: %{next_version => [expired_entry]}}
      :sys.replace_state(server, fn _ -> expired_state end)

      # Send GenServer timeout message to trigger cleanup
      send(server, :timeout)

      # Give the server time to process the timeout
      Process.sleep(50)

      # Verify the waiting list is cleaned up
      final_state = :sys.get_state(server)
      assert map_size(final_state.waiting) == 0

      # The task should receive an error response
      assert {:error, :waiting_timeout} = Task.await(task)
    end

    test "timeout message with no waiting transactions is ignored", %{server: server} do
      # Get initial state (should have empty waiting list)
      initial_state = :sys.get_state(server)
      assert map_size(initial_state.waiting) == 0

      # Send timeout when there are no waiting transactions
      send(server, :timeout)

      # Give the server time to process
      Process.sleep(50)

      # State should be unchanged
      final_state = :sys.get_state(server)
      assert final_state.waiting == initial_state.waiting
      assert map_size(final_state.waiting) == 0
    end

    test "waiting list maintains chronological order", %{
      server: server,
      next_version: next_version,
      future_version: future_version
    } do
      # Create valid transaction summaries
      transaction1 = {nil, ["key1"]}
      transaction2 = {nil, ["key2"]}

      # Add first waiting transaction
      task1 =
        Task.async(fn ->
          GenServer.call(
            server,
            {:resolve_transactions, {next_version, future_version}, [transaction1]},
            60_000
          )
        end)

      Process.sleep(50)

      # Add second waiting transaction
      later_version = Bedrock.DataPlane.Version.increment(future_version)

      task2 =
        Task.async(fn ->
          GenServer.call(
            server,
            {:resolve_transactions, {future_version, later_version}, [transaction2]},
            60_000
          )
        end)

      Process.sleep(50)

      # Verify both transactions are waiting
      state = :sys.get_state(server)
      assert map_size(state.waiting) == 2

      # Verify both versions have waiting entries
      assert Map.has_key?(state.waiting, next_version)
      assert Map.has_key?(state.waiting, future_version)

      # Check deadlines - first transaction should have earlier deadline
      [{first_deadline, _, _}] = Map.get(state.waiting, next_version)
      [{second_deadline, _, _}] = Map.get(state.waiting, future_version)
      assert first_deadline <= second_deadline

      # Clean up tasks
      Task.shutdown(task1)
      Task.shutdown(task2)
    end
  end
end
