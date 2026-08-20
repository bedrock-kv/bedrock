defmodule Bedrock.Internal.RepoTransactTest do
  @moduledoc """
  Behavioral tests for the Repo transaction lifecycle: the cast-based
  in-transaction API, the transact/4 retry machinery, nested transactions,
  and commit/rollback plumbing.

  Two techniques are used:

  - For `transact` new-transaction paths, a *real* TransactionBuilder is
    started with a minimal transaction system layout (`%{epoch: 1, proxies:
    []}`). An empty (read-only) transaction commits locally with `{:ok, 0}`;
    a transaction with mutations fails commit with `{:error, :unavailable}`
    because there are no commit proxies — exercising the retry machinery
    deterministically without any cluster infrastructure.

  - For the nested-transaction and cast APIs, a scripted stub GenServer (or
    `self()`) is seeded through the transaction-context API, so exact
    call/cast messages can be asserted.
  """
  use ExUnit.Case, async: true

  alias Bedrock.Internal.Repo
  alias Bedrock.Internal.Repo.TransactionContext

  defmodule TestRepo do
    use Bedrock.Repo, cluster: MockCluster
  end

  # A cluster whose link!() returns a dead pid: Link.fetch_transaction_system_layout
  # then returns {:error, :unavailable}, which transact treats as retryable.
  defmodule DeadLinkCluster do
    @moduledoc false
    def link! do
      pid = spawn(fn -> :ok end)
      ref = Process.monitor(pid)

      receive do
        {:DOWN, ^ref, :process, ^pid, _} -> pid
      end
    end
  end

  # A cluster whose link!() returns the pid stored (by the test) in the
  # calling process's dictionary; transact runs in the test process.
  defmodule DictLinkCluster do
    @moduledoc false
    def link!, do: Process.get(:stub_link_pid)
  end

  # Stub TransactionBuilder: forwards every call/cast it receives to the test
  # process and replies to calls from a script (a list of {message, reply}
  # entries consumed in order for :commit; :nested_transaction always :ok).
  defmodule ScriptedTxn do
    @moduledoc false
    use GenServer

    def start_link(commit_replies) do
      GenServer.start_link(__MODULE__, {commit_replies, self()})
    end

    @impl true
    def init(state), do: {:ok, state}

    @impl true
    def handle_call(:nested_transaction, _from, {_, test_pid} = state) do
      send(test_pid, {:txn_call, :nested_transaction})
      {:reply, :ok, state}
    end

    def handle_call(:commit, _from, {[reply | rest], test_pid}) do
      send(test_pid, {:txn_call, :commit})
      {:reply, reply, {rest, test_pid}}
    end

    def handle_call({:get, key, opts}, _from, {[reply | rest], test_pid}) do
      send(test_pid, {:txn_call, {:get, key, opts}})
      {:reply, reply, {rest, test_pid}}
    end

    def handle_call({:get, key, opts}, _from, {[], test_pid} = state) do
      send(test_pid, {:txn_call, {:get, key, opts}})
      {:noreply, state}
    end

    @impl true
    def handle_cast(msg, {_, test_pid} = state) do
      send(test_pid, {:txn_cast, msg})
      {:noreply, state}
    end
  end

  # A commit proxy that rejects every transaction with the configured error.
  defmodule RejectingProxy do
    @moduledoc false
    use GenServer

    def start_link(error), do: GenServer.start_link(__MODULE__, error)

    @impl true
    def init(error), do: {:ok, error}

    @impl true
    def handle_call({:commit, _epoch, _tx, _mode}, _from, error), do: {:reply, {:error, error}, error}
  end

  @minimal_tsl %{epoch: 1, proxies: []}

  defp seed_txn(pid), do: TransactionContext.put_builder(TestRepo, pid)

  describe "cast-based transaction API" do
    test "add_read_conflict_key sends the exact cast to the transaction" do
      seed_txn(self())

      assert :ok = Repo.add_read_conflict_key(TestRepo, "read_key")
      assert_receive {:"$gen_cast", {:add_read_conflict_key, "read_key"}}
    end

    test "add_write_conflict_range sends the exact cast to the transaction" do
      seed_txn(self())

      assert :ok = Repo.add_write_conflict_range(TestRepo, "a", "z")
      assert_receive {:"$gen_cast", {:add_write_conflict_range, "a", "z"}}
    end

    test "clear_range sends the cast with default empty opts" do
      seed_txn(self())

      assert :ok = Repo.clear_range(TestRepo, "start", "end")
      assert_receive {:"$gen_cast", {:clear_range, "start", "end", []}}
    end

    test "clear_range forwards no_write_conflict option in the cast" do
      seed_txn(self())

      assert :ok = Repo.clear_range(TestRepo, "start", "end", no_write_conflict: true)
      assert_receive {:"$gen_cast", {:clear_range, "start", "end", [no_write_conflict: true]}}
    end

    test "clear sends the cast with opts" do
      seed_txn(self())

      assert :ok = Repo.clear(TestRepo, "doomed_key")
      assert_receive {:"$gen_cast", {:clear, "doomed_key", []}}
    end

    test "atomic sends the operation, key, and operand in the cast" do
      seed_txn(self())

      assert :ok = Repo.atomic(TestRepo, :add, "counter", <<1::64-little>>)
      assert_receive {:"$gen_cast", {:atomic, :add, "counter", <<1::64-little>>}}
    end

    test "cast API raises when no transaction is active" do
      assert_raise RuntimeError, "No active transaction", fn ->
        Repo.add_read_conflict_key(TestRepo, "key")
      end
    end
  end

  describe "get_range/3 default options" do
    test "delegates with default batch size of 100 and empty transaction opts" do
      test_pid = self()

      txn =
        spawn(fn ->
          receive do
            {:"$gen_call", from, msg} ->
              send(test_pid, {:range_call, msg})
              GenServer.reply(from, {:ok, {[{"k", "v"}], false}})
          end
        end)

      seed_txn(txn)

      assert [{"k", "v"}] = TestRepo |> Repo.get_range("a", "z") |> Enum.to_list()
      assert_receive {:range_call, {:get_range, "a", "z", 100, []}}
    end
  end

  describe "get_range/4 failure handling" do
    test "throws a retryable failure tuple when the batch call fails retryably" do
      txn =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:get_range, _, _, _, _}} ->
              GenServer.reply(from, {:failure, :unavailable})
          end
        end)

      seed_txn(txn)
      stream = Repo.get_range(TestRepo, "a", "z")

      assert {Repo, ^txn, :retryable_failure, :unavailable} = catch_throw(Enum.to_list(stream))
    end

    test "raises when the batch call fails with a non-retryable error" do
      txn =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:get_range, _, _, _, _}} ->
              GenServer.reply(from, {:error, :invalid_range})
          end
        end)

      seed_txn(txn)
      stream = Repo.get_range(TestRepo, "a", "z")

      assert_raise RuntimeError, "Range query failed: :invalid_range", fn ->
        Enum.to_list(stream)
      end
    end
  end

  describe "transact/4 with a provided transaction system layout" do
    test "runs an arity-0 function and returns its result on commit" do
      result = Repo.transact(NoCluster, TestRepo, fn -> :computed_result end, transaction_system_layout: @minimal_tsl)

      assert result == :computed_result
      assert TransactionContext.builder(TestRepo) == nil
    end

    test "passes the repo module to an arity-1 function" do
      result =
        Repo.transact(NoCluster, TestRepo, fn repo -> {:got_repo, repo} end, transaction_system_layout: @minimal_tsl)

      assert result == {:got_repo, TestRepo}
    end

    test "makes the builder visible through the transaction context while running" do
      txn_during =
        Repo.transact(NoCluster, TestRepo, fn -> TransactionContext.builder(TestRepo) end,
          transaction_system_layout: @minimal_tsl
        )

      assert is_pid(txn_during)
      assert TransactionContext.builder(TestRepo) == nil
    end

    test "rolls back and reraises when the function raises" do
      Process.flag(:trap_exit, true)

      assert_raise ArgumentError, "boom", fn ->
        Repo.transact(NoCluster, TestRepo, fn -> raise ArgumentError, "boom" end,
          transaction_system_layout: @minimal_tsl
        )
      end

      assert TransactionContext.builder(TestRepo) == nil
    end

    test "Repo.rollback inside the function returns {:error, reason}" do
      Process.flag(:trap_exit, true)

      result =
        Repo.transact(NoCluster, TestRepo, fn -> Repo.rollback(:user_requested) end,
          transaction_system_layout: @minimal_tsl
        )

      assert result == {:error, :user_requested}
      assert TransactionContext.builder(TestRepo) == nil
    end

    test "surfaces key_out_of_range immediately instead of retrying" do
      # A commit proxy that rejects the transaction with a permanent client
      # error: transact must surface it on the first attempt - retrying a
      # deterministic rejection would only burn the transaction deadline.
      proxy = start_supervised!({RejectingProxy, {:key_out_of_range, <<0xFF, 0xFF>>}})
      attempts = :counters.new(1, [])

      result =
        Repo.transact(
          NoCluster,
          TestRepo,
          fn ->
            :counters.add(attempts, 1, 1)
            Repo.put(TestRepo, "key", "value")
          end,
          transaction_system_layout: %{epoch: 1, proxies: [proxy]},
          retry_limit: 3
        )

      assert result == {:error, {:key_out_of_range, <<0xFF, 0xFF>>}}
      assert :counters.get(attempts, 1) == 1
      assert TransactionContext.builder(TestRepo) == nil
    end

    test "raises after exhausting the retry limit when commit keeps failing" do
      # A put makes the transaction non-empty; with no commit proxies in the
      # layout, commit fails with :unavailable, a retryable failure.
      Process.flag(:trap_exit, true)

      attempts = :counters.new(1, [])

      assert_raise RuntimeError,
                   "Transaction retry limit exceeded after 2 attempts. Last error: :unavailable",
                   fn ->
                     Repo.transact(
                       NoCluster,
                       TestRepo,
                       fn ->
                         :counters.add(attempts, 1, 1)
                         Repo.put(TestRepo, "key", "value")
                       end,
                       transaction_system_layout: @minimal_tsl,
                       retry_limit: 2
                     )
                   end

      # Initial attempt + 2 retries, each in a fresh TransactionBuilder.
      assert :counters.get(attempts, 1) == 3
      assert TransactionContext.builder(TestRepo) == nil
    end

    test "a transaction deadline bounds retries without resetting" do
      started_at = System.monotonic_time(:millisecond)

      assert_raise RuntimeError,
                   "Transaction timed out after 30ms. Last error: :unavailable",
                   fn ->
                     Repo.transact(DeadLinkCluster, TestRepo, fn -> :never_runs end, timeout_in_ms: 30)
                   end

      assert System.monotonic_time(:millisecond) - started_at < 500
      assert TransactionContext.builder(TestRepo) == nil
    end

    test "the deadline is inherited by nested transaction work" do
      assert_raise RuntimeError,
                   "Transaction timed out after 20ms. Last error: :timeout",
                   fn ->
                     Repo.transact(
                       NoCluster,
                       TestRepo,
                       fn ->
                         Repo.transact(NoCluster, TestRepo, fn ->
                           Process.sleep(30)
                           :too_late
                         end)
                       end,
                       transaction_system_layout: @minimal_tsl,
                       timeout_in_ms: 20
                     )
                   end

      assert TransactionContext.builder(TestRepo) == nil
    end
  end

  describe "transact/4 fetching the layout from the cluster link" do
    test "raises after the retry limit when the layout is never available" do
      # link!() returns a dead pid, so fetching the layout fails with
      # :unavailable before any transaction is started (rollback is a no-op).
      assert_raise RuntimeError,
                   "Transaction retry limit exceeded after 1 attempts. Last error: :unavailable",
                   fn ->
                     Repo.transact(DeadLinkCluster, TestRepo, fn -> :never_runs end, retry_limit: 1)
                   end

      assert TransactionContext.builder(TestRepo) == nil
    end

    test "retries a failed layout fetch and succeeds on the second attempt" do
      test_pid = self()

      link =
        spawn(fn ->
          receive do
            {:"$gen_call", from, :get_transaction_system_layout} ->
              GenServer.reply(from, {:error, :unavailable})
          end

          receive do
            {:"$gen_call", from, :get_transaction_system_layout} ->
              send(test_pid, :second_fetch)
              GenServer.reply(from, {:ok, %{epoch: 1, proxies: []}})
          end
        end)

      Process.put(:stub_link_pid, link)

      result = Repo.transact(DictLinkCluster, TestRepo, fn -> :eventually_committed end, [])

      assert result == :eventually_committed
      assert_received :second_fetch
      assert TransactionContext.builder(TestRepo) == nil
    end
  end

  describe "transact/4 nested transactions" do
    test "a client read call cannot wait forever for an unresponsive transaction builder" do
      {:ok, txn} = ScriptedTxn.start_link([])
      seed_txn(txn)

      assert_raise RuntimeError,
                   "Transaction timed out after 20ms. Last error: :timeout",
                   fn ->
                     Repo.transact(
                       NoCluster,
                       TestRepo,
                       fn -> Repo.get(TestRepo, "stuck") end,
                       timeout_in_ms: 20
                     )
                   end

      assert_received {:txn_call, :nested_transaction}
      assert_received {:txn_call, {:get, "stuck", []}}
      assert_receive {:txn_cast, :rollback}
      assert TransactionContext.builder(TestRepo) == txn
    end

    test "reuses the existing transaction and commits locally" do
      {:ok, txn} = ScriptedTxn.start_link([:ok])
      seed_txn(txn)

      result = Repo.transact(NoCluster, TestRepo, fn -> :nested_result end, [])

      assert result == :nested_result
      assert_received {:txn_call, :nested_transaction}
      assert_received {:txn_call, :commit}
      # The outer transaction is still active.
      assert TransactionContext.builder(TestRepo) == txn
    end

    test "returns the result when nested commit reports a version" do
      {:ok, txn} = ScriptedTxn.start_link([{:ok, 42}])
      seed_txn(txn)

      assert :versioned = Repo.transact(NoCluster, TestRepo, fn -> :versioned end, [])
    end

    test "rolls back and retries when nested commit fails retryably" do
      {:ok, txn} = ScriptedTxn.start_link([{:error, :timeout}, :ok])
      seed_txn(txn)

      runs = :counters.new(1, [])

      result =
        Repo.transact(
          NoCluster,
          TestRepo,
          fn ->
            :counters.add(runs, 1, 1)
            :retried_result
          end,
          []
        )

      assert result == :retried_result
      assert :counters.get(runs, 1) == 2
      assert_received {:txn_call, :nested_transaction}
      assert_received {:txn_call, :commit}
      assert_received {:txn_cast, :rollback}
      assert_received {:txn_call, :nested_transaction}
      assert_received {:txn_call, :commit}
      refute_received {:txn_cast, :rollback}
    end

    test "rolls back the nested transaction and reraises when the function raises" do
      {:ok, txn} = ScriptedTxn.start_link([])
      seed_txn(txn)

      assert_raise ArgumentError, "nested boom", fn ->
        Repo.transact(NoCluster, TestRepo, fn -> raise ArgumentError, "nested boom" end, [])
      end

      assert_received {:txn_call, :nested_transaction}
      assert_receive {:txn_cast, :rollback}
      refute_received {:txn_call, :commit}
    end

    test "Repo.rollback in a nested transaction rolls back and returns {:error, reason}" do
      {:ok, txn} = ScriptedTxn.start_link([])
      seed_txn(txn)

      result = Repo.transact(NoCluster, TestRepo, fn -> Repo.rollback(:nested_abort) end, [])

      assert result == {:error, :nested_abort}
      assert_received {:txn_call, :nested_transaction}
      assert_receive {:txn_cast, :rollback}
      refute_received {:txn_call, :commit}
      # The outer transaction remains active for the caller.
      assert TransactionContext.builder(TestRepo) == txn
    end

    test "a non-retryable operation error rolls back and raises with operation context" do
      {:ok, txn} = ScriptedTxn.start_link([{:error, :unsupported_operation}])
      seed_txn(txn)

      assert_raise RuntimeError,
                   ~s(Transaction operation get failed for key "some_key": :unsupported_operation),
                   fn ->
                     Repo.transact(NoCluster, TestRepo, fn -> Repo.get(TestRepo, "some_key") end, [])
                   end

      assert_received {:txn_call, :nested_transaction}
      assert_received {:txn_call, {:get, "some_key", []}}
      assert_receive {:txn_cast, :rollback}
      refute_received {:txn_call, :commit}
    end
  end
end
