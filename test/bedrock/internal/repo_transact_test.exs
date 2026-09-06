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

  alias Bedrock.Cluster.Link.RoutingCache
  alias Bedrock.DataPlane.Version
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

    test "surfaces a system-key read as a permanent error instead of retrying" do
      # A read the transaction is not allowed to address fails the same way a
      # rejected commit does: once, with the offending key, at the client. Its
      # old shape was :layout_lookup_failed - a RETRYABLE routing miss - so
      # the caller burned its whole deadline and then raised something
      # generic about a retry limit.
      attempts = :counters.new(1, [])
      key = <<0xFF, "/system/config/desired_commit_proxies">>

      result =
        Repo.transact(
          NoCluster,
          TestRepo,
          fn ->
            :counters.add(attempts, 1, 1)
            Repo.get(TestRepo, key, next_read_version_fn: fn _t -> {:ok, Version.from_integer(1)} end)
          end,
          transaction_system_layout: @minimal_tsl,
          retry_limit: 3
        )

      assert result == {:error, {:key_out_of_range, key}}
      assert :counters.get(attempts, 1) == 1
      assert TransactionContext.builder(TestRepo) == nil
    end

    test "read_system_keys: true lets the same read through to routing" do
      # With no routing_fn (a caller-provided layout is wiring only), the
      # read fails as :layout_lookup_failed - proof it passed the bound and
      # went looking for the shard, rather than being refused at the client.
      attempts = :counters.new(1, [])

      assert_raise RuntimeError, ~r/retry limit exceeded/, fn ->
        Repo.transact(
          NoCluster,
          TestRepo,
          fn ->
            :counters.add(attempts, 1, 1)

            Repo.get(TestRepo, <<0xFF, "/system/config/desired_commit_proxies">>,
              next_read_version_fn: fn _t -> {:ok, Version.from_integer(1)} end
            )
          end,
          transaction_system_layout: @minimal_tsl,
          read_system_keys: true,
          retry_limit: 1
        )
      end

      assert :counters.get(attempts, 1) == 2
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

  describe "transact/4 proxy-served routing" do
    defmodule RoutingCluster do
      @moduledoc false
      def link!, do: Process.get(:stub_link_pid)
      def otp_name_for_worker(id), do: :"repo_transact_routing_worker_#{id}"
      # The routing cache is read directly from ETS now, so the cluster
      # must name the table just as a real cluster does.
      # Fixed name: tests within a module run sequentially, and the
      # lookup happens in the TransactionBuilder process, which cannot
      # see the test's process dictionary.
      def otp_name(:link_routing), do: :repo_transact_routing_cache
    end

    # A real routing cache, seeded per test. The read path no longer goes
    # through the Link, so seeding the table IS how a "cache hit" is set
    # up -- the stub link only serves misses, the TSL, and invalidation.
    defp seed_routing_cache(entry) do
      table = RoutingCluster.otp_name(:link_routing)
      if :ets.whereis(table) != :undefined, do: :ets.delete(table)
      RoutingCache.new(table)

      case entry do
        {start_key, end_key, raw_ref} -> RoutingCache.insert(table, start_key, end_key, raw_ref)
        nil -> :ok
      end

      table
    end

    # A covering entry as the proxy serves it: shard bounds, tag, raw
    # string-encoded materializer ref.
    defp covering_entry(worker_id \\ "wkr1"), do: {<<>>, <<0xFF, 0xFF>>, 0, {worker_id, Atom.to_string(node())}}

    # The shape the Link caches: {start, end, raw_ref}.
    defp cached_entry({start_key, end_key, _tag, raw_ref}), do: {start_key, end_key, raw_ref}

    defp spawn_stub_materializer(test_pid, answers \\ 1) do
      materializer = spawn(fn -> materializer_loop(test_pid, answers) end)
      Process.register(materializer, RoutingCluster.otp_name_for_worker("wkr1"))
      materializer
    end

    defp materializer_loop(_test_pid, 0), do: :ok

    defp materializer_loop(test_pid, answers) do
      receive do
        {:"$gen_call", from, {:get, key, _version, _opts}} ->
          send(test_pid, {:materializer_got, key})
          GenServer.reply(from, {:ok, "routed_value"})
          materializer_loop(test_pid, answers - 1)
      end
    end

    defp spawn_stub_sequencer do
      spawn(fn -> sequencer_loop() end)
    end

    defp sequencer_loop do
      receive do
        {:"$gen_call", from, {:next_read_version, _epoch}} ->
          GenServer.reply(from, {:ok, Version.from_integer(1)})
          sequencer_loop()
      end
    end

    # A one-entry Link cache: covers [start, end) or nothing.
    defp link_loop(tsl, cached_entry, test_pid, table) do
      receive do
        {:"$gen_call", from, :get_transaction_system_layout} ->
          GenServer.reply(from, {:ok, tsl})
          link_loop(tsl, cached_entry, test_pid, table)

        {:"$gen_cast", {:cache_routing_entry, {start_key, end_key, raw_ref} = entry}} ->
          RoutingCache.insert(table, start_key, end_key, raw_ref)

          send(test_pid, {:routing_cached, entry})
          link_loop(tsl, entry, test_pid, table)

        {:"$gen_call", from, :invalidate_routing} ->
          RoutingCache.clear(table)
          GenServer.reply(from, :ok)
          send(test_pid, :routing_invalidated)
          link_loop(tsl, nil, test_pid, table)
      end
    end

    test "a cache miss fetches the single covering entry from a proxy, caches it, and routes the read" do
      test_pid = self()
      spawn_stub_materializer(test_pid)
      entry = covering_entry()
      expected_cached = cached_entry(entry)

      proxy =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:fetch_routing, key}} ->
              send(test_pid, {:proxy_asked, key})
              GenServer.reply(from, {:ok, entry})
          end
        end)

      tsl = %{epoch: 1, sequencer: spawn_stub_sequencer(), proxies: [proxy]}
      table = seed_routing_cache(nil)
      link = spawn(fn -> link_loop(tsl, nil, test_pid, table) end)
      Process.put(:stub_link_pid, link)

      result = Repo.transact(RoutingCluster, TestRepo, fn -> Repo.get(TestRepo, "some_key") end, [])

      assert result == "routed_value"
      assert_received {:materializer_got, "some_key"}
      # The ask was by key (GetKeyServerLocations, never a bulk map), and
      # the raw (string-ref) entry was cached back for the next
      # transaction on this node - the Link is the locationCache.
      assert_receive {:proxy_asked, "some_key"}
      assert_receive {:routing_cached, ^expected_cached}
    end

    test "a cache hit routes the read without touching any proxy" do
      test_pid = self()
      spawn_stub_materializer(test_pid)

      # No proxies at all: a proxy fetch would fail loudly.
      tsl = %{epoch: 1, sequencer: spawn_stub_sequencer(), proxies: []}
      table = seed_routing_cache(cached_entry(covering_entry()))
      link = spawn(fn -> link_loop(tsl, cached_entry(covering_entry()), test_pid, table) end)
      Process.put(:stub_link_pid, link)

      result = Repo.transact(RoutingCluster, TestRepo, fn -> Repo.get(TestRepo, "some_key") end, [])

      assert result == "routed_value"
      assert_received {:materializer_got, "some_key"}
      refute_received {:routing_cached, _}
    end

    test "a routing-shaped read failure invalidates the cache exactly once, and the retry refetches" do
      test_pid = self()
      spawn_stub_materializer(test_pid)

      # The cached entry routes to a worker that is not registered: the
      # read fails :unavailable (a dead ref IS what a stale snapshot
      # looks like). The retry must invalidate, refetch from the proxy -
      # whose fresh entry names the live worker - and succeed.
      stale = cached_entry(covering_entry("wkr_gone"))
      fresh = covering_entry()
      expected_cached = cached_entry(fresh)

      proxy =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:fetch_routing, _key}} -> GenServer.reply(from, {:ok, fresh})
          end
        end)

      tsl = %{epoch: 1, sequencer: spawn_stub_sequencer(), proxies: [proxy]}
      table = seed_routing_cache(stale)
      link = spawn(fn -> link_loop(tsl, stale, test_pid, table) end)
      Process.put(:stub_link_pid, link)

      result = Repo.transact(RoutingCluster, TestRepo, fn -> Repo.get(TestRepo, "some_key") end, [])

      assert result == "routed_value"
      # Exactly one invalidation: the failed attempt's reason fired it; the
      # first attempt (no prior failure) must not.
      assert_receive :routing_invalidated
      refute_received :routing_invalidated
      assert_receive {:routing_cached, ^expected_cached}
    end

    test "a second read in the same shard uses the builder-local entry — no re-ask" do
      test_pid = self()
      spawn_stub_materializer(test_pid, 2)
      entry = covering_entry()

      # The proxy answers exactly once and the Link never caches (always
      # :not_cached): a second resolve would hang on the dead proxy, so
      # the second read completing proves the builder-local index served
      # it (FDB: DatabaseContext locationCache + per-read resolution).
      proxy =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:fetch_routing, _key}} -> GenServer.reply(from, {:ok, entry})
          end
        end)

      never_caching_link = fn loop ->
        receive do
          {:"$gen_call", from, {:get_covering_entry, _key}} ->
            GenServer.reply(from, {:error, :not_cached})
            loop.(loop)

          {:"$gen_call", from, :get_transaction_system_layout} ->
            GenServer.reply(from, {:ok, %{epoch: 1, sequencer: spawn_stub_sequencer(), proxies: [proxy]}})
            loop.(loop)

          {:"$gen_cast", {:cache_routing_entry, _entry}} ->
            loop.(loop)
        end
      end

      link = spawn(fn -> never_caching_link.(never_caching_link) end)
      Process.put(:stub_link_pid, link)

      result =
        Repo.transact(
          RoutingCluster,
          TestRepo,
          fn -> {Repo.get(TestRepo, "key_one"), Repo.get(TestRepo, "key_two")} end,
          []
        )

      assert result == {"routed_value", "routed_value"}
      assert_received {:materializer_got, "key_one"}
      assert_received {:materializer_got, "key_two"}
    end

    test "a locked proxy is a retry, not an error: the next attempt refetches and succeeds" do
      test_pid = self()
      spawn_stub_materializer(test_pid)
      entry = covering_entry()

      # First ask: the proxy is still locked (recovery in flight from the
      # client's view). :locked classifies as retryable AND routing-
      # invalidating; the retry refetches and the second ask succeeds.
      proxy =
        spawn(fn ->
          receive do
            {:"$gen_call", from, {:fetch_routing, _key}} ->
              GenServer.reply(from, {:error, :locked})

              receive do
                {:"$gen_call", from2, {:fetch_routing, _key2}} -> GenServer.reply(from2, {:ok, entry})
              end
          end
        end)

      tsl = %{epoch: 1, sequencer: spawn_stub_sequencer(), proxies: [proxy]}
      table = seed_routing_cache(nil)
      link = spawn(fn -> link_loop(tsl, nil, test_pid, table) end)
      Process.put(:stub_link_pid, link)

      result = Repo.transact(RoutingCluster, TestRepo, fn -> Repo.get(TestRepo, "some_key") end, [])

      assert result == "routed_value"
      assert_receive :routing_invalidated
    end

    test "a slow materializer retries without dropping the node's routing cache — slow is not stale" do
      test_pid = self()

      # The materializer holds the call forever: the read times out. A
      # timeout is retryable but NOT routing-shaped — FDB's locationCache
      # survives timeouts (only definitive signals evict: an unroutable
      # key, no servers, a dead ref). Under overload, evicting on timeout
      # would convert latency into node-wide cache-thrash and refetch
      # traffic. A dead materializer surfaces as :unavailable, which does
      # evict.
      materializer = spawn(fn -> Process.sleep(:infinity) end)
      on_exit(fn -> Process.exit(materializer, :kill) end)
      Process.register(materializer, RoutingCluster.otp_name_for_worker("wkr1"))

      tsl = %{epoch: 1, sequencer: spawn_stub_sequencer(), proxies: []}
      table = seed_routing_cache(cached_entry(covering_entry()))
      link = spawn(fn -> link_loop(tsl, cached_entry(covering_entry()), test_pid, table) end)
      Process.put(:stub_link_pid, link)

      assert_raise RuntimeError, ~r/retry limit/, fn ->
        Repo.transact(
          RoutingCluster,
          TestRepo,
          fn -> Repo.get(TestRepo, "some_key") end,
          retry_limit: 1,
          timeout_in_ms: 2_000
        )
      end

      refute_received :routing_invalidated
    end

    test "a transaction that never reads never fetches routing" do
      test_pid = self()

      # Link serves wiring only; a routing fetch would hit the empty proxy
      # list and fail. Lazy routing means this commit-only (read-free)
      # transaction never asks.
      tsl = %{epoch: 1, sequencer: spawn_stub_sequencer(), proxies: []}
      table = seed_routing_cache(nil)
      link = spawn(fn -> link_loop(tsl, nil, test_pid, table) end)
      Process.put(:stub_link_pid, link)

      assert Repo.transact(RoutingCluster, TestRepo, fn -> :no_reads end, []) == :no_reads
      refute_received {:routing_cached, _}
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
