defmodule Bedrock.Internal.TransactionBuilder.RangeReadsTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Resolver
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version
  alias Bedrock.Internal.Repo.TransactionContext
  alias Bedrock.Internal.TransactionBuilder
  alias Bedrock.Internal.TransactionBuilder.RangeReads
  alias Bedrock.Internal.TransactionBuilder.State
  alias Bedrock.Internal.TransactionBuilder.Tx

  defmodule Repo do
    use Bedrock.Repo, cluster: UnusedCluster
  end

  # Only scheduling and storage are controlled here; the real builder encoder,
  # Resolver.Server verdict and public Repo retry loop all execute.
  defmodule ResolvingProxy do
    @moduledoc false
    use GenServer

    def start_link(opts), do: GenServer.start_link(__MODULE__, opts)
    def init(opts), do: {:ok, {opts, []}}
    def handle_call({:commit, 1, tx, :user}, from, {opts, []}), do: {:noreply, {opts, [{from, tx}]}}

    def handle_call({:commit, 1, tx, :user}, from, {opts, [{first, first_tx}]}) do
      old = Version.from_integer(100)
      next = Version.from_integer(200)
      {:ok, aborted, _} = Resolver.resolve_transactions(opts[:resolver], 1, old, next, [first_tx, tx], [[], []])
      {:ok, %{mutations: [{:set, key, value}]}} = Transaction.decode(first_tx)
      Agent.update(opts[:store], fn _ -> {next, [{key, value}]} end)
      GenServer.reply(first, {:ok, next, 0})
      GenServer.reply(from, if(1 in aborted, do: {:error, :conflict}, else: {:ok, next, 1}))
      {:noreply, {opts, []}}
    end
  end

  @version Version.from_integer(100)

  defp state(tx \\ Tx.new(), range \\ {"", "z"}) do
    {start_key, end_key} = range
    %State{read_version: @version, tx: tx, routing_fn: fn _ -> {:ok, {start_key, end_key, [self()]}} end}
  end

  defp empty_source(_server, _start, _end, _version, _opts), do: {:ok, {[], false}}

  defp read(state, range, opts \\ []) do
    RangeReads.get_range(
      state,
      range,
      Keyword.get(opts, :batch_size, 10),
      Keyword.put_new(opts, :storage_get_range_fn, &empty_source/5)
    )
  end

  defp conflicts(state) do
    {:ok, tx} = Transaction.decode(Tx.commit(state.tx, state.read_version))
    tx.read_conflicts
  end

  test "empty storage merges pending writes and clears inside the requested range" do
    tx =
      Tx.new()
      |> Tx.set("a", "outside")
      |> Tx.set("c", "local")
      |> Tx.set("d", "clear")
      |> Tx.clear("d")
      |> Tx.set("f", "outside")

    {s, result} = read(state(tx), {"b", "f"})
    assert result == {:ok, {[{"c", "local"}], false}}
    assert conflicts(s) == {@version, [{"b", "f"}]}
  end

  test "an empty range result records the full requested absence" do
    {s, {:ok, {[], false}}} = read(state(), {"b", "f"})
    assert conflicts(s) == {@version, [{"b", "f"}]}
  end

  test "snapshot range merges local writes without introducing read conflicts" do
    tx = Tx.new() |> Tx.add_read_conflict_key("existing") |> Tx.set("c", "local")
    s = state(tx)
    {after_read, result} = read(s, {"b", "f"}, snapshot: true)
    assert result == {:ok, {[{"c", "local"}], false}}
    assert after_read.tx.reads == s.tx.reads
    assert after_read.tx.range_reads == s.tx.range_reads
  end

  test "nonempty snapshot results do not leak into conflict tracking" do
    {s, result} =
      read(state(), {"b", "f"},
        snapshot: true,
        storage_get_range_fn: fn _, _, _, _, _ -> {:ok, {[{"c", "stored"}], false}} end
      )

    assert result == {:ok, {[{"c", "stored"}], false}}
    assert conflicts(s) == {nil, []}
  end

  test "public Repo put then range sees pending writes over empty storage" do
    {:ok, builder} = TransactionBuilder.start_link(transaction_system_layout: %{epoch: 1, proxies: []})
    :sys.replace_state(builder, fn _ -> state() end)
    TransactionContext.put_builder(Repo, builder)
    Repo.put("c", "local")
    assert Enum.to_list(Repo.get_range({"b", "f"}, storage_get_range_fn: &empty_source/5)) == [{"c", "local"}]
    GenServer.stop(builder)
  end

  test "two conditional inserts based on the same empty range cannot both resolve" do
    resolver =
      start_supervised!(
        {Resolver.Server, cluster: __MODULE__, director: self(), key_range: {"", "z"}, epoch: 1, last_version: @version}
      )

    transactions =
      for key <- ["c", "d"] do
        {s, {:ok, {[], false}}} = read(state(), {"b", "f"})
        s.tx |> Tx.set(key, "reserved") |> Tx.commit(@version)
      end

    assert {:ok, [1], _} =
             Resolver.resolve_transactions(resolver, 1, @version, Version.from_integer(200), transactions, [[], []])
  end

  test "pending writes paginate without reading or conflicting past the returned limit" do
    s = Tx.new() |> Tx.set("c", "1") |> Tx.set("d", "2") |> state()
    {s, first} = read(s, {"b", "f"}, batch_size: 1)
    assert first == {:ok, {[{"c", "1"}], true}}
    assert conflicts(s) == {@version, [{"b", "c\0"}]}
    {s, second} = read(s, {"c\0", "f"}, batch_size: 1)
    assert second == {:ok, {[{"d", "2"}], false}}
    assert conflicts(s) == {@version, [{"b", "f"}]}
  end

  test "pending writes before the first stored key appear in the ordered page" do
    s = state(Tx.set(Tx.new(), "b", "local"))

    {s, result} =
      read(s, {"a", "f"},
        batch_size: 1,
        storage_get_range_fn: fn _, _, _, _, _ -> {:ok, {[{"c", "stored"}], true}} end
      )

    assert result == {:ok, {[{"b", "local"}], true}}
    assert conflicts(s) == {@version, [{"a", "b\0"}]}
  end

  test "empty shards do not end a scan before subsequent covered shards" do
    s = %{
      state()
      | routing_fn: fn key ->
          if key < "m", do: {:ok, {"", "m", [:first]}}, else: {:ok, {"m", "z", [:second]}}
        end
    }

    source = fn
      :first, _, _, _, _ -> {:ok, {[], false}}
      :second, _, _, _, _ -> {:ok, {[{"n", "stored"}], false}}
    end

    {s, result} = read(s, {"b", "x"}, storage_get_range_fn: source)
    assert result == {:ok, {[{"n", "stored"}], false}}
    assert conflicts(s) == {@version, [{"b", "x"}]}
  end

  test "a fully cleared storage page continues to the next storage page" do
    s = state(Tx.clear(Tx.new(), "c"))

    source = fn _, start_key, _, _, _ ->
      if start_key <= "c", do: {:ok, {[{"c", "stored"}], true}}, else: {:ok, {[{"d", "stored"}], false}}
    end

    {s, result} = read(s, {"b", "f"}, batch_size: 1, storage_get_range_fn: source)
    assert result == {:ok, {[{"d", "stored"}], false}}
    assert conflicts(s) == {@version, [{"b", "f"}]}
  end

  test "empty query bounds add neither a read conflict nor a storage request" do
    s = state()
    source = fn _, _, _, _, _ -> flunk("empty query reached storage") end
    {after_read, result} = read(s, {"c", "c"}, storage_get_range_fn: source)
    assert result == {:ok, {[], false}}
    assert after_read.tx == s.tx
  end

  test "local range clears hide storage but preserve later local sets" do
    tx = Tx.new() |> Tx.clear_range("b", "f") |> Tx.set("c", "new")

    {_s, result} =
      read(state(tx), {"b", "f"},
        storage_get_range_fn: fn _, _, _, _, _ -> {:ok, {[{"c", "old"}, {"d", "old"}], false}} end
      )

    assert result == {:ok, {[{"c", "new"}], false}}
  end

  test "local sets followed by range clears leave an empty observed interval" do
    tx = Tx.new() |> Tx.set("c", "new") |> Tx.clear_range("b", "f")
    {s, result} = read(state(tx), {"b", "f"})
    assert result == {:ok, {[], false}}
    assert conflicts(s) == {@version, [{"b", "f"}]}
  end

  test "bounded pagination agrees with an independent local mutation model" do
    operations = [{:set, "b", "new"}, {:set, "d", "new"}, {:clear, "b"}, {:clear_range, "b", "e"}]

    for one <- operations,
        two <- operations,
        three <- operations,
        initial <- [%{}, %{"b" => "old", "c" => "old", "f" => "endpoint"}],
        batch_size <- [1, 2, 3] do
      {tx, model} =
        Enum.reduce([one, two, three], {Tx.new(), initial}, fn
          {:set, k, v}, {tx, model} ->
            {Tx.set(tx, k, v), Map.put(model, k, v)}

          {:clear, k}, {tx, model} ->
            {Tx.clear(tx, k), Map.delete(model, k)}

          {:clear_range, a, b}, {tx, model} ->
            {Tx.clear_range(tx, a, b), Map.reject(model, fn {k, _} -> a <= k and k < b end)}
        end)

      source = fn _, start_key, end_key, _, opts ->
        rows = initial |> Enum.filter(fn {k, _} -> start_key <= k and k < end_key end) |> Enum.sort()
        {page, rest} = Enum.split(rows, opts[:limit])
        {:ok, {page, rest != []}}
      end

      {actual, final_state} = collect_pages(state(tx), "a", "f", batch_size, source, [])
      expected = model |> Enum.filter(fn {k, _} -> "a" <= k and k < "f" end) |> Enum.sort()
      assert actual == expected, inspect({one, two, three, initial, batch_size})
      assert conflicts(final_state) == {@version, [{"a", "f"}]}
    end
  end

  defp collect_pages(s, cursor, end_key, size, source, acc) do
    {s, {:ok, {rows, more}}} = read(s, {cursor, end_key}, batch_size: size, storage_get_range_fn: source)

    if more do
      assert rows != []
      collect_pages(s, elem(List.last(rows), 0) <> <<0>>, end_key, size, source, acc ++ rows)
    else
      {acc ++ rows, s}
    end
  end

  test "nonprogressing and invalid coverage responses are retryable failures" do
    assert {_, {:failure, %{unavailable: []}}} =
             read(state(), {"b", "f"}, storage_get_range_fn: fn _, _, _, _, _ -> {:ok, {[], true}} end)

    assert {_, {:failure, %{unavailable: []}}} = read(state(Tx.new(), {"m", "z"}), {"b", "f"})
  end

  test "canonical selectors share empty-range merging and snapshot policy" do
    first = Bedrock.KeySelector.first_greater_or_equal("b")
    last = Bedrock.KeySelector.first_greater_or_equal("f")
    s = state(Tx.set(Tx.new(), "c", "new"))

    {after_read, result} =
      RangeReads.get_range_selectors(s, first, last, 10, snapshot: true, storage_get_range_fn: &empty_source/5)

    assert result == {:ok, {[{"c", "new"}], false}}
    assert after_read.tx == s.tx
    {after_read, _} = RangeReads.get_range_selectors(s, first, last, 10, storage_get_range_fn: &empty_source/5)
    assert conflicts(after_read) == {@version, [{"b", "f"}]}
  end

  test "offset selector singleton snapshots do not crash or add conflicts" do
    first = Bedrock.KeySelector.first_greater_than("b")
    last = Bedrock.KeySelector.first_greater_than("f")
    s = state()

    {after_read, result} =
      RangeReads.get_range_selectors(s, first, last, 10,
        snapshot: true,
        storage_get_range_fn: fn _, _, _, _, _ -> {:ok, {[{"c", "stored"}], false}} end
      )

    assert result == {:ok, {[{"c", "stored"}], false}}
    assert after_read.tx.reads == s.tx.reads
    assert after_read.tx.range_reads == s.tx.range_reads

    assert {_, {:ok, {[], false}}} =
             RangeReads.get_range_selectors(s, first, last, 10, snapshot: true, storage_get_range_fn: &empty_source/5)
  end

  test "public Repo retries the losing concurrent empty-range conditional insert" do
    resolver =
      start_supervised!(
        {Resolver.Server, cluster: __MODULE__, director: self(), key_range: {"", "z"}, epoch: 1, last_version: @version}
      )

    store = start_supervised!({Agent, fn -> {@version, []} end})
    proxy = start_supervised!({ResolvingProxy, resolver: resolver, store: store})
    owner = self()

    tasks =
      for key <- ["c", "d"] do
        Task.async(fn ->
          Repo.transact(
            fn ->
              send(owner, {:attempt, key})
              {version, rows} = Agent.get(store, & &1)
              builder = TransactionContext.builder(Repo)

              :sys.replace_state(builder, fn current ->
                %{current | read_version: version, routing_fn: fn _ -> {:ok, {"", "z", [owner]}} end}
              end)

              source = fn _, _, _, _, _ -> {:ok, {rows, false}} end

              case Enum.to_list(Repo.get_range({"b", "f"}, storage_get_range_fn: source)) do
                [] ->
                  Repo.put(key, "reserved")
                  :inserted

                [_] ->
                  :occupied
              end
            end,
            transaction_system_layout: %{epoch: 1, proxies: [proxy]},
            retry_limit: 1
          )
        end)
      end

    assert tasks |> Enum.map(&Task.await/1) |> Enum.sort() == [:inserted, :occupied]
    for _ <- 1..3, do: assert_receive({:attempt, _})
    refute_receive {:attempt, _}
    assert {_, [_]} = Agent.get(store, & &1)
  end

  test "snapshot scans through cleared pages preserve prior point and range conflicts" do
    tx = Tx.new() |> Tx.add_read_conflict_key("a") |> Tx.add_read_conflict_range("w", "x") |> Tx.clear("c")

    source = fn _, cursor, _, _, _ ->
      if cursor <= "c", do: {:ok, {[{"c", "old"}], true}}, else: {:ok, {[{"d", "old"}], false}}
    end

    {after_read, result} = read(state(tx), {"b", "f"}, batch_size: 1, snapshot: true, storage_get_range_fn: source)
    assert result == {:ok, {[{"d", "old"}], false}}
    assert after_read.tx == tx
  end

  test "empty or reversed ranges do not acquire an unset read version" do
    s = %{state() | read_version: nil}

    for range <- [{"c", "c"}, {"f", "b"}] do
      assert {^s, {:ok, {[], false}}} = read(s, range)
    end
  end
end
