defmodule Bedrock.DataPlane.Materializer.Olivine.MutationOrderTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.CommitProxy.Batch
  alias Bedrock.DataPlane.CommitProxy.Finalization
  alias Bedrock.DataPlane.CommitProxy.ResolverLayout
  alias Bedrock.DataPlane.CommitProxy.RoutingData
  alias Bedrock.DataPlane.Materializer.Olivine.Database
  alias Bedrock.DataPlane.Materializer.Olivine.Index
  alias Bedrock.DataPlane.Materializer.Olivine.Index.Page
  alias Bedrock.DataPlane.Materializer.Olivine.IndexManager
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version
  alias Bedrock.Test.DataPlane.FinalizationTestSupport, as: Support

  @cases [
    add_add: [{:atomic, :add, "key", <<1>>}, {:atomic, :add, "key", <<1>>}],
    set_atomic: [{:set, "key", <<5>>}, {:atomic, :add, "key", <<2>>}],
    clear_atomic: [{:clear, "key"}, {:atomic, :add, "key", <<2>>}],
    set_range: [{:set, "newkey", <<5>>}, {:clear_range, "new", "newz"}],
    range_set: [{:clear_range, "j", "l"}, {:set, "key", <<5>>}],
    range_atomic: [{:clear_range, "j", "l"}, {:atomic, :add, "key", <<2>>}]
  ]

  for {name, mutations} <- @cases do
    test "sequential oracle: #{name}", %{tmp_dir: dir} do
      check_history(dir, [[{:set, "key", <<9>>}], unquote(Macro.escape(mutations))])
    end
  end

  @moduletag :tmp_dir

  test "staged inserts and clears span existing pages and newly split pages", %{tmp_dir: dir} do
    seed = for n <- 1..800, do: {:set, key(n), <<9>>}
    inserts = for n <- 1..800, do: {:set, key(n) <> "x", <<3>>}

    mutations =
      inserts ++
        [
          {:clear_range, key(100), key(700) <> "w"},
          {:atomic, :add, key(400), <<2>>},
          {:set, key(600), <<7>>},
          {:atomic, :add, key(799) <> "x", <<2>>}
        ]

    check_history(dir, [seed, mutations])
  end

  test "all three-operation histories agree with the sequential oracle", %{tmp_dir: dir} do
    operations = [{:set, "key", <<5>>}, {:clear, "key"}, {:clear_range, "j", "l"}, {:atomic, :add, "key", <<2>>}]
    histories = for first <- operations, second <- operations, third <- operations, do: [first, second, third]

    for {history, n} <- Enum.with_index(histories), seed <- [[], [{:set, "key", <<9>>}]] do
      history_dir = Path.join(dir, "#{n}-#{length(seed)}")
      File.mkdir_p!(history_dir)
      check_history(history_dir, [seed, history])
    end
  end

  test "range clears persist empty page zero and removed page links", %{tmp_dir: dir} do
    seed = for n <- 1..800, do: {:set, key(n), <<9>>}
    check_history(dir, [seed, [{:clear_range, "k", "l"}]])
  end

  test "separate durable advances preserve predecessor-only deletion and recycled page IDs", %{tmp_dir: dir} do
    path = Path.join(dir, "incremental")
    seed = for n <- 1..800, do: {:set, key(n), <<9>>}
    {manager, db, expected} = durable_step(IndexManager.new(), open_database(path), %{}, seed, path)
    [{_, {index, _}} | _] = manager.versions
    assert map_size(index.page_map) > 2
    {removed_page, _} = Map.fetch!(index.page_map, 1)
    removed_keys = Page.keys(removed_page)
    {predecessor, 1} = Map.fetch!(index.page_map, 0)
    clears = Enum.map(removed_keys, &{:clear, &1})
    {manager, db, expected} = durable_step(manager, db, expected, clears, path)
    [{_, {index, _}} | _] = manager.versions
    assert {^predecessor, next} = Map.fetch!(index.page_map, 0)
    refute next == 1
    refute Map.has_key?(index.page_map, 1)
    for key <- removed_keys, do: assert({:error, :not_found} = Index.locator_for_key(index, key))

    inserts = for n <- 1..800, do: {:set, "r" <> key(n), <<3>>}
    {manager, db, _} = durable_step(manager, db, expected, inserts, path)
    [{_, {index, _}} | _] = manager.versions
    assert Map.has_key?(index.page_map, 1)
    :ok = Database.close(db)
  end

  test "newly created atomic values observe earlier additions", %{tmp_dir: dir} do
    check_history(dir, [[{:atomic, :add, "counter", <<1>>}, {:atomic, :add, "counter", <<1>>}]])
  end

  for aborted <- [[], [15]] do
    test "commit proxy combines distinct clients in arrival order with aborts #{inspect(aborted)}", %{tmp_dir: dir} do
      aborted = unquote(aborted)
      test_pid = self()
      clients = for n <- 1..40, do: [{:set, "key", <<n>>}, {:atomic, :add, "key", <<1>>}]

      batch =
        Enum.reduce(
          Enum.with_index(clients),
          Batch.new_batch(0, Version.zero(), Version.from_integer(1)),
          fn {mutations, client}, batch ->
            Batch.add_transaction(
              batch,
              Transaction.encode(%{mutations: mutations}),
              fn reply -> send(test_pid, {:client, client, reply}) end,
              :user
            )
          end
        )

      routing =
        RoutingData.from_snapshot(%{
          shard_layout: %{<<255, 255>> => {0, <<>>}},
          log_map: %{0 => :log},
          log_services: %{},
          replication_factor: 1
        })

      test_pid = self()

      n_aborts = length(aborted)
      n_oks = 40 - n_aborts

      assert {:ok, ^n_aborts, ^n_oks} =
               Finalization.finalize_batch(batch,
                 epoch: 1,
                 sequencer: :sequencer,
                 resolver_layout: %ResolverLayout.Single{resolver_ref: :resolver},
                 metadata_apply_fn: Support.metadata_apply_fn(routing),
                 resolver_fn: fn _, _, last, version, transactions, _, _ ->
                   assert length(transactions) == 40
                   {:ok, aborted, Support.tiling_window(last, version)}
                 end,
                 batch_log_push_fn: fn _, transactions_by_log, _, _ ->
                   send(test_pid, {:logged, Map.fetch!(transactions_by_log, :log)})
                   :ok
                 end,
                 sequencer_notify_fn: fn _, _, _, _ -> :ok end
               )

      for client <- 0..39 do
        if client in aborted do
          assert_receive {:client, ^client, {:error, :aborted}}
        else
          version = Version.from_integer(1)
          assert_receive {:client, ^client, {:ok, ^version, ^client}}
        end
      end

      refute_receive {:client, _, _}

      clients =
        clients
        |> Enum.with_index()
        |> Enum.reject(fn {_, client} -> client in aborted end)
        |> Enum.flat_map(&elem(&1, 0))

      assert_receive {:logged, encoded}
      assert encoded |> Transaction.mutations!() |> Enum.to_list() == clients
      check_encoded_history(dir, [encoded], [clients])
    end
  end

  defp key(n), do: "k" <> String.pad_leading(Integer.to_string(n), 4, "0")

  defp check_history(dir, history) do
    encoded =
      history
      |> Enum.with_index(1)
      |> Enum.map(fn {mutations, n} ->
        Transaction.encode(%{mutations: mutations, commit_version: Version.from_integer(n)})
      end)

    check_encoded_history(dir, encoded, history)
  end

  defp check_encoded_history(dir, encoded, history) do
    path = Path.join(dir, "db")
    db = open_database(path)

    {manager, db, expected} =
      encoded
      |> Enum.zip(history)
      |> Enum.reduce({IndexManager.new(), db, %{}}, fn {tx, mutations}, {manager, db, expected} ->
        expected = interpret(expected, mutations)
        {manager, db} = IndexManager.apply_transaction(manager, tx, db)
        assert values(manager, db) == expected
        {manager, db, expected}
      end)

    pages = manager.output_queue |> :queue.to_list() |> Enum.map(&elem(&1, 3))

    {:ok, db, _} =
      Database.advance_durable_version(
        db,
        manager.current_version,
        Version.zero(),
        manager.last_version_ended_at_offset,
        pages
      )

    :ok = Database.close(db)
    db = open_database(path)
    {:ok, recovered} = IndexManager.recover_from_database(db)
    assert values(recovered, db) == expected
    :ok = Database.close(db)

    replay_db = open_database(Path.join(dir, "replay"))
    assert Database.durable_version(replay_db) == Version.zero()
    {replayed, replay_db} = IndexManager.apply_transactions(IndexManager.new(), encoded, replay_db)
    assert values(replayed, replay_db) == expected
    :ok = Database.close(replay_db)
  end

  defp durable_step(manager, db, expected, mutations, path) do
    version = Version.from_integer(Version.to_integer(manager.current_version) + 1)
    tx = Transaction.encode(%{commit_version: version, mutations: mutations})
    {manager, db} = IndexManager.apply_transaction(manager, tx, db)
    expected = interpret(expected, mutations)
    assert values(manager, db) == expected
    [{_, {_, pages}} | _] = manager.versions

    {:ok, db, _} =
      Database.advance_durable_version(
        db,
        version,
        Database.durable_version(db),
        manager.last_version_ended_at_offset,
        [pages]
      )

    :ok = Database.close(db)
    db = open_database(path)
    {:ok, manager} = IndexManager.recover_from_database(db)
    assert manager.current_version == version
    assert values(manager, db) == expected
    {manager, db, expected}
  end

  defp open_database(path) do
    File.mkdir_p!(path)
    {:ok, db} = Database.open(__MODULE__, Path.join(path, "db"))

    on_exit(fn ->
      try do
        Database.close(db)
      catch
        _, _ -> :ok
      end
    end)

    db
  end

  defp values(manager, db) do
    [{_, {index, _}} | _] = manager.versions

    {entries, visited} = chain_entries(index.page_map, 0, MapSet.new())
    assert MapSet.size(visited) == map_size(index.page_map)
    keys = Enum.map(entries, &elem(&1, 0))
    assert keys == Enum.sort(Enum.uniq(keys))

    Map.new(entries, fn {key, locator} ->
      assert {:ok, _, ^locator} = Index.locator_for_key(index, key)
      {:ok, value} = Database.load_value(db, locator)
      {key, value}
    end)
  end

  defp chain_entries(pages, id, visited) do
    refute MapSet.member?(visited, id)
    {page, next} = Map.fetch!(pages, id)
    visited = MapSet.put(visited, id)

    if next == 0 do
      {Page.key_locators(page), visited}
    else
      {remaining, visited} = chain_entries(pages, next, visited)
      {Page.key_locators(page) ++ remaining, visited}
    end
  end

  # Deliberately independent of production atomics, index routing and mutation helpers.
  defp interpret(state, mutations) do
    Enum.reduce(mutations, state, fn
      {:set, key, value}, state ->
        Map.put(state, key, value)

      {:clear, key}, state ->
        Map.delete(state, key)

      {:clear_range, first, last}, state ->
        Map.reject(state, fn {key, _} -> key >= first and key < last end)

      {:atomic, :add, key, <<operand>>}, state ->
        <<value>> = Map.get(state, key, <<0>>)
        current = value
        Map.put(state, key, <<rem(current + operand, 256)>>)
    end)
  end
end
