defmodule Bedrock.DataPlane.Materializer.Olivine.ExclusiveClearTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Materializer.Olivine.Database
  alias Bedrock.DataPlane.Materializer.Olivine.Index
  alias Bedrock.DataPlane.Materializer.Olivine.Index.Page
  alias Bedrock.DataPlane.Materializer.Olivine.IndexManager
  alias Bedrock.DataPlane.ShardRouter
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version
  alias Bedrock.Internal.TransactionBuilder.Tx

  @moduletag :tmp_dir

  for {name, keys, first, last} <- [
        {:single_page, ["a", "b", "c"], "a", "b"},
        {:empty, ["a", "b", "c"], "b", "b"},
        {:reversed, ["a", "b", "c"], "c", "a"},
        {:binary_prefix, [<<0>>, <<0, 0>>, <<0, 255>>, <<1>>], <<0>>, <<0, 255>>}
      ],
      pending <- [false, true] do
    test "#{name}: exclusive endpoint with pending=#{pending}", %{tmp_dir: dir} do
      keys = unquote(keys)
      first = unquote(first)
      last = unquote(last)

      pending = unquote(pending)
      path = Path.join(dir, "db-#{pending}")
      db = open_db(path)
      sets = Enum.map(keys, &{:set, &1, "value"})
      {manager, db} = apply_mutations(IndexManager.new(), db, if(pending, do: [], else: sets))
      db = persist(manager, db)
      mutations = if(pending, do: sets, else: [])
      {manager, db} = apply_mutations(manager, db, mutations ++ [{:clear_range, first, last}])
      expected = Enum.reject(keys, &(&1 >= first and &1 < last))
      assert_keys(manager, db, expected)
      db = persist(manager, db)
      Database.close(db)
      db = open_db(path)
      {:ok, recovered} = IndexManager.recover_from_database(db)
      assert recovered.current_version == manager.current_version
      assert_keys(recovered, db, expected)
      Database.close(db)
    end
  end

  test "exclusive ends at page edges retain endpoint and clear earlier page right edges", %{tmp_dir: dir} do
    keys = for n <- 1..800, do: key(n)

    ranges = [
      {key(100), key(200)},
      {key(100), key(201)},
      {key(100), key(400)},
      {key(100), key(401)},
      {key(200), key(700)},
      {key(201), key(201)},
      {key(1), key(800)}
    ]

    for {{first, last}, n} <- Enum.with_index(ranges) do
      path = Path.join(dir, "pages-#{n}")
      db = open_db(path)
      {manager, db} = apply_mutations(IndexManager.new(), db, Enum.map(keys, &{:set, &1, "value"}))
      [{_, {index, _}} | _] = manager.versions
      assert map_size(index.page_map) > 2
      assert {page_zero, next_id} = Map.fetch!(index.page_map, 0)
      assert Page.right_key(page_zero) == key(200)
      assert Page.left_key(Index.get_page!(index, next_id)) == key(201)
      db = persist(manager, db)
      {manager, db} = apply_mutations(manager, db, [{:set, last, "value"}, {:clear_range, first, last}])
      expected = Enum.reject(keys, &(&1 >= first and &1 < last))
      assert_keys(manager, db, expected)
      db = persist(manager, db)
      Database.close(db)
      db = open_db(path)
      {:ok, recovered} = IndexManager.recover_from_database(db)
      assert recovered.current_version == manager.current_version
      assert_keys(recovered, db, expected)
      Database.close(db)
    end
  end

  test "affected keys match the builder write conflict and shard boundary", %{tmp_dir: dir} do
    keys = ["a", "l", "m", "z"]
    tx = Tx.new() |> Tx.clear_range("a", "m") |> Tx.commit(nil)
    assert {:ok, %{write_conflicts: [{"a", "m"}]}} = Transaction.decode(tx)
    shards = :gb_trees.from_orddict([{"m", {0, ""}}, {<<255, 255>>, {1, "m"}}])
    assert [{0, "", "m"}] = ShardRouter.lookup_shards_with_ranges(shards, "a", "m")
    assert 1 = ShardRouter.lookup_shard(shards, "m")
    db = open_db(Path.join(dir, "conflicts"))
    {manager, db} = apply_mutations(IndexManager.new(), db, Enum.map(keys, &{:set, &1, "value"}))
    {:ok, tx} = Transaction.add_commit_version(tx, Version.from_integer(2))
    {manager, db} = IndexManager.apply_transaction(manager, tx, db)
    assert_keys(manager, db, ["m", "z"])
    Database.close(db)
  end

  defp key(n), do: "k" <> String.pad_leading(Integer.to_string(n), 4, "0")

  defp open_db(path) do
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

  defp apply_mutations(manager, db, mutations) do
    version = Version.from_integer(Version.to_integer(manager.current_version) + 1)
    IndexManager.apply_transaction(manager, Transaction.encode(%{commit_version: version, mutations: mutations}), db)
  end

  defp persist(manager, db) do
    [{_, {_, pages}} | _] = manager.versions

    {:ok, db, _} =
      Database.advance_durable_version(
        db,
        manager.current_version,
        Database.durable_version(db),
        manager.last_version_ended_at_offset,
        [pages]
      )

    db
  end

  defp assert_keys(manager, db, expected) do
    [{_, {index, _}} | _] = manager.versions
    {actual, visited} = chain_keys(index.page_map, 0, MapSet.new())
    assert MapSet.size(visited) == map_size(index.page_map)
    assert actual == expected

    for key <- expected do
      assert {:ok, _, locator} = Index.locator_for_key(index, key)
      assert {:ok, "value"} = Database.load_value(db, locator)
    end
  end

  defp chain_keys(pages, id, visited) do
    refute MapSet.member?(visited, id)
    {page, next} = Map.fetch!(pages, id)
    visited = MapSet.put(visited, id)

    if next == 0 do
      {Page.keys(page), visited}
    else
      {remaining, visited} = chain_keys(pages, next, visited)
      {Page.keys(page) ++ remaining, visited}
    end
  end
end
