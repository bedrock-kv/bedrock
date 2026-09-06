defmodule Bedrock.Test.History.SnapshotFixture do
  @moduledoc false
  import ExUnit.Assertions

  alias Bedrock.DataPlane.Materializer.Olivine.Database
  alias Bedrock.DataPlane.Materializer.Olivine.Index
  alias Bedrock.DataPlane.Materializer.Olivine.IndexManager
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.Service.Foreman
  alias Bedrock.Test.History.Driver
  alias Bedrock.Test.History.Gates
  alias Bedrock.Test.History.Oracle

  def materializer(cluster) do
    {:ok, services} = Foreman.get_all_running_services(cluster.otp_name(:foreman))

    Enum.find_value(services, fn
      {id, :materializer, name} ->
        pid = Process.whereis(name)
        if pid && :sys.get_state(pid).shard_num == 1, do: {id, pid}

      _ ->
        nil
    end)
  end

  def await(function, attempts \\ 1_500)
  def await(_function, 0), do: flunk("snapshot fixture did not reach its observed boundary")

  def await(function, attempts) do
    case function.() do
      nil ->
        Process.sleep(10)
        await(function, attempts - 1)

      false ->
        Process.sleep(10)
        await(function, attempts - 1)

      value ->
        value
    end
  end

  def baseline_batch(cluster, repo, recorder) do
    %{proxies: [proxy]} = cluster.transaction_system_layout!()
    :ok = :sys.suspend(proxy)

    tasks =
      try do
        tasks =
          for n <- [1, 2] do
            Task.async(fn ->
              Driver.attempt(repo, recorder, "snapshot-batch-#{n}", [
                {:add, "history/counter", n},
                {:put, "history/batch_order", Integer.to_string(n)}
              ])
            end)
          end

        await(fn -> elem(Process.info(proxy, :message_queue_len), 1) >= 2 end)
        tasks
      after
        :sys.resume(proxy)
      end

    entries = Enum.map(tasks, &Task.await(&1, 15_000))
    assert Enum.all?(entries, &(&1.status == :committed))
    ids = MapSet.new(entries, & &1.id)

    batch =
      Agent.get(recorder, fn history ->
        Enum.find(history.batches, &(MapSet.new(&1.ids) == ids))
      end)

    assert batch, "snapshot prefix transactions did not share one real log batch"
    batch.version
  end

  def record(recorder, event), do: Agent.update(recorder, &Map.update!(&1, :faults, fn events -> [event | events] end))

  def version(recorder, id) do
    await(fn ->
      Agent.get(recorder, fn state ->
        Enum.find_value(state.batches, fn batch -> if id in batch.ids, do: batch.version end)
      end)
    end)
  end

  # Decode only marker placement and version. Expected mutations stay in the
  # independent driver history and are interpreted exclusively by Oracle.
  def after_tail_event(_event, _measurements, %{transaction: encoded}, {gate, owner, marker}) do
    {:ok, decoded} = Transaction.decode(encoded)

    marker_seen =
      Enum.any?(decoded.mutations, fn
        {:set, ^marker, _} -> true
        _ -> false
      end)

    if marker_seen do
      Gates.arm(gate, %{stage: :after_wal_sync, match: fn _ -> true end, owner: owner})
    else
      Gates.pause(gate, :after_wal_sync, encoded)
    end
  end

  def startup_event(_event, _measurements, %{storage_id: id}, gate),
    do: Gates.pause(gate, :cold_replacement_started, id)

  def expected_prefix(recorder, version) do
    history = Agent.get(recorder, & &1)

    history.batches
    |> Enum.reverse()
    |> Enum.filter(&(&1.version <= version))
    |> Enum.flat_map(& &1.ids)
    |> Enum.reduce(%{}, fn id, state ->
      entry = Map.fetch!(history.attempts, id)
      assert entry.status == :committed
      {next, observations} = Oracle.evaluate(state, entry.ops)
      assert observations == entry.reads
      next
    end)
  end

  def cold_map(path) do
    name = String.to_atom("snapshot_cold_#{System.unique_integer([:positive])}")
    {:ok, database} = Database.open(name, Path.join(path, "dets"), pool_size: 1)

    try do
      {:ok, manager} = IndexManager.recover_from_database(database)
      [{version, {index, _}}] = manager.versions

      values =
        for {_, {page, _}} <- index.page_map,
            {key, locator} <- Index.Page.key_locators(page),
            key >= "history/" and key < "history0",
            into: %{} do
          {:ok, value} = Database.load_value(database, locator)
          {key, value}
        end

      {version, values}
    after
      Database.close(database)
    end
  end

  def assert_final(repo, recorder, scenario, extra) do
    final =
      repo.transact(fn -> {"history/", "history0"} |> repo.get_range() |> Enum.to_list() |> Map.new() end,
        timeout_in_ms: 15_000
      )

    artifact = Driver.artifact(recorder, scenario, Map.put(extra, :final, final))
    entries = Agent.get(recorder, &Map.values(&1.attempts))
    assert {:ok, _} = Oracle.check(%{}, entries, final), "history mismatch: #{artifact}"
    artifact
  end
end
