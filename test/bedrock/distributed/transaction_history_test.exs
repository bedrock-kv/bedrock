defmodule Bedrock.Distributed.TransactionHistoryTest do
  use ExUnit.Case, async: false

  alias Bedrock.DataPlane.Transaction
  alias Bedrock.Test.History.Driver
  alias Bedrock.Test.History.Gates
  alias Bedrock.Test.History.Oracle

  defmodule Cluster do
    @moduledoc false
    use Bedrock.Cluster, otp_app: :bedrock, name: "transaction_history"
  end

  defmodule Repo do
    use Bedrock.Repo, cluster: Cluster
  end

  @moduletag :distributed
  @moduletag timeout: 60_000

  setup_all do
    assert Node.alive?(), "run with elixir --sname bedrock_history -S mix test --include distributed"
    root = Path.join(System.tmp_dir!(), "bedrock-history-#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)
    previous = Application.get_env(:bedrock, Cluster)
    previous_storage = Application.get_env(:bedrock, Bedrock.ObjectStorage)
    gates = start_supervised!({Agent, fn -> nil end})
    backend = {Gates, root: Path.join(root, "objects"), gates: gates}
    Application.put_env(:bedrock, Bedrock.ObjectStorage, backend: backend)

    Application.put_env(:bedrock, Cluster,
      capabilities: [:coordination, :log, :materializer],
      durability_mode: :relaxed,
      path_to_descriptor: Path.join(root, "descriptor"),
      object_storage: backend,
      coordinator: [path: root],
      materializer: [path: root, object_storage: backend],
      log: [path: root, object_storage: backend]
    )

    supervisor = start_supervised!({Cluster, []})

    on_exit(fn ->
      if previous, do: Application.put_env(:bedrock, Cluster, previous), else: Application.delete_env(:bedrock, Cluster)

      if previous_storage,
        do: Application.put_env(:bedrock, Bedrock.ObjectStorage, previous_storage),
        else: Application.delete_env(:bedrock, Bedrock.ObjectStorage)

      File.rm_rf!(root)
    end)

    Repo.transact(fn -> Repo.put("ready", "yes") end, timeout_in_ms: 15_000)
    assert Repo.transact(fn -> Repo.get("ready") end, timeout_in_ms: 15_000) == "yes"
    %{root: root, supervisor: supervisor, gates: gates, backend: backend}
  end

  setup context do
    {:ok, recorder} = Driver.start_recorder()
    Process.unlink(recorder)
    Driver.metadata(recorder, %{scenario: context.test, seed: Map.get(context, :history_seed, 239)})
    id = Driver.attach(recorder)

    on_exit(fn ->
      :telemetry.detach(id)
      path = Driver.artifact(recorder, "transaction-history")
      IO.puts("Transaction history artifact: #{path}")
      Agent.stop(recorder)
    end)

    Repo.transact(fn -> Repo.clear_range({"history/", "history0"}) end)
    %{recorder: recorder}
  end

  @tag counterexample: true
  test "acknowledged atomic increments in a shared batch match sequential arithmetic", %{recorder: recorder} do
    %{proxies: [proxy]} = Cluster.transaction_system_layout!()
    :ok = :sys.suspend(proxy)

    tasks =
      try do
        tasks =
          for n <- 1..8 do
            Task.async(fn -> Driver.attempt(Repo, recorder, "atomic-#{n}", [{:add, "history/atomic/counter", 1}]) end)
          end

        wait_until(fn -> elem(Process.info(proxy, :message_queue_len), 1) >= 8 end)
        tasks
      after
        :sys.resume(proxy)
      end

    entries = Enum.map(tasks, &Task.await(&1, 15_000))
    assert Enum.all?(entries, &(&1.status == :committed))
    batches = Agent.get(recorder, & &1.batches)
    assert Enum.any?(batches, &(length(&1.ids) == 8)), inspect(batches)
    assert_history(recorder, "atomic", entries)
  end

  @tag counterexample: true
  test "half-open range clears preserve their endpoint through the complete transaction path", %{recorder: recorder} do
    first =
      Driver.attempt(Repo, recorder, "bounds-seed", [
        {:put, "history/bounds/a", "a"},
        {:put, "history/bounds/b", "b"},
        {:put, "history/bounds/c", "c"}
      ])

    second = Driver.attempt(Repo, recorder, "bounds-clear", [{:clear_range, "history/bounds/a", "history/bounds/b"}])
    third = Driver.attempt(Repo, recorder, "bounds-read", [{:range, "history/bounds/a", "history/bounds/d"}])
    assert Enum.all?([first, second, third], &(&1.status == :committed))
    assert_history(recorder, "bounds", [first, second, third])
  end

  @tag counterexample: true
  test "concurrent conditional reservations based on absence admit only one key", %{recorder: recorder} do
    parent = self()

    tasks =
      for key <- ["a", "b"] do
        Task.async(fn ->
          barrier = fn _ ->
            send(parent, {:read_complete, self()})

            receive do
              :release -> :ok
            after
              5_000 -> raise "read barrier timed out"
            end
          end

          Driver.attempt(
            Repo,
            recorder,
            "reservation-#{key}",
            [{:reserve, {"history/reservation/", "history/reservation0"}, "history/reservation/" <> key}],
            after_read: barrier
          )
        end)
      end

    await_and_release_readers(tasks, fn ->
      attempts = Agent.get(recorder, &Map.values(&1.attempts))
      assert length(attempts) == 2
      assert Enum.all?(attempts, &(&1.status == :in_flight and not &1.callback_complete))
      assert Enum.all?(attempts, &(&1.reads == [{:reserve, true}]))
    end)

    entries = Enum.map(tasks, &Task.await(&1, 15_000))
    assert Enum.count(entries, &(&1.status == :committed)) == 1
    assert Enum.count(entries, &(&1.status == :aborted)) == 1

    retry =
      Driver.attempt(Repo, recorder, "reservation-retry", [
        {:reserve, {"history/reservation/", "history/reservation0"}, "history/reservation/retry"}
      ])

    assert retry.status == :committed
    assert retry.reads == [{:reserve, false}]
    assert_history(recorder, "reservation", entries ++ [retry])
  end

  test "point-dependent transfers preserve a multi-key invariant", %{recorder: recorder} do
    seed = Driver.attempt(Repo, recorder, "transfer-seed", [{:put, "history/alice", <<10::64-little>>}])
    parent = self()

    tasks =
      for n <- 1..2,
          do:
            Task.async(fn ->
              barrier = fn _ ->
                send(parent, {:read_complete, self()})

                receive do
                  :release -> :ok
                after
                  5_000 -> raise "read barrier timed out"
                end
              end

              Driver.attempt(Repo, recorder, "transfer-#{n}", [{:transfer, "history/alice", "history/bob", 7}],
                after_read: barrier
              )
            end)

    await_and_release_readers(tasks)
    entries = Enum.map(tasks, &Task.await(&1, 15_000))
    assert Enum.count(entries, &(&1.status == :committed)) == 1
    assert Enum.count(entries, &(&1.status == :aborted)) == 1
    assert_history(recorder, "transfer", [seed | entries])
  end

  @tag counterexample: true
  test "public pending writes are included in an otherwise empty range", %{recorder: recorder} do
    entry =
      Driver.attempt(Repo, recorder, "pending", [
        {:put, "history/local/b", "new"},
        {:range, "history/local/a", "history/local/c"}
      ])

    assert entry.status == :committed
    assert_history(recorder, "pending", [entry])
  end

  for seed_value <- [239, 240, 241] do
    @tag history_seed: seed_value
    test "mixed mutation history seed #{seed_value} agrees with the sequential model", %{
      recorder: recorder,
      history_seed: seed_value
    } do
      seed =
        Driver.attempt(Repo, recorder, "mixed-seed", [
          {:put, "history/mixed/a", <<7::64-little>>},
          {:put, "history/mixed/b", <<11::64-little>>}
        ])

      choices = [
        {:put, "history/mixed/b", <<19::64-little>>},
        {:clear, "history/mixed/a"},
        {:clear_range, "history/mixed/a", "history/mixed/b"},
        {:add, "history/mixed/b", 1}
      ]

      rng = :rand.seed_s(:exsss, {seed_value, seed_value + 1, seed_value + 2})

      {operations, _} =
        Enum.map_reduce(1..3, rng, fn _, rng ->
          {index, rng} = :rand.uniform_s(length(choices), rng)
          {Enum.at(choices, index - 1), rng}
        end)

      entries =
        operations
        |> Enum.with_index()
        |> Enum.map(fn {op, i} ->
          Task.async(fn -> Driver.attempt(Repo, recorder, "mixed-#{i}", [op]) end)
        end)
        |> Enum.map(&Task.await(&1, 15_000))

      assert Enum.all?([seed | entries], &(&1.status == :committed))
      Driver.artifact(recorder, "mixed", %{seed: seed_value})
      assert_history(recorder, "mixed-#{seed_value}", [seed | entries])
    end
  end

  defp await_and_release_readers(tasks, inspect_pending \\ fn -> :ok end) do
    for _ <- tasks, do: assert_receive({:read_complete, _}, 5_000)
    inspect_pending.()
  after
    Enum.each(tasks, &send(&1.pid, :release))
  end

  test "failed callbacks retain partial read evidence without committing", %{recorder: recorder} do
    entry =
      Driver.attempt(Repo, recorder, "failed-callback", [{:put, "history/fail", "local"}, {:get, "history/fail"}],
        after_read: fn _ -> raise "controlled callback failure" end
      )

    assert entry.status == :aborted
    refute entry.callback_complete
    assert entry.reads == [{:get, "history/fail", "local"}]
    assert_history(recorder, "failed-callback", [entry])
  end

  for boundary <- [:before_wal_append, :after_wal_sync] do
    @tag boundary: boundary
    test "coupled coordinator and log crash at #{boundary} preserves acknowledged history across an epoch", %{
      recorder: recorder,
      boundary: boundary
    } do
      seed = Driver.attempt(Repo, recorder, "crash-seed", [{:put, "history/safe", "acknowledged"}])
      assert seed.status == :committed
      %{epoch: epoch, logs: logs} = Cluster.transaction_system_layout!()
      [log_id] = Map.keys(logs)
      log = Process.whereis(Cluster.otp_name_for_worker(log_id))
      assert is_pid(log)
      {:ok, wal_gate} = Agent.start_link(fn -> nil end)
      handler_id = {__MODULE__, boundary, make_ref()}
      :ok = :telemetry.attach(handler_id, [:bedrock, :log, :push], &Gates.log_event/4, wal_gate)
      marker = "history/meta/crash-tail"

      match = fn encoded ->
        {:ok, tx} = Transaction.decode(encoded)
        Enum.any?(tx.mutations, &match?({:set, ^marker, _}, &1))
      end

      if boundary == :before_wal_append,
        do: :sys.suspend(log),
        else: Gates.arm(wal_gate, %{stage: :after_wal_sync, match: match, owner: self()})

      task =
        Task.async(fn ->
          Driver.attempt(Repo, recorder, "crash-tail", [{:add, "history/tail", 1}], timeout_in_ms: 2_000)
        end)

      coordinator = Process.whereis(Cluster.otp_name(:coordinator))
      coupled = System.get_env("BEDROCK_HISTORY_SINGLE_LOG_REPRO") != "1"
      Driver.metadata(recorder, %{epoch_before: epoch, boundary: boundary, coordinator_crashed: coupled})

      try do
        gate_evidence =
          case boundary do
            :before_wal_append ->
              wait_until(fn -> elem(Process.info(log, :message_queue_len), 1) > 0 end)
              :queued_before_append

            :after_wal_sync ->
              assert_receive {:history_gate, :after_wal_sync, ^log, _token, encoded}, 5_000
              {:after_sync_before_reply, encoded}
          end

        Driver.metadata(recorder, %{fault: gate_evidence})
        if coupled, do: :sys.suspend(coordinator)
        monitor = Process.monitor(log)
        Process.exit(log, :kill)
        assert_receive {:DOWN, ^monitor, :process, ^log, :killed}, 5_000

        if coupled do
          coordinator_monitor = Process.monitor(coordinator)
          Process.exit(coordinator, :kill)
          assert_receive {:DOWN, ^coordinator_monitor, :process, ^coordinator, :killed}, 5_000
        end

        tail = Task.await(task, 10_000)
        assert tail.status in [:unknown, :aborted]

        wait_until(
          fn ->
            case Cluster.fetch_transaction_system_layout() do
              {:ok, %{epoch: current}} -> current > epoch
              _ -> false
            end
          end,
          3_000
        )

        assert Repo.transact(fn -> Repo.get("history/safe") end, timeout_in_ms: 15_000) == "acknowledged"

        if boundary == :before_wal_append do
          assert Repo.transact(fn -> Repo.get("history/tail") end) == nil
        end

        Driver.artifact(recorder, "wal-#{boundary}", %{
          seed: 239,
          epoch_before: epoch,
          epoch_after: Cluster.transaction_system_layout!().epoch,
          coordinator_crashed: coupled,
          fault: gate_evidence
        })

        assert_history(recorder, "wal-#{boundary}", [seed, tail])
      after
        :telemetry.detach(handler_id)
        Gates.disarm(wal_gate)
        if Process.alive?(log), do: :sys.resume(log)
        if coupled and Process.alive?(coordinator), do: :sys.resume(coordinator)
      end
    end
  end

  defp assert_history(recorder, scenario, entries) do
    final = Repo.transact(fn -> {"history/", "history0"} |> Repo.get_range() |> Enum.to_list() |> Map.new() end)
    artifact = Driver.artifact(recorder, scenario, %{final: final})
    assert {:ok, _} = Oracle.check(%{}, entries, final), "history has no legal serialization: #{artifact}"
  end

  defp wait_until(predicate, attempts \\ 200)
  defp wait_until(_predicate, 0), do: flunk("condition did not become true")

  defp wait_until(predicate, attempts) do
    if predicate.(),
      do: :ok,
      else:
        (
          Process.sleep(10)
          wait_until(predicate, attempts - 1)
        )
  end
end
