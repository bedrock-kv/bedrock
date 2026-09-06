defmodule Bedrock.Distributed.SnapshotHistoryTest do
  use ExUnit.Case, async: false

  alias Bedrock.DataPlane.Materializer.Olivine.Database
  alias Bedrock.DataPlane.Version
  alias Bedrock.ObjectStorage.Snapshot
  alias Bedrock.ObjectStorage.SnapshotBundle
  alias Bedrock.Service.Foreman
  alias Bedrock.Test.History.Driver
  alias Bedrock.Test.History.Gates
  alias Bedrock.Test.History.SnapshotFixture

  defmodule BeforeCluster do
    @moduledoc false
    use Bedrock.Cluster, otp_app: :bedrock, name: "snapshot_history_before"
  end

  defmodule BeforeRepo do
    use Bedrock.Repo, cluster: BeforeCluster
  end

  defmodule AfterCluster do
    @moduledoc false
    use Bedrock.Cluster, otp_app: :bedrock, name: "snapshot_history_after"
  end

  defmodule AfterRepo do
    use Bedrock.Repo, cluster: AfterCluster
  end

  @moduletag :distributed
  @moduletag timeout: 90_000

  for {stage, cluster, repo} <- [
        {:before_snapshot_publication, BeforeCluster, BeforeRepo},
        {:after_snapshot_publication, AfterCluster, AfterRepo}
      ] do
    test "exact historical snapshot and acknowledged tail survive #{stage}" do
      cluster = unquote(cluster)
      repo = unquote(repo)
      stage = unquote(stage)
      assert Node.alive?()
      root = Path.join(System.tmp_dir!(), "snapshot-history-#{System.unique_integer([:positive])}")
      File.mkdir_p!(root)
      previous = Application.get_env(:bedrock, cluster)
      previous_storage = Application.get_env(:bedrock, Bedrock.ObjectStorage)
      snapshots = start_supervised!({Agent, fn -> nil end}, id: :snapshot_gates)
      startups = start_supervised!({Agent, fn -> nil end}, id: :startup_gates)
      wal = start_supervised!({Agent, fn -> nil end}, id: :wal_gates)
      backend = {Gates, root: Path.join(root, "objects"), gates: snapshots}
      Application.put_env(:bedrock, Bedrock.ObjectStorage, backend: backend)

      Application.put_env(:bedrock, cluster,
        capabilities: [:coordination, :log, :materializer],
        durability_mode: :relaxed,
        path_to_descriptor: Path.join(root, "descriptor"),
        object_storage: backend,
        coordinator: [path: root],
        materializer: [path: root, object_storage: backend],
        log: [path: root, object_storage: backend]
      )

      start_supervised!({cluster, []})

      on_exit(fn ->
        if previous,
          do: Application.put_env(:bedrock, cluster, previous),
          else: Application.delete_env(:bedrock, cluster)

        if previous_storage,
          do: Application.put_env(:bedrock, Bedrock.ObjectStorage, previous_storage),
          else: Application.delete_env(:bedrock, Bedrock.ObjectStorage)

        File.rm_rf!(root)
      end)

      repo.transact(fn -> repo.put("ready", "yes") end, timeout_in_ms: 15_000)
      assert repo.transact(fn -> repo.get("ready") end, timeout_in_ms: 15_000) == "yes"
      {:ok, recorder} = Driver.start_recorder()
      Process.unlink(recorder)

      on_exit(fn ->
        path = Driver.artifact(recorder, "snapshot-#{stage}", %{seed: 239, stage: stage})
        IO.puts("Snapshot history artifact: #{path}")
        Agent.stop(recorder)
      end)

      trace = Driver.attach(recorder)
      on_exit(fn -> :telemetry.detach(trace) end)

      seed =
        Driver.attempt(repo, recorder, "snapshot-seed", [
          {:put, "history/counter", <<10::64-little>>},
          {:put, "history/value", "baseline"},
          {:put, "history/clear/a", "a"},
          {:put, "history/clear/b", "b"}
        ])

      assert seed.status == :committed
      seed_version = SnapshotFixture.baseline_batch(cluster, repo, recorder)
      {worker_id, materializer} = SnapshotFixture.await(fn -> SnapshotFixture.materializer(cluster) end)
      SnapshotFixture.await(fn -> Database.durable_version(:sys.get_state(materializer).database) >= seed_version end)

      gate_trace = {__MODULE__, self()}

      :ok =
        :telemetry.attach(
          gate_trace,
          [:bedrock, :log, :push],
          &SnapshotFixture.after_tail_event/4,
          {wal, self(), "history/meta/snapshot-tail"}
        )

      on_exit(fn -> :telemetry.detach(gate_trace) end)

      try do
        tail =
          Driver.attempt(repo, recorder, "snapshot-tail", [
            {:add, "history/counter", 5},
            {:put, "history/value", "later"},
            {:clear_range, "history/clear/a", "history/clear/b"}
          ])

        assert tail.status == :committed
        tail_version = SnapshotFixture.version(recorder, tail.id)
        assert_receive {:history_gate, :after_wal_sync, log, wal_token, _}, 5_000

        try do
          state =
            SnapshotFixture.await(fn ->
              state = :sys.get_state(materializer)
              if state.index_manager.current_version >= tail_version, do: state
            end)

          durable = Database.durable_version(state.database)
          assert durable <= state.known_committed_version
          assert state.known_committed_version < state.index_manager.current_version
          assert durable < tail_version

          SnapshotFixture.record(recorder, %{
            event: :applied_tail,
            durable: durable,
            kcv: state.known_committed_version,
            applied: state.index_manager.current_version
          })

          expected = SnapshotFixture.expected_prefix(recorder, durable)
          assert expected["history/counter"] == <<13::64-little>>
          assert expected["history/value"] == "baseline"
          snapshot = state.snapshot
          Gates.arm(snapshots, %{stage: stage, owner: self(), match: &String.starts_with?(&1, "s/1/")})
          assert :ok = GenServer.call(materializer, :compact)
          assert_receive {:history_gate, ^stage, uploader, upload_token, key}, 5_000

          try do
            SnapshotFixture.record(recorder, %{event: stage, key: key, uploader: uploader, materializer: materializer})
            assert uploader != materializer
            checkpoint = Path.join(root, "cold")
            File.mkdir_p!(checkpoint)
            compacted = :sys.get_state(materializer)
            {data, idx} = compacted.database
            File.cp!(to_string(data.file_name), Path.join(checkpoint, "data"))
            File.cp!(to_string(idx.file_name), Path.join(checkpoint, "idx"))
            assert {^durable, ^expected} = SnapshotFixture.cold_map(checkpoint)
            assert_publication(stage, snapshot, durable, expected, root, checkpoint)
            # The unlinked upload task must die too: a materializer crash alone
            # would leave it able to publish after the selected boundary.
            startup_trace = {__MODULE__, :startup, self()}

            :ok =
              :telemetry.attach(
                startup_trace,
                [:bedrock, :materializer, :startup_complete],
                &SnapshotFixture.startup_event/4,
                startups
              )

            Gates.arm(startups, %{stage: :cold_replacement_started, owner: self(), match: &(&1 != worker_id)})
            on_exit(fn -> :telemetry.detach(startup_trace) end)
            upload_down = Process.monitor(uploader)
            Process.exit(uploader, :kill)
            assert_receive {:DOWN, ^upload_down, :process, ^uploader, :killed}
            worker_down = Process.monitor(materializer)
            Process.exit(materializer, :kill)
            assert_receive {:DOWN, ^worker_down, :process, ^materializer, :killed}

            SnapshotFixture.await(fn ->
              workers = :sys.get_state(cluster.otp_name(:foreman)).workers

              case Map.get(workers, worker_id) do
                %{health: {:ok, restarted}} when is_pid(restarted) and restarted != materializer -> restarted
                _ -> nil
              end
            end)

            # Reclaim any automatic restart of this disposable cache and force
            # the distributor to create a worker with no local data/idx files.
            assert :ok = Foreman.remove_worker(cluster.otp_name(:foreman), worker_id)
            send(log, {:release_history_gate, wal_token})
            assert_receive {:history_gate, :cold_replacement_started, replacement, startup_token, new_id}, 10_000

            try do
              SnapshotFixture.record(recorder, %{event: :cold_replacement_started, id: new_id})
              path = Path.join(Path.dirname(state.path), new_id)
              assert_cold_replacement(stage, path, durable, expected)
            after
              send(replacement, {:release_history_gate, startup_token})
            end

            artifact =
              SnapshotFixture.assert_final(repo, recorder, Atom.to_string(stage), %{
                seed: 239,
                durable: durable,
                kcv: state.known_committed_version,
                applied: state.index_manager.current_version,
                snapshot_key: key,
                stage: stage
              })

            assert File.exists?(artifact)
          after
            send(uploader, {:release_history_gate, upload_token})
          end
        after
          send(log, {:release_history_gate, wal_token})
        end
      after
        :telemetry.detach(gate_trace)
        Enum.each([snapshots, startups, wal], &Gates.disarm/1)
      end
    end
  end

  defp assert_cold_replacement(:before_snapshot_publication, path, _durable, _expected) do
    {version, values} = SnapshotFixture.cold_map(path)
    assert version == Version.zero()
    assert values == %{}
  end

  defp assert_cold_replacement(:after_snapshot_publication, path, durable, expected) do
    assert {^durable, ^expected} = SnapshotFixture.cold_map(path)
  end

  defp assert_publication(:before_snapshot_publication, snapshot, _durable, _expected, _root, _checkpoint) do
    assert {:error, :not_found} = Snapshot.read_latest(snapshot)
  end

  defp assert_publication(:after_snapshot_publication, snapshot, durable, expected, root, checkpoint) do
    assert {:ok, version, bytes} = Snapshot.read_latest(snapshot)
    assert version == Version.to_integer(durable)
    bundle = Path.join(root, "snapshot.bundle")
    File.write!(bundle, bytes)
    assert {:ok, _, _} = SnapshotBundle.split(bundle, Path.join(checkpoint, "data"), Path.join(checkpoint, "idx"))
    assert {^durable, ^expected} = SnapshotFixture.cold_map(checkpoint)
  end
end
