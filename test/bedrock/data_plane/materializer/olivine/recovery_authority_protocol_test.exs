defmodule Bedrock.DataPlane.Materializer.Olivine.RecoveryAuthorityProtocolTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Materializer.Olivine
  alias Bedrock.DataPlane.Materializer.Olivine.Server
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version
  alias Bedrock.Service.Manifest
  alias Bedrock.Service.RecoveryControl
  alias Bedrock.Test.RecoveryAuthorityTestSupport, as: AuthoritySupport

  setup do
    path = Path.join(System.tmp_dir!(), "olivine-authority-#{System.unique_integer([:positive])}")
    File.rm_rf!(path)
    on_exit(fn -> File.rm_rf(path) end)
    {:ok, path: path}
  end

  test "durable authority survives normal shutdown and restart", %{path: path} do
    id = "restart-worker"
    cluster = AuthoritySupport.prepare_worker!(path, id, Olivine)
    authority = AuthoritySupport.authority(7, "restart-grant")

    {:ok, pid} = start_server(path, id, cluster)
    assert {:ok, ^pid, _} = GenServer.call(pid, {:lock_for_recovery, authority})
    assert :ok = GenServer.call(pid, {:unlock_after_recovery, authority, Version.zero(), []})
    version = Version.from_integer(1)
    transaction = Transaction.encode(%{mutations: [{:set, "key", "value"}], commit_version: version})
    assert :ok = GenServer.call(pid, {:ingest, [transaction], version})
    assert {:ok, "value"} = GenServer.call(pid, {:get, "key", version, []})
    GenServer.stop(pid)

    assert {:ok, restarted} = start_server(path, id, cluster, "restart")
    assert {:ok, :materializer} = GenServer.call(restarted, {:info, :kind})
    GenServer.stop(restarted)
  end

  test "a running checkpoint follows a durable compaction cutover", %{path: path} do
    id = "compacted-worker"
    cluster = AuthoritySupport.prepare_worker!(path, id, Olivine)
    authority = AuthoritySupport.authority(9, "compaction-grant")

    {:ok, pid} = start_server(path, id, cluster)
    assert {:ok, ^pid, _} = GenServer.call(pid, {:lock_for_recovery, authority})
    assert :ok = GenServer.call(pid, {:unlock_after_recovery, authority, Version.zero(), []})
    assert :ok = GenServer.call(pid, :compact)
    wait_for_compaction(pid)
    GenServer.stop(pid)

    assert {:ok, restarted} = start_server(path, id, cluster, "after-compaction")
    GenServer.stop(restarted)
  end

  test "same grant works across task PIDs while a foreign same-generation grant fails", %{path: path} do
    id = "grant-worker"
    cluster = AuthoritySupport.prepare_worker!(path, id, Olivine)
    winner = AuthoritySupport.authority(8, "winner")
    foreign = AuthoritySupport.authority(8, "foreign")
    {:ok, pid} = start_server(path, id, cluster)

    assert {:ok, ^pid, _} = GenServer.call(pid, {:lock_for_recovery, winner})

    assert :ok =
             fn -> GenServer.call(pid, {:unlock_after_recovery, winner, Version.zero(), []}) end
             |> Task.async()
             |> Task.await()

    assert {:error, :not_lock_owner} =
             GenServer.call(pid, {:unlock_after_recovery, foreign, Version.zero(), []})

    GenServer.stop(pid)
  end

  test "same-grant duplicate unlock is exact and conflicting intent is rejected unchanged", %{path: path} do
    id = "duplicate-unlock-worker"
    cluster = AuthoritySupport.prepare_worker!(path, id, Olivine)
    authority = AuthoritySupport.authority(8, "duplicate-unlock")
    {:ok, pid} = start_server(path, id, cluster)

    assert {:ok, ^pid, _} = GenServer.call(pid, {:lock_for_recovery, authority})
    assert :ok = GenServer.call(pid, {:unlock_after_recovery, authority, Version.zero(), []})
    before = {directory_bytes(path), :sys.get_state(pid)}

    assert :ok = GenServer.call(pid, {:unlock_after_recovery, authority, Version.zero(), []})
    assert {directory_bytes(path), :sys.get_state(pid)} == before

    assert {:error, :conflicting_recovery_intent} =
             GenServer.call(pid, {:unlock_after_recovery, authority, Version.from_integer(1), []})

    assert {directory_bytes(path), :sys.get_state(pid)} == before

    assert {:error, :conflicting_recovery_intent} =
             GenServer.call(pid, {:unlock_after_recovery, authority, Version.zero(), [{"foreign-source", self()}]})

    assert {directory_bytes(path), :sys.get_state(pid)} == before
    GenServer.stop(pid)
  end

  test "running checkpoint rehydrates exactly one puller after restart", %{path: path} do
    id = "rehydrate-worker"
    cluster = AuthoritySupport.prepare_worker!(path, id, Olivine)
    authority = AuthoritySupport.authority(9, "rehydrate")
    source = spawn(fn -> Process.sleep(:infinity) end)
    sources = [{"source-log", source}]
    {:ok, pid} = start_server(path, id, cluster, "initial", shard_id: 0)

    assert {:ok, ^pid, _} = GenServer.call(pid, {:lock_for_recovery, authority})
    assert :ok = GenServer.call(pid, {:unlock_after_recovery, authority, Version.zero(), sources})
    first_puller = :sys.get_state(pid).pull_task.pid
    assert Process.alive?(first_puller)
    GenServer.stop(pid)

    assert {:ok, restarted} = start_server(path, id, cluster, "rehydrated", shard_id: 0)
    assert :sys.get_state(restarted).mode == :locked
    assert :ok = GenServer.call(restarted, {:unlock_after_recovery, authority, Version.zero(), sources})
    state = :sys.get_state(restarted)
    assert state.mode == :running
    assert %Task{pid: puller} = state.pull_task
    assert Process.alive?(puller)
    refute puller == first_puller
    GenServer.stop(restarted)
    Process.exit(source, :kill)
  end

  test "legacy lock and unlock messages fail closed", %{path: path} do
    id = "legacy-worker"
    cluster = AuthoritySupport.prepare_worker!(path, id, Olivine)
    {:ok, pid} = start_server(path, id, cluster)

    assert {:error, :invalid_recovery_authority} = GenServer.call(pid, {:lock_for_recovery, 1})

    assert {:error, :invalid_recovery_authority} =
             GenServer.call(pid, {:unlock_after_recovery, Version.zero(), []})

    GenServer.stop(pid)
  end

  test "an unmarked directory cannot enable migration through direct actor options", %{path: path} do
    id = "unmarked-worker"
    File.mkdir_p!(path)
    manifest = Manifest.new(AuthoritySupport.TestCluster.name(), id, Olivine, %{})
    :ok = Manifest.write_to_file(manifest, Path.join(path, "manifest.json"))
    before = directory_bytes(path)

    assert {:error, {:recovery_authority, :unprepared_worker_directory}} =
             GenServer.start(
               Server,
               {unique_name("unmarked"), self(), id, path,
                [cluster: AuthoritySupport.TestCluster, recovery_authority_migration: :enabled]}
             )

    assert directory_bytes(path) == before
    refute_received {"$gen_cast", {:worker_health, ^id, _}}
  end

  test "a marked manifest with a deleted authority record fails without touching data", %{path: path} do
    id = "deleted-record-worker"
    AuthoritySupport.prepare_worker!(path, id, Olivine)
    File.write!(Path.join(path, "data"), "sentinel-data")
    File.write!(Path.join(path, "idx"), "sentinel-index")
    File.rm!(RecoveryControl.path(path))
    before = directory_bytes(path)

    assert {:error, {:recovery_authority, :missing_after_migration}} =
             GenServer.start(
               Server,
               {unique_name("missing"), self(), id, path, [cluster: AuthoritySupport.TestCluster]}
             )

    assert directory_bytes(path) == before
    refute_received {"$gen_cast", {:worker_health, ^id, _}}
  end

  defp start_server(path, id, cluster, suffix \\ "initial", extra_opts \\ []) do
    name = unique_name(suffix)
    GenServer.start(Server, {name, self(), id, path, [cluster: cluster] ++ extra_opts}, name: name)
  end

  defp unique_name(suffix), do: :"olivine_authority_#{suffix}_#{System.unique_integer([:positive])}"

  defp wait_for_compaction(pid, attempts \\ 100)
  defp wait_for_compaction(_pid, 0), do: flunk("compaction did not finish")

  defp wait_for_compaction(pid, attempts) do
    if :sys.get_state(pid).compaction_task do
      Process.sleep(10)
      wait_for_compaction(pid, attempts - 1)
    else
      :ok
    end
  end

  defp directory_bytes(path) do
    path
    |> File.ls!()
    |> Enum.sort()
    |> Map.new(fn name -> {name, File.read!(Path.join(path, name))} end)
  end
end
