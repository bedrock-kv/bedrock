defmodule Bedrock.DataPlane.Materializer.Olivine.CompactionVersionTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Materializer.Olivine.Database
  alias Bedrock.DataPlane.Materializer.Olivine.Index
  alias Bedrock.DataPlane.Materializer.Olivine.IndexDatabase
  alias Bedrock.DataPlane.Materializer.Olivine.IndexManager
  alias Bedrock.DataPlane.Materializer.Olivine.Logic
  alias Bedrock.DataPlane.Materializer.Olivine.State
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version
  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.LocalFilesystem
  alias Bedrock.ObjectStorage.Snapshot

  for output <- [:compaction, :spindown] do
    @tag :tmp_dir
    test "#{output} cold replacement excludes an uncommitted tail", %{tmp_dir: path} do
      state = baseline(path, "old")
      state = apply_at(state, 200, [{:set, "a", "future"}])
      assert value(state, 200) == "future"
      restored = restore(unquote(output), state, path)

      assert restored.index_manager.current_version == Version.from_integer(100)
      assert value(restored, 100) == "old"
      assert {:ok, rolled_back} = Logic.unlock_after_recovery(state, Version.from_integer(100), [])
      assert value(rolled_back, 100) == "old"
      assert rolled_back.index_manager.current_version == restored.index_manager.current_version
      Database.close(restored.database)
      Database.close(state.database)
    end

    @tag :tmp_dir
    test "#{output} keeps an empty durable baseline empty", %{tmp_dir: path} do
      live = Path.join(path, "live")
      File.mkdir_p!(live)
      {:ok, database} = Database.open(unique_name(), Path.join(live, "db"), pool_size: 1)
      state = %State{database: database, index_manager: IndexManager.new(), known_committed_version: Version.zero()}
      state = apply_at(state, 200, [{:set, "a", "future"}])
      restored = restore(unquote(output), state, path)
      assert restored.index_manager.current_version == Version.zero()
      assert File.read!(Path.join([path, "replacement", "data"])) == ""
      [{_, {index, _}}] = restored.index_manager.versions
      assert Enum.all?(index.page_map, fn {_, {page, _}} -> Index.Page.key_count(page) == 0 end)
      Database.close(restored.database)
      Database.close(state.database)
    end

    @tag :tmp_dir
    test "#{output} committed atomic suffix is replayed exactly once", %{tmp_dir: path} do
      state = baseline(path, <<10>>)
      mutation = [{:atomic, :add, "a", <<5>>}]
      state = apply_at(state, 200, mutation)
      state = %{state | known_committed_version: Version.from_integer(200)}
      assert value(state, 200) == <<15>>
      restored = restore(unquote(output), state, path)
      assert restored.index_manager.current_version == Version.from_integer(100)

      replayed = apply_at(restored, 200, mutation)
      assert value(replayed, 200) == <<15>>
      assert value(replayed, 100) == <<10>>
      Database.close(replayed.database)
      Database.close(state.database)
    end
  end

  @tag :tmp_dir
  test "async upload captures its immutable baseline before returning to ingestion", %{tmp_dir: path} do
    state = baseline(path, "old")
    backend = ObjectStorage.backend(LocalFilesystem, root: Path.join(path, "objects"))
    snapshot = Snapshot.new(backend, "1")
    data_path = Path.join(path, "upload-data")
    idx_path = Path.join(path, "upload-idx")
    version = Version.from_integer(100)

    File.write!(
      idx_path,
      IndexDatabase.build_snapshot_record(version, IndexManager.get_complete_page_map(state.index_manager))
    )

    # A Unix FIFO holds the first file capture open until explicitly released.
    # Its writer-open handshake proves the reader entered capture; this does
    # not depend on winning a race with an asynchronously scheduled task.
    assert {_, 0} = System.cmd("mkfifo", [data_path])
    parent = self()

    producer =
      spawn(fn ->
        {:ok, file} = :file.open(String.to_charlist(data_path), [:raw, :write, :binary])
        send(parent, :capture_started)

        receive do
          :release -> :file.write(file, "oldkeepkeep")
        end

        :file.close(file)
      end)

    caller =
      spawn(fn ->
        result = Logic.maybe_upload_snapshot(%{state | snapshot: snapshot}, data_path, idx_path, version)
        send(parent, {:upload_returned, result})
      end)

    on_exit(fn ->
      Process.exit(caller, :kill)
      Process.exit(producer, :kill)
    end)

    assert_receive :capture_started, 5_000
    refute_receive {:upload_returned, _}
    send(producer, :release)
    assert_receive {:upload_returned, :ok}, 5_000

    # The server can now advance or replace its live files. The uploaded
    # bundle must still describe the version captured before this return.
    File.rm!(data_path)
    File.write!(data_path, "future")
    File.write!(idx_path, "new live index")
    assert {:ok, 100, _} = await_snapshot(snapshot)
    replacement = Path.join(path, "replacement")
    File.mkdir_p!(replacement)
    assert :ok = Logic.maybe_load_snapshot(replacement, snapshot)
    restored = open_replacement(replacement)
    assert restored.index_manager.current_version == version
    assert value(restored, 100) == "old"
    Database.close(restored.database)
    Database.close(state.database)
  end

  defp await_snapshot(snapshot, attempts \\ 100)
  defp await_snapshot(_snapshot, 0), do: {:error, :timed_out}

  defp await_snapshot(snapshot, attempts) do
    case Snapshot.read_latest(snapshot) do
      {:error, :not_found} ->
        Process.sleep(10)
        await_snapshot(snapshot, attempts - 1)

      result ->
        result
    end
  end

  @tag :tmp_dir
  test "repeated window advancement retains the exact durable index", %{tmp_dir: path} do
    state = baseline(path, "old")
    state = %{state | index_manager: %{state.index_manager | window_lag_time_μs: 0}}
    {:ok, state} = Logic.advance_window(state)
    assert Enum.map(state.index_manager.versions, &elem(&1, 0)) == [Version.from_integer(100)]
    state = apply_at(state, 200, [{:set, "a", "committed"}])
    state = %{state | known_committed_version: Version.from_integer(200)}
    {:ok, state} = Logic.advance_window(state)
    assert Database.durable_version(state.database) == Version.from_integer(200)
    assert Enum.map(state.index_manager.versions, &elem(&1, 0)) == [Version.from_integer(200)]

    assert {:error, :version_not_retained} =
             IndexManager.get_complete_page_map(state.index_manager, Version.from_integer(150))

    state = apply_at(state, 300, [{:set, "a", "future"}])
    {:ok, state} = Logic.advance_window(state)
    assert Database.durable_version(state.database) == Version.from_integer(200)
    restored = restore(:compaction, state, path)
    assert value(restored, 200) == "committed"
    Database.close(restored.database)
    Database.close(state.database)
  end

  @tag :tmp_dir
  test "missing exact durable baseline produces neither compacted index nor uploaded snapshot", %{tmp_dir: path} do
    state = baseline(path, "old")
    state = apply_at(state, 200, [{:set, "a", "future"}])

    assert {:error, :version_not_retained} =
             IndexManager.get_complete_page_map(state.index_manager, Version.from_integer(150))

    versions = Enum.reject(state.index_manager.versions, fn {v, _} -> v == Version.from_integer(100) end)
    state = %{state | index_manager: %{state.index_manager | versions: versions}}

    assert {:error, :version_not_retained} =
             IndexManager.get_complete_page_map(state.index_manager, Version.from_integer(100))

    {:ok, task} = Logic.start_compaction(state)
    assert_receive {:compaction_failed, :version_not_retained}, 5_000
    Task.await(task)
    refute_receive {:compaction_ready, _, _, _, _, _, _, _, _, _, _, _}
    backend = ObjectStorage.backend(LocalFilesystem, root: Path.join(path, "objects"))
    snapshot = Snapshot.new(backend, "1")
    assert {:error, :version_not_retained} = Logic.upload_snapshot_before_spindown(%{state | snapshot: snapshot})
    assert {:error, :not_found} = Snapshot.read_latest(snapshot)
    refute File.exists?(Path.join([path, "live", "data.spindown"]))
    refute File.exists?(Path.join([path, "live", "idx.spindown"]))
    Database.close(state.database)
  end

  defp baseline(path, value) do
    live = Path.join(path, "live")
    File.mkdir_p!(live)
    {:ok, database} = Database.open(unique_name(), Path.join(live, "db"), pool_size: 1)

    state = %State{
      database: database,
      index_manager: IndexManager.new(),
      known_committed_version: Version.from_integer(100)
    }

    state = apply_at(state, 100, [{:set, "a", value}, {:set, "b", "keep"}, {:set, "c", "keep"}])
    state = %{state | index_manager: %{state.index_manager | window_lag_time_μs: 0}}
    {:ok, state} = Logic.advance_window(state)
    state
  end

  defp apply_at(state, version, mutations) do
    txn = Transaction.encode(%{mutations: mutations, commit_version: Version.from_integer(version)})
    {:ok, state, _} = Logic.apply_transactions(state, [txn])
    state
  end

  defp value(state, version) do
    {:ok, page} = IndexManager.page_for_key(state.index_manager, "a", Version.from_integer(version))
    {:ok, locator} = Index.Page.locator_for_key(page, "a")
    {:ok, value} = Database.load_value(state.database, locator)
    value
  end

  defp restore(:compaction, state, path) do
    {:ok, task} = Logic.start_compaction(state)
    assert_receive {:compaction_ready, _, _, dp, ip, _, _, _, advertised, _, _, _}, 5_000
    Task.await(task)
    assert advertised <= state.known_committed_version
    recovered = Path.join(path, "replacement")
    File.mkdir_p!(recovered)
    File.cp!(to_string(dp), Path.join(recovered, "data"))
    File.cp!(to_string(ip), Path.join(recovered, "idx"))
    open_replacement(recovered)
  end

  defp restore(:spindown, state, path) do
    backend = ObjectStorage.backend(LocalFilesystem, root: Path.join(path, "objects"))
    snapshot = Snapshot.new(backend, "1")
    assert :ok = Logic.upload_snapshot_before_spindown(%{state | snapshot: snapshot})
    assert {:ok, advertised, _} = Snapshot.read_latest(snapshot)
    assert advertised <= Version.to_integer(state.known_committed_version)
    recovered = Path.join(path, "replacement")
    File.mkdir_p!(recovered)
    assert :ok = Logic.maybe_load_snapshot(recovered, snapshot)
    open_replacement(recovered)
  end

  defp open_replacement(path) do
    {:ok, database} = Database.open(unique_name(), Path.join(path, "db"), pool_size: 1)
    {:ok, index_manager} = IndexManager.recover_from_database(database)
    %State{database: database, index_manager: index_manager}
  end

  defp unique_name, do: :"compaction_version_#{System.unique_integer([:positive])}"
end
