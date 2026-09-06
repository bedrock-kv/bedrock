defmodule Bedrock.DataPlane.Log.Shale.ReplayCursorIntegrationTest do
  use ExUnit.Case, async: false

  alias Bedrock.DataPlane.Demux.Server, as: Demux
  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.Log.Shale.Segment
  alias Bedrock.DataPlane.Log.Shale.Server, as: Shale
  alias Bedrock.DataPlane.Version
  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.LocalFilesystem
  alias Bedrock.Service.RecoveryControl
  alias Bedrock.Test.DataPlane.TransactionTestSupport
  alias Bedrock.Test.RecoveryAuthorityTestSupport

  @moduletag :tmp_dir
  @authority %{generation: 1, recovery_id: "replay-cursor-test"}

  defmodule TestCluster do
    @moduledoc false
    def name, do: "replay-cursor-test-cluster"
  end

  test "a legacy BED0 source rolls forward to BED1 and replays its first retained transaction", %{tmp_dir: tmp_dir} do
    backend = object_storage(tmp_dir)
    source_path = Path.join(tmp_dir, "legacy-source")
    destination_path = Path.join(tmp_dir, "legacy-destination")
    first_version = Version.from_integer(41)
    legacy_last_version = Version.from_integer(500)
    last_inclusive = Version.from_integer(900)
    first = transaction(first_version, "legacy-first")
    legacy_last = transaction(legacy_last_version, "legacy-last")
    current = transaction(last_inclusive, "current")

    write_legacy_wal(source_path, [{first_version, first}, {legacy_last_version, legacy_last}])
    {source, source_id} = start_restartable_log("legacy-source", source_path, backend, true)
    assert :ok = Log.push(source, @authority, current, legacy_last_version, [])
    assert :ok = stop_supervised({Shale, source_id})

    {source, ^source_id} =
      start_restartable_log("legacy-source-restart", source_path, backend, false, source_id)

    destination = start_log("legacy-destination", destination_path, backend, false)
    expected_cursor = Version.from_integer(40)

    assert {:ok,
            %{
              available_after: ^expected_cursor,
              oldest_version: ^first_version,
              last_version: ^last_inclusive
            }} = Log.info(source, [:available_after, :oldest_version, :last_version])

    assert {:ok, ^destination} =
             recover(destination, [source], expected_cursor, last_inclusive)

    assert {:ok, [^first, ^legacy_last, ^current]} =
             Log.pull(destination, expected_cursor, last_version: last_inclusive)
  end

  test "untrimmed recovery copies widely gapped transactions exactly once", %{tmp_dir: tmp_dir} do
    backend = object_storage(tmp_dir)
    source = start_log("gapped-source", Path.join(tmp_dir, "gapped-source"), backend, true)
    destination = start_log("gapped-destination", Path.join(tmp_dir, "gapped-destination"), backend, false)

    first_version = Version.from_integer(100)
    last_inclusive = Version.from_integer(10_000)
    first = transaction(first_version, "first")
    last = transaction(last_inclusive, "last")

    assert :ok = Log.push(source, @authority, first, Version.zero(), [])
    assert :ok = Log.push(source, @authority, last, first_version, [])
    lock(source)

    assert {:ok, %{available_after: replay_after}} = Log.info(source, [:available_after])
    assert replay_after == Version.zero()
    assert {:ok, ^destination} = recover(destination, [source], replay_after, last_inclusive)
    assert {:ok, [^first, ^last]} = Log.pull(destination, replay_after, last_version: last_inclusive)
  end

  test "a single retained transaction is data, not the lower cursor", %{tmp_dir: tmp_dir} do
    backend = object_storage(tmp_dir)
    source = start_log("single-source", Path.join(tmp_dir, "single-source"), backend, true)
    destination = start_log("single-destination", Path.join(tmp_dir, "single-destination"), backend, false)

    last_inclusive = Version.from_integer(700)
    only_transaction = transaction(last_inclusive, "only")

    assert :ok = Log.push(source, @authority, only_transaction, Version.zero(), [])
    lock(source)

    assert {:ok, ^destination} =
             recover(destination, [source], Version.zero(), last_inclusive)

    assert {:ok, [^only_transaction]} =
             Log.pull(destination, Version.zero(), last_version: last_inclusive)
  end

  test "physical trim and cold restart preserve the predecessor of retained data", %{tmp_dir: tmp_dir} do
    backend = object_storage(tmp_dir)
    source_path = Path.join(tmp_dir, "trimmed-source")
    destination_path = Path.join(tmp_dir, "trimmed-destination")
    {source, source_id} = start_restartable_log("trimmed-source", source_path, backend, true)
    destination = start_log("trimmed-destination", destination_path, backend, false)

    first_version = Version.from_integer(1_000)
    last_inclusive = Version.from_integer(Demux.default_cut_interval_us())
    cut_version = Version.from_integer(Demux.default_cut_interval_us() - 1)
    first = transaction(first_version, "trimmed-away")
    retained = transaction(last_inclusive, "retained")

    assert :ok = Log.push(source, @authority, first, Version.zero(), known_committed_version: first_version)
    assert :ok = Log.push(source, @authority, retained, first_version, known_committed_version: last_inclusive)

    eventually(fn ->
      assert %{min_durable_version: ^cut_version, segments: [], available_after: ^first_version} =
               :sys.get_state(source)
    end)

    assert :ok = stop_supervised({Shale, source_id})

    {restarted_source, ^source_id} =
      start_restartable_log("trimmed-source-restart", source_path, backend, false, source_id)

    assert {:ok,
            %{
              available_after: ^first_version,
              oldest_version: ^last_inclusive,
              last_version: ^last_inclusive
            }} = Log.info(restarted_source, [:available_after, :oldest_version, :last_version])

    assert {:ok, ^destination} =
             recover(destination, [restarted_source], first_version, last_inclusive)

    assert {:ok, [^retained]} =
             Log.pull(destination, first_version, last_version: last_inclusive)
  end

  test "an empty replay baseline survives restart and anchors the next push", %{tmp_dir: tmp_dir} do
    backend = object_storage(tmp_dir)
    wal_path = Path.join(tmp_dir, "empty-baseline")
    {log, log_id} = start_restartable_log("empty-baseline", wal_path, backend, false)
    replay_after = Version.from_integer(500)

    assert {:ok, ^log} = Log.recover_from(log, @authority, [], replay_after, replay_after)
    assert :ok = stop_supervised({Shale, log_id})

    {restarted, ^log_id} = start_restartable_log("empty-baseline-restart", wal_path, backend, true, log_id)

    assert {:ok, %{available_after: ^replay_after, last_version: ^replay_after}} =
             Log.info(restarted, [:available_after, :last_version])

    next_version = Version.from_integer(900)
    next_transaction = transaction(next_version, "next")
    assert :ok = Log.push(restarted, @authority, next_transaction, replay_after, [])
    assert {:ok, [^next_transaction]} = Log.pull(restarted, replay_after, last_version: next_version)
    assert %{active_segment: %{previous_version: ^replay_after}} = :sys.get_state(restarted)
  end

  defp transaction(version, value) do
    TransactionTestSupport.new_log_transaction(version, %{"key" => value}, shard_id: 7)
  end

  defp object_storage(tmp_dir) do
    root = Path.join(tmp_dir, "objects")
    File.mkdir_p!(root)
    ObjectStorage.backend(LocalFilesystem, root: root)
  end

  defp write_legacy_wal(path, [{first_version, _} | _] = transactions) do
    File.mkdir_p!(path)
    wal_path = Path.join(path, Segment.encode_file_name(Version.to_integer(first_version)))

    entries =
      Enum.map(transactions, fn {version, transaction} ->
        <<version::binary, byte_size(transaction)::unsigned-big-32, transaction::binary,
          :erlang.crc32(transaction)::unsigned-big-32>>
      end)

    eof_marker = <<0xFFFFFFFFFFFFFFFF::unsigned-big-64, 0::unsigned-big-32, 0::unsigned-big-32>>
    File.write!(wal_path, ["BED0", entries, eof_marker])
  end

  defp start_log(label, wal_path, object_storage, start_unlocked) do
    {pid, _id} = start_restartable_log(label, wal_path, object_storage, start_unlocked)
    pid
  end

  defp start_restartable_log(label, wal_path, object_storage, start_unlocked, id \\ nil) do
    File.mkdir_p!(wal_path)
    suffix = :erlang.unique_integer([:positive])
    id = id || "replay-cursor-#{label}-#{suffix}"

    if !File.exists?(Path.join(wal_path, "manifest.json")) do
      RecoveryAuthorityTestSupport.prepare_worker!(wal_path, id, Bedrock.DataPlane.Log.Shale, cluster: TestCluster)
    end

    spec =
      [
        cluster: TestCluster,
        otp_name: String.to_atom("replay_cursor_#{label}_#{suffix}"),
        id: id,
        foreman: self(),
        path: wal_path,
        object_storage: object_storage
      ]
      |> Shale.child_spec()
      |> Supervisor.child_spec(restart: :temporary)

    pid = start_supervised!(spec)

    eventually(fn ->
      assert %{init_state: :initialized, demux: demux, segment_recycler: recycler} = :sys.get_state(pid)
      assert is_pid(demux)
      assert is_pid(recycler)
    end)

    lock(pid)
    if start_unlocked, do: activate_log(pid, wal_path)

    {pid, id}
  end

  defp lock(log) do
    assert {:ok, ^log, _} = Log.lock_for_recovery(log, @authority)
  end

  defp recover(destination, sources, replay_after, last_inclusive) do
    assert {:ok, ^destination} =
             Log.recover_from(destination, @authority, sources, replay_after, last_inclusive)

    assert :ok = Log.unlock_after_recovery(destination, @authority)
    {:ok, destination}
  end

  defp activate_log(log, path) do
    state = :sys.get_state(log)

    if state.last_version == Version.zero() do
      assert {:ok, ^log} = Log.recover_from(log, @authority, [], Version.zero(), Version.zero())
    else
      started =
        RecoveryControl.replay_started(
          state.recovery_control,
          @authority,
          state.available_after,
          state.last_version
        )

      {:ok, identity} = RecoveryControl.wal_identity(path, state.last_version)
      complete = RecoveryControl.replay_complete(started, identity)
      :ok = RecoveryControl.write(path, complete)
      :sys.replace_state(log, &%{&1 | recovery_control: complete})
    end

    assert :ok = Log.unlock_after_recovery(log, @authority)
  end

  defp eventually(assertion, timeout \\ 2_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    eventually(assertion, deadline, nil)
  end

  defp eventually(assertion, deadline, last_error) do
    assertion.()
  rescue
    error in [ExUnit.AssertionError] ->
      if System.monotonic_time(:millisecond) < deadline do
        Process.sleep(10)
        eventually(assertion, deadline, error)
      else
        reraise(last_error || error, __STACKTRACE__)
      end
  end
end
