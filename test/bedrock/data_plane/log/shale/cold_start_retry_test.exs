defmodule Bedrock.DataPlane.Log.Shale.ColdStartRetryTest do
  @moduledoc """
  Cold start is transactional and preserves I/O causes (bedrock-qzr.27).

  Transient resource failures (`:emfile`, `:enfile`, `:enomem`) retry with
  bounded backoff; format errors and non-retryable I/O failures fail closed
  with their real cause. A failed attempt owns its resources: nothing it
  started survives it, and only one recycler and one demux are published
  after success.

  These tests drive the Server callbacks in the test process, so anything
  an attempt links is visible in this process's link set.
  """
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Bedrock.DataPlane.Log.Shale.ColdStarting
  alias Bedrock.DataPlane.Log.Shale.Segment
  alias Bedrock.DataPlane.Log.Shale.Server
  alias Bedrock.DataPlane.Log.Shale.State
  alias Bedrock.DataPlane.Log.Shale.Writer
  alias Bedrock.DataPlane.Version
  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.LocalFilesystem

  # ExUnit owns the directory lifecycle (fresh per test, wiped before the
  # test runs), so no teardown can race a recycler a successful attempt
  # left running.
  @moduletag :tmp_dir

  setup %{tmp_dir: dir} do
    storage_dir = Path.join(dir, "object_storage")
    File.mkdir_p!(storage_dir)
    %{dir: dir, backend: ObjectStorage.backend(LocalFilesystem, root: storage_dir)}
  end

  defp initial_state(dir, backend, segment_loader) do
    %State{
      path: dir,
      cluster: "cold-start-test-cluster",
      mode: :locked,
      init_state: {:retrying, 1},
      id: "cold-start-log",
      otp_name: :cold_start_test_log,
      foreman: self(),
      object_storage: backend,
      segment_loader: segment_loader,
      available_after: Version.zero(),
      oldest_version: Version.zero(),
      last_version: Version.zero()
    }
  end

  defp links, do: self() |> Process.info(:links) |> elem(1) |> MapSet.new()

  defp write_wal_segment(dir, version, previous_version) do
    path = Path.join(dir, Segment.encode_file_name(version))
    File.write!(path, :binary.copy(<<0>>, 4096))
    {:ok, writer} = Writer.open(path, Version.from_integer(previous_version))
    :ok = Writer.close(writer)
    path
  end

  test "a transient header-read resource error retries, then initializes; failed attempts leak nothing",
       %{dir: dir, backend: backend} do
    wal_path = write_wal_segment(dir, 1_000, 0)
    original_bytes = File.read!(wal_path)

    {:ok, fault} = Agent.start_link(fn -> :emfile end)

    loader = fn path ->
      case Agent.get(fault, & &1) do
        nil -> ColdStarting.reload_segments_at_path(path)
        posix -> {:error, {:wal_io, wal_path, posix}}
      end
    end

    state = initial_state(dir, backend, loader)
    links_before = links()

    # First attempt: the transient fault sends the server into backoff…
    {{:noreply, retrying_state}, log} =
      with_log(fn -> Server.handle_continue(:initialization, state) end)

    assert retrying_state.init_state == {:retrying, 2}
    assert log =~ "resource exhaustion"

    # …owning its resources: the failed attempt left nothing linked.
    assert links() == links_before

    # The bounded backoff schedules a real retry message (1s first step).
    assert_receive :retry_initialization, 3_000

    # Fault clears; the retry initializes for real.
    Agent.update(fault, fn _ -> nil end)

    {:noreply, ready_state} = Server.handle_info(:retry_initialization, retrying_state)

    assert ready_state.init_state == :initialized
    assert is_pid(ready_state.segment_recycler)
    assert is_pid(ready_state.demux)
    assert ready_state.last_version == Version.from_integer(0)
    assert [%Segment{}] = [ready_state.active_segment]

    # Exactly one recycler and one demux were published — and they are the
    # only processes the whole retry saga added.
    new_links = MapSet.difference(links(), links_before)
    assert MapSet.member?(new_links, ready_state.segment_recycler)
    assert MapSet.member?(new_links, ready_state.demux)
    assert MapSet.size(new_links) == 2

    # No retry path mutated the existing WAL segment.
    assert File.read!(wal_path) == original_bytes
  end

  test "a non-retryable I/O failure fails closed as WAL I/O, not format corruption",
       %{dir: dir, backend: backend} do
    loader = fn _path -> {:error, {:wal_io, Path.join(dir, "wal_x"), :eacces}} end
    state = initial_state(dir, backend, loader)
    links_before = links()

    assert_raise RuntimeError, ~r/WAL I\/O failure.*eacces/s, fn ->
      Server.handle_continue(:initialization, state)
    end

    assert links() == links_before
  end

  test "format errors fail closed and are never retried as resource exhaustion",
       %{dir: dir, backend: backend} do
    loader = fn _path -> {:error, {:wal_format, Path.join(dir, "wal_x"), :unsupported_wal_format}} end
    state = initial_state(dir, backend, loader)

    assert_raise RuntimeError, ~r/replay cursor.*unsupported_wal_format/s, fn ->
      Server.handle_continue(:initialization, state)
    end

    refute_receive :retry_initialization, 50
  end
end
