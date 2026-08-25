defmodule Bedrock.DataPlane.Log.Shale.SegmentRecyclerServerTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Log.Shale.SegmentRecycler.Logic
  alias Bedrock.DataPlane.Log.Shale.SegmentRecycler.Server

  # The GenServer clauses around the pool, tested as pure callbacks: both
  # defects here are in how a failure is reported, and both are invisible
  # to a test that only exercises the happy path.
  @moduletag :tmp_dir

  @segment_size 1024

  defp state(dir, opts \\ []) do
    {:ok, state} = Logic.new(dir, @segment_size, 1, 4)
    struct!(state, opts)
  end

  describe "handle_continue(:ensure_min_available, _)" do
    # Replies.stop/2 is stop(state, reason) :: {:stop, reason, state}.
    # Called as stop(reason, :shutdown) the recycler exits with the
    # reason ':shutdown' — which reads as an orderly stop — and installs
    # the real cause as its own state, discarding it. Shale has a
    # classify_resource_error/1 precisely to tell :enospc / :emfile /
    # :enomem apart; that distinction was being thrown away here.
    test "stops with the real allocation failure, not with :shutdown", %{tmp_dir: dir} do
      state = state(dir, segments: [], min_available: 1)
      on_exit(fn -> File.chmod(dir, 0o755) end)
      :ok = File.chmod(dir, 0o500)

      assert {:stop, reason, returned_state} = Server.handle_continue(:ensure_min_available, state)

      refute reason == :shutdown,
             "the exit reason must carry the allocation failure, not mask it as an orderly stop"

      assert reason == :eacces
      assert returned_state == state, "the state must survive the stop, not be replaced by the reason"
    end

    test "carries on when the pool can be refilled", %{tmp_dir: dir} do
      assert {:noreply, refilled} = Server.handle_continue(:ensure_min_available, state(dir, segments: []))

      assert length(refilled.segments) == 1
    end
  end

  describe "handle_call({:check_out, _}, _, _)" do
    test "schedules a refill after a successful checkout", %{tmp_dir: dir} do
      {:ok, state} = Logic.ensure_min_available(state(dir), 1)

      assert {:reply, :ok, _state, {:continue, :ensure_min_available}} =
               Server.handle_call({:check_out, Path.join(dir, "wal_a")}, self(), state)
    end

    # An exhausted pool is the one moment a refill is most needed, and it
    # was the one moment nothing scheduled it. Nothing else drives
    # :ensure_min_available, so a pool that ever reached zero could not
    # refill itself — every later checkout fails and
    # Segment.allocate_from_recycler!/4 raises. Reachable once allocation
    # can fail transiently: a full disk that later clears leaves the pool
    # stuck at zero even after space returns.
    test "schedules a refill after an exhausted checkout too", %{tmp_dir: dir} do
      state = state(dir, segments: [])

      assert {:reply, {:error, :unavailable}, _state, {:continue, :ensure_min_available}} =
               Server.handle_call({:check_out, Path.join(dir, "wal_a")}, self(), state)
    end
  end
end
