defmodule Bedrock.DataPlane.Log.Shale.SegmentRecyclerTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Log.Shale.SegmentRecycler
  alias Bedrock.DataPlane.Log.Shale.SegmentRecycler.Logic

  # The pool is a disk-backed cache of preallocated 64 MiB files. Its
  # ceiling is the only thing standing between a trim burst and hundreds
  # of megabytes pinned for the life of the worker, so these tests assert
  # the ceiling holds against bursts, not just against the steady
  # one-in-one-out cadence a running log produces.
  @moduletag :tmp_dir

  @segment_size 1024
  @max_available 3

  setup %{tmp_dir: dir} do
    {:ok, state} = Logic.new(dir, @segment_size, 1, @max_available)
    %{dir: dir, state: state}
  end

  # A stand-in for a retired WAL segment: check_in either renames it into
  # the pool or unlinks it, so the contents are irrelevant and the size
  # need not be realistic.
  defp retired_segment(dir, name) do
    path = Path.join(dir, name)
    :ok = File.write(path, "retired")
    path
  end

  defp check_in_all(state, dir, names) do
    Enum.reduce(names, state, fn name, acc ->
      {:ok, acc} = Logic.check_in(acc, retired_segment(dir, name))
      acc
    end)
  end

  defp pooled_files(dir), do: Logic.find_existing_preallocated_files(dir)

  describe "check_in/2 below the cap" do
    test "pools the segment under a preallocated name", %{dir: dir, state: state} do
      path = retired_segment(dir, "wal_a")

      {:ok, state} = Logic.check_in(state, path)

      assert [pooled] = state.segments
      assert File.exists?(pooled)
      assert Path.basename(pooled) =~ ~r/^preallocated\./
      refute File.exists?(path), "check_in must rename the retired segment, not copy it"
    end

    test "accumulates up to max_available", %{dir: dir, state: state} do
      state = check_in_all(state, dir, ["wal_a", "wal_b", "wal_c"])

      assert length(state.segments) == @max_available
      assert length(pooled_files(dir)) == @max_available
    end
  end

  describe "check_in/2 at the cap" do
    test "deletes the returned segment instead of pooling it", %{dir: dir, state: state} do
      state = check_in_all(state, dir, ["wal_a", "wal_b", "wal_c"])
      assert length(state.segments) == @max_available

      surplus = retired_segment(dir, "wal_surplus")
      {:ok, state} = Logic.check_in(state, surplus)

      assert length(state.segments) == @max_available,
             "pool must not grow past max_available"

      refute File.exists?(surplus), "the surplus segment must be unlinked, not left on disk"
      assert length(pooled_files(dir)) == @max_available
    end

    # The ratchet only bites on a burst: trim_durable_segments/1 can split
    # off many segments at once when a durability watermark jumps, and
    # checks them all in back-to-back with no intervening checkout.
    test "a burst of consecutive check-ins never exceeds max_available", %{dir: dir, state: state} do
      surplus_count = 5
      names = Enum.map(1..(@max_available + surplus_count), &"wal_#{&1}")

      state = check_in_all(state, dir, names)

      assert length(state.segments) == @max_available
      assert length(pooled_files(dir)) == @max_available

      assert Path.wildcard(Path.join(dir, "wal_*")) == [],
             "every checked-in segment must be either pooled or unlinked"
    end
  end

  describe "new/4 configuration" do
    test "refuses a pool with no slack between min and max", %{tmp_dir: dir} do
      assert {:error, :max_available_must_exceed_min_available} = Logic.new(dir, @segment_size, 1, 1)
      assert {:error, :max_available_must_exceed_min_available} = Logic.new(dir, @segment_size, 3, 2)
    end

    test "accepts a pool with slack", %{tmp_dir: dir} do
      assert {:ok, %{min_available: 2, max_available: 3}} = Logic.new(dir, @segment_size, 2, 3)
    end
  end

  # The Logic tests above never run the min-refill, which is the other
  # half of the cycle: a live recycler must hold the ceiling while
  # ensure_min_available keeps putting segments back.
  describe "the running recycler" do
    setup %{tmp_dir: dir} do
      {:ok, recycler} =
        SegmentRecycler.start_link(
          path: dir,
          min_available: 2,
          max_available: @max_available,
          segment_size: @segment_size
        )

      %{recycler: recycler}
    end

    test "refuses to start with no slack", %{tmp_dir: dir} do
      Process.flag(:trap_exit, true)

      assert {:error, :max_available_must_exceed_min_available} =
               SegmentRecycler.start_link(path: dir, min_available: 1, max_available: 1, segment_size: @segment_size)
    end

    test "holds the ceiling across a burst and still serves checkouts", %{dir: dir, recycler: recycler} do
      for i <- 1..8, do: :ok = SegmentRecycler.check_in(recycler, retired_segment(dir, "wal_#{i}"))

      assert length(pooled_files(dir)) <= @max_available

      # The pool is still usable afterwards — deletion must never starve
      # the very checkout the pool exists to serve.
      assert :ok = SegmentRecycler.check_out(recycler, Path.join(dir, "wal_next"))
      assert File.exists?(Path.join(dir, "wal_next"))
    end
  end
end
