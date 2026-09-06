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

  # Large enough that no filesystem can satisfy it, so `:file.allocate/3`
  # fails *after* the open has already created the file — the ENOSPC
  # shape, without needing a full disk.
  @unallocatable_size Bitwise.bsl(1, 62)

  setup %{tmp_dir: dir} do
    {:ok, state} = Logic.new(dir, @segment_size, 1, @max_available)
    %{dir: dir, state: state}
  end

  # A stand-in for a retired WAL segment: check_in either renames it into
  # the pool or unlinks it, so the contents are irrelevant — but the size
  # is not, since only a whole segment is worth pooling.
  defp retired_segment(dir, name, size \\ @segment_size) do
    path = Path.join(dir, name)
    :ok = File.write(path, :binary.copy(<<0>>, size))
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

  # A log recovering over the wreckage a pre-atomic-publish version left
  # behind checks that wreckage back in: the 28-byte stub a zero-length
  # pool file becomes once Writer.open/3 has written a header loads as a
  # segment, and Recovery.discard_all_segments/1 returns it to the pool.
  # new/4 never sees it — it arrives under a `wal_` name, after the scan.
  describe "check_in/2 of something that is not a whole segment" do
    test "unlinks it instead of pooling it", %{dir: dir, state: state} do
      stub = retired_segment(dir, "wal_stub", 28)

      {:ok, state} = Logic.check_in(state, stub)

      assert state.segments == []
      refute File.exists?(stub)
      assert pooled_files(dir) == []
    end
  end

  describe "a successful allocation" do
    test "publishes a whole segment and nothing else", %{tmp_dir: dir, state: state} do
      {:ok, state} = Logic.ensure_min_available(state, 1)

      assert [pooled] = state.segments
      assert File.stat!(pooled).size == @segment_size

      assert File.ls!(dir) == [Path.basename(pooled)],
             "the scratch name must be consumed by the publish, not left alongside the pool file"
    end
  end

  # Preallocation is create-then-extend: the file exists at zero length
  # before it is a segment. A failure between those two steps must leave
  # nothing behind that a later scan can mistake for a whole segment.
  describe "an allocation that fails partway" do
    test "publishes nothing into the pool", %{tmp_dir: dir} do
      {:ok, state} = Logic.new(dir, @unallocatable_size, 1, @max_available)

      assert {:error, _reason} = Logic.ensure_min_available(state, 1)

      assert pooled_files(dir) == [],
             "a preallocation that never reached full size must not appear in the pool"
    end

    test "leaves nothing on disk at all", %{tmp_dir: dir} do
      {:ok, state} = Logic.new(dir, @unallocatable_size, 1, @max_available)

      assert {:error, _reason} = Logic.ensure_min_available(state, 1)

      assert File.ls!(dir) == [], "the scratch file must be removed when the allocation fails"
    end
  end

  # bedrock-61c.2's doctrine for worker directories applies to the pool:
  # membership must be provable, not assumed from the name. `check_out`
  # renames a pooled file straight into service and `Writer.open/3`
  # derives its write budget from `File.stat/1`, so a short file adopted
  # here presents as a healthy segment with a nonsense budget.
  describe "new/4 adoption" do
    test "adopts a pool file of the configured size", %{tmp_dir: dir} do
      whole = Path.join(dir, "preallocated.1")
      :ok = File.write(whole, :binary.copy(<<0>>, @segment_size))

      assert {:ok, %{segments: [^whole], next_id: 2}} = Logic.new(dir, @segment_size, 1, @max_available)
    end

    test "discards a zero-length pool file", %{tmp_dir: dir} do
      orphan = Path.join(dir, "preallocated.1")
      :ok = File.write(orphan, "")

      assert {:ok, %{segments: []}} = Logic.new(dir, @segment_size, 1, @max_available)

      refute File.exists?(orphan),
             "a file that is not a whole segment must be discarded, not left for the next scan to re-adopt"
    end

    test "discards a pool file that is short of the configured size", %{tmp_dir: dir} do
      short = Path.join(dir, "preallocated.2")
      :ok = File.write(short, :binary.copy(<<0>>, div(@segment_size, 2)))

      assert {:ok, %{segments: []}} = Logic.new(dir, @segment_size, 1, @max_available)
      refute File.exists?(short)
    end

    # A scratch file cannot be adopted — it does not match the pool's
    # glob — but it must not accumulate either: at 64 MiB apiece, one
    # per incarnation that dies mid-allocation.
    test "sweeps a scratch file left behind by a previous incarnation", %{tmp_dir: dir} do
      scratch = Path.join(dir, ".partial.preallocated.1")
      :ok = File.write(scratch, "")

      assert {:ok, %{segments: []}} = Logic.new(dir, @segment_size, 1, @max_available)
      refute File.exists?(scratch)
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

  # The two ends joined up: wreckage left in the pool directory by a
  # previous incarnation must never reach a WAL segment.
  describe "a fresh incarnation over a poisoned pool directory" do
    test "never checks out a file that is not a whole segment", %{tmp_dir: dir} do
      :ok = File.write(Path.join(dir, "preallocated.1"), "")

      {:ok, recycler} =
        SegmentRecycler.start_link(
          path: dir,
          min_available: 1,
          max_available: @max_available,
          segment_size: @segment_size
        )

      handed_out = Path.join(dir, "wal_next")
      assert :ok = SegmentRecycler.check_out(recycler, handed_out)

      assert File.stat!(handed_out).size == @segment_size,
             "the recycler handed out a file that is not a whole segment"
    end
  end
end
