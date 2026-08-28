defmodule Bedrock.DataPlane.Log.Shale.PushingTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Demux
  alias Bedrock.DataPlane.Log.Shale.ColdStarting
  alias Bedrock.DataPlane.Log.Shale.Pushing
  alias Bedrock.DataPlane.Log.Shale.Segment
  alias Bedrock.DataPlane.Log.Shale.SegmentRecycler
  alias Bedrock.DataPlane.Log.Shale.State
  alias Bedrock.DataPlane.Log.Shale.Writer
  alias Bedrock.DataPlane.Version
  alias Bedrock.Test.DataPlane.TransactionTestSupport

  # Every push returns the same transition shape; these tests assert the
  # effects as data (replies, append events, parking) rather than
  # observing callbacks — Pushing performs no effects itself.
  #
  # ExUnit owns the directory lifecycle: each test gets a fresh tmp_dir,
  # wiped before the test runs — so no teardown can race the linked
  # recycler still creating preallocated files.
  @moduletag :tmp_dir

  defp recycler_in_tmp_dir(%{tmp_dir: dir}) do
    {:ok, recycler} =
      SegmentRecycler.start_link(
        path: dir,
        min_available: 2,
        max_available: 4,
        segment_size: 1_000_000
      )

    %{dir: dir, recycler: recycler}
  end

  describe "push/4 rejections" do
    test "rejects while locked" do
      state = %State{mode: :locked, last_version: Version.zero()}

      assert %{state: ^state, appended: [], replies: [{:tok, {:error, :not_ready}}], parked?: false} =
               Pushing.push(state, Version.zero(), <<1, 2, 3>>, :tok)
    end

    test "rejects transaction that is too large" do
      state = %State{
        mode: :running,
        last_version: Version.from_integer(0)
      }

      # Create a transaction larger than 10MB limit
      large_transaction = :binary.copy(<<0>>, 10_000_001)

      assert %{state: ^state, appended: [], replies: [{:tok, {:error, :tx_too_large}}], parked?: false} =
               Pushing.push(state, Version.from_integer(1), large_transaction, :tok)
    end

    test "rejects out of order transaction with version less than last_version" do
      state = %State{
        mode: :running,
        last_version: Version.from_integer(5),
        pending_pushes: %{}
      }

      transaction = TransactionTestSupport.new_log_transaction(3, %{"a" => "1"})

      # Trying to push version 3 when last_version is 5
      assert %{state: ^state, appended: [], replies: [{:tok, {:error, :tx_out_of_order}}], parked?: false} =
               Pushing.push(state, Version.from_integer(3), transaction, :tok)
    end

    test "parks a future transaction version with its token, unreplied" do
      state = %State{
        mode: :running,
        last_version: Version.from_integer(5),
        pending_pushes: %{}
      }

      transaction = TransactionTestSupport.new_log_transaction(10, %{"a" => "1"})

      assert %{state: parked_state, appended: [], replies: [], parked?: true} =
               Pushing.push(state, Version.from_integer(10), transaction, :tok)

      assert {^transaction, :tok} = Map.fetch!(parked_state.pending_pushes, Version.from_integer(10))
    end

    test "a sync failure is an error reply with the caller's state intact" do
      path = Path.join(System.tmp_dir!(), "shale_push_sync_fail_#{System.unique_integer([:positive])}.log")
      File.write!(path, :binary.copy(<<0>>, 1024))

      on_exit(fn ->
        File.rm(path)
      end)

      assert {:ok, writer} = Writer.open(path, Version.zero(), sync_fun: fn _fd -> {:error, :eio} end)

      state = %State{
        mode: :running,
        last_version: Version.from_integer(0),
        pending_pushes: %{},
        writer: writer,
        active_segment: %Segment{path: path, min_version: Version.zero(), transactions: []}
      }

      transaction = TransactionTestSupport.new_log_transaction(0, %{"a" => "1"})

      assert %{state: ^state, appended: [], replies: [{:tok, {:error, :eio}}], parked?: false} =
               Pushing.push(state, Version.from_integer(0), transaction, :tok)

      assert :ok = Writer.close(writer)
    end
  end

  describe "draining queued pushes" do
    setup :recycler_in_tmp_dir

    test "a drain returns every append event and reply in predecessor-chain order", %{dir: dir, recycler: recycler} do
      state = %State{
        mode: :running,
        path: dir,
        segment_recycler: recycler,
        writer: nil,
        active_segment: nil,
        segments: [],
        last_version: Version.zero(),
        pending_pushes: %{}
      }

      v1 = Version.from_integer(1)
      v2 = Version.from_integer(2)
      first = TransactionTestSupport.new_log_transaction(1, %{"first" => "1"})
      second = TransactionTestSupport.new_log_transaction(2, %{"second" => "2"})

      assert %{state: state, parked?: true, replies: []} =
               Pushing.push(state, v1, second, :second_token)

      assert %{
               state: state,
               appended: [{^v1, ^first}, {^v2, ^second}],
               replies: [{:first_token, :ok}, {:second_token, :ok}],
               parked?: false
             } = Pushing.push(state, Version.zero(), first, :first_token)

      assert state.last_version == v2
      assert state.pending_pushes == %{}
    end

    test "a partial drain keeps the admitted prefix and reports the failure", ctx do
      path = Path.join(System.tmp_dir!(), "pushing_partial_#{System.unique_integer([:positive])}.log")
      File.write!(path, :binary.copy(<<0>>, 1_000_000))
      on_exit(fn -> File.rm(path) end)

      _ = ctx

      {:ok, sync_count} = Agent.start_link(fn -> 0 end)

      sync_fun = fn _fd ->
        Agent.get_and_update(sync_count, fn
          0 -> {:ok, 1}
          count -> {{:error, :eio}, count + 1}
        end)
      end

      assert {:ok, writer} = Writer.open(path, Version.zero(), sync_fun: sync_fun)

      state = %State{
        mode: :running,
        last_version: Version.zero(),
        pending_pushes: %{},
        writer: writer,
        active_segment: %Segment{path: path, min_version: Version.zero(), transactions: []}
      }

      v1 = Version.from_integer(1)
      first = TransactionTestSupport.new_log_transaction(1, %{"first" => "1"})
      second = TransactionTestSupport.new_log_transaction(2, %{"second" => "2"})

      assert %{state: state, parked?: true} = Pushing.push(state, v1, second, :second_token)

      assert %{
               state: state,
               appended: [{^v1, ^first}],
               replies: [{:first_token, :ok}, {:second_token, {:error, :eio}}],
               parked?: false
             } = Pushing.push(state, Version.zero(), first, :first_token)

      assert state.last_version == v1
      assert state.pending_pushes == %{}
      assert :ok = Writer.close(state.writer)
    end
  end

  describe "WAL safety limit bounds every prospective commit version" do
    setup :recycler_in_tmp_dir

    defp bounded_state(dir, recycler, limit, floor_int) do
      %State{
        mode: :running,
        path: dir,
        segment_recycler: recycler,
        writer: nil,
        active_segment: nil,
        segments: [],
        last_version: Version.from_integer(1_000),
        available_after: Version.from_integer(1_000),
        oldest_version: Version.from_integer(1_000),
        reject_pushes_above_lag_us: limit,
        min_durable_version: floor_int && Version.from_integer(floor_int),
        pending_pushes: %{}
      }
    end

    defp bp_tx(version_int), do: TransactionTestSupport.new_log_transaction(version_int, %{"k" => "v"})

    defp assert_wal_limit_error(reason, expected) do
      assert {:recovery_required, {:wal_limit_exceeded, details}} = reason

      Enum.each(expected, fn {key, value} ->
        assert Map.fetch!(details, key) == value
      end)

      reason
    end

    test "a direct push beyond the bound trips the recovery-required fuse before append",
         %{dir: dir, recycler: recycler} do
      state = bounded_state(dir, recycler, 1_000, 1_000)

      assert %{state: after_state, appended: [], replies: [{:tok, {:error, reason}}], parked?: false} =
               Pushing.push(state, Version.from_integer(1_000), bp_tx(2_001), :tok)

      assert_wal_limit_error(reason,
        commit_version: Version.from_integer(2_001),
        min_durable_version: Version.from_integer(1_000),
        last_version: Version.from_integer(1_000),
        lag_us: 1_001,
        limit_us: 1_000
      )

      assert after_state.last_version == Version.from_integer(1_000)
    end

    test "a future push beyond the bound signals recovery instead of entering the queue",
         %{dir: dir, recycler: recycler} do
      state = bounded_state(dir, recycler, 1_000, 1_000)

      assert %{appended: [], replies: [{:tok, {:error, reason}}], parked?: false} =
               Pushing.push(state, Version.from_integer(3_000), bp_tx(4_000), :tok)

      assert_wal_limit_error(reason,
        commit_version: Version.from_integer(4_000),
        min_durable_version: Version.from_integer(1_000),
        last_version: Version.from_integer(1_000),
        lag_us: 3_000,
        limit_us: 1_000
      )
    end

    test "entries queued before the floor existed cannot cross the bound when the gap fills",
         %{dir: dir, recycler: recycler} do
      # No durable floor yet: the future push is admitted unchecked (the
      # bound cannot be evaluated without a floor).
      state = bounded_state(dir, recycler, 2_500, nil)

      assert %{state: state, parked?: true} =
               Pushing.push(state, Version.from_integer(3_000), bp_tx(5_000), :queued_token)

      # The first confirmation establishes the floor. Filling the gap now
      # admits the filler (2_000 <= 2_500 behind the floor) but must NOT
      # blindly drain the queue past the bound: the queued transaction sits
      # 4_000 behind the floor.
      state = %{state | min_durable_version: Version.from_integer(1_000)}
      filler = bp_tx(3_000)
      v3000 = Version.from_integer(3_000)

      assert %{
               state: after_state,
               appended: [{^v3000, ^filler}],
               replies: [{:filler_token, :ok}, {:queued_token, {:error, reason}}],
               parked?: false
             } = Pushing.push(state, Version.from_integer(1_000), filler, :filler_token)

      assert_wal_limit_error(reason,
        commit_version: Version.from_integer(5_000),
        min_durable_version: Version.from_integer(1_000),
        last_version: Version.from_integer(3_000),
        lag_us: 4_000,
        limit_us: 2_500
      )

      # Ordering state stays consistent: the admitted prefix is the tip,
      # the rejected suffix is gone, nobody is stranded.
      assert after_state.last_version == v3000
      assert after_state.pending_pushes == %{}
    end

    test "tripping the fuse releases every queued successor so recovery cannot strand callers",
         %{dir: dir, recycler: recycler} do
      state = bounded_state(dir, recycler, 2_500, nil)

      assert %{state: state, parked?: true} =
               Pushing.push(state, Version.from_integer(9_000), bp_tx(9_500), :queued_token_1)

      assert %{state: state, parked?: true} =
               Pushing.push(state, Version.from_integer(9_500), bp_tx(10_000), :queued_token_2)

      state = %{state | min_durable_version: Version.from_integer(1_000)}

      assert %{
               state: after_state,
               appended: [],
               replies: [
                 {:direct_token, {:error, reason}},
                 {:queued_token_1, {:error, reason_1}},
                 {:queued_token_2, {:error, reason_2}}
               ],
               parked?: false
             } = Pushing.push(state, Version.from_integer(1_000), bp_tx(9_000), :direct_token)

      assert_wal_limit_error(reason,
        commit_version: Version.from_integer(9_000),
        min_durable_version: Version.from_integer(1_000),
        last_version: Version.from_integer(1_000),
        lag_us: 8_000,
        limit_us: 2_500
      )

      assert reason_1 == reason
      assert reason_2 == reason
      assert after_state.pending_pushes == %{}
    end

    test "with no hard limit, arbitrarily lagged pushes stay admitted", %{dir: dir, recycler: recycler} do
      state = bounded_state(dir, recycler, nil, 1_000)
      tx = bp_tx(10_000_000)
      v = Version.from_integer(10_000_000)

      assert %{state: state, appended: [{^v, ^tx}], replies: [{:tok, :ok}], parked?: false} =
               Pushing.push(state, Version.from_integer(1_000), tx, :tok)

      assert state.last_version == v
    end
  end

  describe "segment rolling on cut boundaries" do
    setup :recycler_in_tmp_dir

    defp write!(state, version_int) do
      tx = TransactionTestSupport.new_log_transaction(version_int, %{"k" => "v"})
      {:ok, state, _event} = Pushing.append_transaction(state, tx)
      state
    end

    test "a push crossing a cut-interval boundary rolls the active segment", %{dir: dir, recycler: recycler} do
      interval = Demux.Server.default_cut_interval_us()

      state = %State{
        mode: :running,
        path: dir,
        segment_recycler: recycler,
        writer: nil,
        active_segment: nil,
        segments: [],
        last_version: Version.from_integer(0)
      }

      # Two writes inside the first bucket: one segment, no roll
      state = state |> write!(1_000) |> write!(2_000)
      assert state.segments == []
      first_segment = state.active_segment

      # Crossing into the next bucket rolls: fresh active segment named for
      # the crossing version, previous segment retired to the trimmable list
      state = write!(state, interval + 1)

      assert state.active_segment.min_version == Version.from_integer(interval + 1)
      assert [%{min_version: min}] = state.segments
      assert min == first_segment.min_version

      # Another write in the same bucket does not roll again
      state = write!(state, interval + 2_000)
      assert length(state.segments) == 1

      # A quiet stretch spanning several buckets still rolls just once
      state = write!(state, 4 * interval + 1)
      assert length(state.segments) == 2
      assert state.active_segment.min_version == Version.from_integer(4 * interval + 1)
    end

    # The roll boundary exists to match the Demux's cut boundary exactly —
    # a segment holds one cut bucket, which is what lets trimming drop
    # history at the cut cadence despite the active segment being
    # trim-immune. The Demux's bucket width is configurable, so reading
    # the module default here would silently break that correspondence
    # for any log whose Demux was configured otherwise.
    defp state_with_cut_interval(dir, recycler, cut_interval_us) do
      %State{
        mode: :running,
        path: dir,
        segment_recycler: recycler,
        writer: nil,
        active_segment: nil,
        segments: [],
        last_version: Version.from_integer(0),
        cut_interval_us: cut_interval_us
      }
    end

    test "rolls on a configured interval narrower than the default", %{dir: dir, recycler: recycler} do
      interval = div(Demux.Server.default_cut_interval_us(), 5)
      state = state_with_cut_interval(dir, recycler, interval)

      state = write!(state, 1_000)
      assert state.segments == []

      # Crosses the configured boundary while still inside bucket 0 at the
      # default width — only a state-driven interval can see this roll.
      state = write!(state, interval + 1)

      assert [_predecessor] = state.segments
      assert state.active_segment.min_version == Version.from_integer(interval + 1)
    end

    test "does not roll at the default boundary when configured wider", %{dir: dir, recycler: recycler} do
      default = Demux.Server.default_cut_interval_us()
      state = state_with_cut_interval(dir, recycler, default * 4)

      state = write!(state, 1_000)

      # Crosses the DEFAULT boundary but not the configured one.
      state = write!(state, default + 1)

      assert state.segments == [],
             "the roll must follow the configured cut interval, not the module default"

      assert state.active_segment.min_version == Version.from_integer(1_000)
    end
  end

  describe "rollover publishes only after the successor cursor is durable" do
    setup :recycler_in_tmp_dir

    # A sync_fun that succeeds until armed, then fails with :eio until
    # disarmed. Also counts every call so the single-barrier criterion can
    # be asserted.
    defp controlled_sync do
      {:ok, control} = Agent.start_link(fn -> %{fail: false, calls: 0} end)

      sync_fun = fn _fd ->
        Agent.get_and_update(control, fn state ->
          result = if state.fail, do: {:error, :eio}, else: :ok
          {result, %{state | calls: state.calls + 1}}
        end)
      end

      {control, sync_fun}
    end

    defp state_with(dir, recycler, writer_opts) do
      %State{
        mode: :running,
        path: dir,
        segment_recycler: recycler,
        writer: nil,
        active_segment: nil,
        segments: [],
        last_version: Version.from_integer(0),
        oldest_version: nil,
        writer_opts: writer_opts
      }
    end

    defp tx(version_int), do: TransactionTestSupport.new_log_transaction(version_int, %{"k" => "v"})

    defp wal_files(dir), do: dir |> File.ls!() |> Enum.filter(&String.starts_with?(&1, "wal_"))

    test "a failed first-append sync on a rolled successor leaves the predecessor active and trim-immune",
         %{dir: dir, recycler: recycler} do
      interval = Demux.Server.default_cut_interval_us()
      {control, sync_fun} = controlled_sync()

      state = state_with(dir, recycler, sync_fun: sync_fun)
      {:ok, state, _} = Pushing.append_transaction(state, tx(1_000))
      {:ok, state, _} = Pushing.append_transaction(state, tx(2_000))
      predecessor = state.active_segment
      wal_files_before = wal_files(dir)

      # The roll happens with the predecessor's writer already closed, so
      # the state entering the staged path carries writer: nil.
      Agent.update(control, &%{&1 | fail: true})

      assert {:error, :eio, failed_state} =
               Pushing.append_transaction(state, tx(interval + 1))

      # The predecessor is still the active segment — not in the
      # trim-eligible list — and no successor is represented as owned WAL
      # state. A watermark processed now has nothing to recycle and no
      # successor to advance available_after from.
      assert failed_state.active_segment.path == predecessor.path
      assert failed_state.segments == []
      assert failed_state.writer == nil
      assert failed_state.last_version == Version.from_integer(2_000)
      assert failed_state.oldest_version == Version.from_integer(1_000)

      # The failed successor file was recycled back to the preallocated
      # pool: the WAL namespace holds exactly the files it held before.
      assert wal_files(dir) == wal_files_before

      # A subsequent valid push retries the roll and succeeds.
      Agent.update(control, &%{&1 | fail: false})
      Agent.update(control, &%{&1 | calls: 0})

      assert {:ok, rolled_state, _} =
               Pushing.append_transaction(failed_state, tx(interval + 1))

      assert rolled_state.active_segment.min_version == Version.from_integer(interval + 1)
      assert [%{path: rolled_predecessor_path}] = rolled_state.segments
      assert rolled_predecessor_path == predecessor.path
      assert rolled_state.last_version == Version.from_integer(interval + 1)

      # Header plus first entry share one sync barrier: the successful
      # roll issued exactly one sync.
      assert Agent.get(control, & &1.calls) == 1
    end

    test "a failed first-append pwrite on a rolled successor leaves the predecessor active",
         %{dir: dir, recycler: recycler} do
      interval = Demux.Server.default_cut_interval_us()
      {:ok, control} = Agent.start_link(fn -> false end)

      # Fail only entry pwrites (offset > 0) while armed; the header write
      # at offset 0 succeeds, so the failure lands on the first append.
      pwrite_fun = fn fd, offset, data ->
        if Agent.get(control, & &1) and offset > 0 do
          {:error, :enospc}
        else
          :file.pwrite(fd, offset, data)
        end
      end

      state = state_with(dir, recycler, pwrite_fun: pwrite_fun)
      {:ok, state, _} = Pushing.append_transaction(state, tx(1_000))
      predecessor = state.active_segment
      wal_files_before = wal_files(dir)

      Agent.update(control, fn _ -> true end)

      assert {:error, :enospc, failed_state} =
               Pushing.append_transaction(state, tx(interval + 1))

      assert failed_state.active_segment.path == predecessor.path
      assert failed_state.segments == []
      assert failed_state.writer == nil
      assert failed_state.last_version == Version.from_integer(1_000)
      assert wal_files(dir) == wal_files_before

      Agent.update(control, fn _ -> false end)

      assert {:ok, rolled_state, _} =
               Pushing.append_transaction(failed_state, tx(interval + 1))

      assert rolled_state.active_segment.min_version == Version.from_integer(interval + 1)
      assert [%{path: kept}] = rolled_state.segments
      assert kept == predecessor.path
    end

    test "cold restart after a failed roll reconstructs the retained predecessor cursor and transactions",
         %{dir: dir, recycler: recycler} do
      interval = Demux.Server.default_cut_interval_us()
      {control, sync_fun} = controlled_sync()

      state = state_with(dir, recycler, sync_fun: sync_fun)
      first = tx(1_000)
      second = tx(2_000)
      {:ok, state, _} = Pushing.append_transaction(state, first)
      {:ok, state, _} = Pushing.append_transaction(state, second)

      Agent.update(control, &%{&1 | fail: true})
      assert {:error, :eio, _failed_state} = Pushing.append_transaction(state, tx(interval + 1))

      # What a cold start reads from disk is the predecessor alone, with
      # both transactions and its original replay cursor.
      assert {:ok, [segment]} = ColdStarting.reload_segments_at_path(dir)
      assert segment.previous_version == Version.from_integer(0)
      assert [^second, ^first] = Segment.transactions(segment)
    end
  end

  describe "appends decide emptiness from state, never by reading segments" do
    setup :recycler_in_tmp_dir

    test "a recycler-allocated segment is known empty without touching the file", %{dir: dir, recycler: recycler} do
      assert {:ok, segment} =
               Segment.allocate_from_recycler(recycler, dir, Version.from_integer(1_000), Version.zero())

      assert segment.transactions == []
    end

    test "an append never loads segment contents to decide emptiness", %{dir: dir} do
      # The active segment's transaction cache is unloaded and its path
      # does not exist: any attempt to read the file to answer "is the
      # WAL empty?" raises instead of costing a silent 64 MiB read.
      path = Path.join(dir, "wal_real_segment")
      File.write!(path, :binary.copy(<<0>>, 1024))
      assert {:ok, writer} = Writer.open(path, Version.zero())

      state = %State{
        mode: :running,
        last_version: Version.from_integer(1_000),
        available_after: Version.zero(),
        oldest_version: Version.from_integer(1_000),
        writer: writer,
        active_segment: %Segment{
          path: Path.join(dir, "does_not_exist"),
          min_version: Version.from_integer(1_000),
          transactions: nil
        }
      }

      tx = TransactionTestSupport.new_log_transaction(2_000, %{"k" => "v"})
      assert {:ok, state, _event} = Pushing.append_transaction(state, tx)

      # Nonempty WAL (tip past the floor): oldest_version is retained.
      assert state.oldest_version == Version.from_integer(1_000)
      assert state.last_version == Version.from_integer(2_000)
      assert :ok = Writer.close(state.writer)
    end

    test "the first append to an empty WAL initializes oldest_version from the cursor invariant",
         %{dir: dir, recycler: recycler} do
      # Empty retained WAL: the tip sits exactly on the persisted
      # exclusive floor (last_version == available_after).
      floor = Version.from_integer(5_000)

      state = %State{
        mode: :running,
        path: dir,
        segment_recycler: recycler,
        writer: nil,
        active_segment: nil,
        segments: [],
        last_version: floor,
        available_after: floor,
        oldest_version: floor
      }

      tx = TransactionTestSupport.new_log_transaction(6_000, %{"k" => "v"})
      assert {:ok, state, _event} = Pushing.append_transaction(state, tx)

      assert state.oldest_version == Version.from_integer(6_000)
      assert state.last_version == Version.from_integer(6_000)

      # And the next append keeps it: the WAL is no longer empty.
      tx2 = TransactionTestSupport.new_log_transaction(7_000, %{"k" => "v"})
      assert {:ok, state, _event} = Pushing.append_transaction(state, tx2)
      assert state.oldest_version == Version.from_integer(6_000)
    end
  end

  describe "append_transaction/2 error handling" do
    test "an unversionable transaction is an error with the caller's state" do
      state = %State{
        mode: :running,
        last_version: Version.from_integer(0),
        writer: nil,
        segment_recycler: nil
      }

      # Malformed transaction that will fail version extraction
      malformed_transaction = <<0, 1, 2>>

      assert {:error, _reason, ^state} = Pushing.append_transaction(state, malformed_transaction)
    end
  end
end
