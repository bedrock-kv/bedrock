defmodule Bedrock.DataPlane.Log.Shale.PushingTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Demux
  alias Bedrock.DataPlane.Log.Shale.Pushing
  alias Bedrock.DataPlane.Log.Shale.Segment
  alias Bedrock.DataPlane.Log.Shale.SegmentRecycler
  alias Bedrock.DataPlane.Log.Shale.State
  alias Bedrock.DataPlane.Log.Shale.Writer
  alias Bedrock.DataPlane.Version
  alias Bedrock.Test.DataPlane.TransactionTestSupport

  describe "push/4 error conditions" do
    test "rejects transaction that is too large" do
      state = %State{
        mode: :ready,
        last_version: Version.from_integer(0)
      }

      # Create a transaction larger than 10MB limit
      large_transaction = :binary.copy(<<0>>, 10_000_001)
      ack_fn = fn _result -> :ok end

      assert {:error, :tx_too_large} =
               Pushing.push(state, Version.from_integer(1), large_transaction, ack_fn)
    end

    test "rejects out of order transaction with version less than last_version" do
      state = %State{
        mode: :ready,
        last_version: Version.from_integer(5),
        pending_pushes: %{}
      }

      transaction = TransactionTestSupport.new_log_transaction(3, %{"a" => "1"})
      ack_fn = fn _result -> :ok end

      # Trying to push version 3 when last_version is 5
      assert {:error, :tx_out_of_order} =
               Pushing.push(state, Version.from_integer(3), transaction, ack_fn)
    end

    test "waits for future transaction version" do
      state = %State{
        mode: :ready,
        last_version: Version.from_integer(5),
        pending_pushes: %{}
      }

      transaction = TransactionTestSupport.new_log_transaction(10, %{"a" => "1"})
      ack_fn = fn _result -> :ok end

      # Trying to push version 10 when last_version is 5 should wait
      assert {:wait, new_state} =
               Pushing.push(state, Version.from_integer(10), transaction, ack_fn)

      # Transaction should be in pending pushes
      assert Map.has_key?(new_state.pending_pushes, Version.from_integer(10))
    end

    test "acknowledges sync failure as push error" do
      path = Path.join(System.tmp_dir!(), "shale_push_sync_fail_#{System.unique_integer([:positive])}.log")
      File.write!(path, :binary.copy(<<0>>, 1024))

      on_exit(fn ->
        File.rm(path)
      end)

      assert {:ok, writer} = Writer.open(path, sync_fun: fn _fd -> {:error, :eio} end)

      state = %State{
        mode: :ready,
        last_version: Version.from_integer(0),
        pending_pushes: %{},
        writer: writer,
        active_segment: %Segment{path: path, min_version: Version.zero(), transactions: []}
      }

      transaction = TransactionTestSupport.new_log_transaction(0, %{"a" => "1"})
      caller = self()

      ack_fn = fn result ->
        send(caller, {:ack_result, result})
        :ok
      end

      assert {:error, :eio, ^state, []} =
               Pushing.push(state, Version.from_integer(0), transaction, ack_fn)

      assert_receive {:ack_result, {:error, :eio}}

      assert :ok = Writer.close(writer)
    end
  end

  describe "draining queued pushes" do
    setup do
      dir = Path.join(System.tmp_dir!(), "pushing_drain_#{System.unique_integer([:positive])}")
      File.mkdir_p!(dir)

      {:ok, recycler} =
        SegmentRecycler.start_link(
          path: dir,
          min_available: 2,
          max_available: 4,
          segment_size: 1_000_000
        )

      on_exit(fn -> File.rm_rf!(dir) end)

      %{dir: dir, recycler: recycler}
    end

    test "returns every appended transaction in predecessor-chain order", %{dir: dir, recycler: recycler} do
      state = %State{
        mode: :ready,
        path: dir,
        segment_recycler: recycler,
        writer: nil,
        active_segment: nil,
        segments: [],
        last_version: Version.zero(),
        pending_pushes: %{}
      }

      first = TransactionTestSupport.new_log_transaction(1, %{"first" => "1"})
      second = TransactionTestSupport.new_log_transaction(2, %{"second" => "2"})
      caller = self()

      assert {:wait, state} =
               Pushing.push(state, Version.from_integer(1), second, fn result ->
                 send(caller, {:ack, :second, result})
                 :ok
               end)

      refute_receive {:ack, :second, _}

      assert {:ok, state, [^first, ^second]} =
               Pushing.push(state, Version.zero(), first, fn result ->
                 send(caller, {:ack, :first, result})
                 :ok
               end)

      assert_receive {:ack, :first, :ok}
      assert_receive {:ack, :second, :ok}
      assert state.last_version == Version.from_integer(2)
      assert state.pending_pushes == %{}
    end

    test "retains the valid state and appended prefix when a queued append fails" do
      path = Path.join(System.tmp_dir!(), "pushing_partial_#{System.unique_integer([:positive])}.log")
      File.write!(path, :binary.copy(<<0>>, 1_000_000))
      on_exit(fn -> File.rm(path) end)

      {:ok, sync_count} = Agent.start_link(fn -> 0 end)

      sync_fun = fn _fd ->
        Agent.get_and_update(sync_count, fn
          0 -> {:ok, 1}
          count -> {{:error, :eio}, count + 1}
        end)
      end

      assert {:ok, writer} = Writer.open(path, sync_fun: sync_fun)

      state = %State{
        mode: :ready,
        last_version: Version.zero(),
        pending_pushes: %{},
        writer: writer,
        active_segment: %Segment{path: path, min_version: Version.zero(), transactions: []}
      }

      first = TransactionTestSupport.new_log_transaction(1, %{"first" => "1"})
      second = TransactionTestSupport.new_log_transaction(2, %{"second" => "2"})
      caller = self()

      assert {:wait, state} =
               Pushing.push(state, Version.from_integer(1), second, fn result ->
                 send(caller, {:ack, :second, result})
                 :ok
               end)

      assert {:error, :eio, state, [^first]} =
               Pushing.push(state, Version.zero(), first, fn result ->
                 send(caller, {:ack, :first, result})
                 :ok
               end)

      assert_receive {:ack, :first, :ok}
      assert_receive {:ack, :second, {:error, :eio}}
      assert state.last_version == Version.from_integer(1)
      assert state.pending_pushes == %{}
      assert :ok = Writer.close(state.writer)
    end
  end

  describe "segment rolling on cut boundaries" do
    setup do
      dir = Path.join(System.tmp_dir!(), "pushing_roll_#{System.unique_integer([:positive])}")
      File.mkdir_p!(dir)

      {:ok, recycler} =
        SegmentRecycler.start_link(
          path: dir,
          min_available: 2,
          max_available: 4,
          segment_size: 1_000_000
        )

      on_exit(fn -> File.rm_rf!(dir) end)

      %{dir: dir, recycler: recycler}
    end

    defp write!(state, version_int) do
      tx = TransactionTestSupport.new_log_transaction(version_int, %{"k" => "v"})
      {:ok, state} = Pushing.write_encoded_transaction(state, tx)
      state
    end

    test "a push crossing a cut-interval boundary rolls the active segment", %{dir: dir, recycler: recycler} do
      interval = Demux.Server.default_cut_interval_us()

      state = %State{
        mode: :ready,
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
  end

  describe "write_encoded_transaction/2 error handling" do
    test "raises when transaction version cannot be extracted" do
      state = %State{
        mode: :ready,
        last_version: Version.from_integer(0),
        writer: nil,
        segment_recycler: nil
      }

      # Malformed transaction that will fail version extraction
      malformed_transaction = <<0, 1, 2>>

      assert_raise RuntimeError, ~r/Failed to extract version/, fn ->
        Pushing.write_encoded_transaction(state, malformed_transaction)
      end
    end
  end
end
