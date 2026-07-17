defmodule Bedrock.DataPlane.Log.Shale.TrimmingTest do
  @moduledoc """
  Tests for inline durability reporting (`durable_up_to` on `Log.pull`) and
  the automatic WAL segment trimming it drives.

  All tests are driven by explicit pushes/pulls and synchronous calls — no
  timing dependence.
  """
  use ExUnit.Case, async: false

  alias Bedrock.Cluster
  alias Bedrock.DataPlane.Log.Shale.Segment
  alias Bedrock.DataPlane.Log.Shale.Server
  alias Bedrock.DataPlane.Log.Shale.Writer
  alias Bedrock.DataPlane.Version
  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.LocalFilesystem
  alias Bedrock.Test.DataPlane.TransactionTestSupport

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    otp_name = :"test_log_trim_#{System.unique_integer([:positive])}"
    id = "test_log_trim_#{System.unique_integer([:positive])}"
    path = Path.join(tmp_dir, "log_segments")
    object_storage = ObjectStorage.backend(LocalFilesystem, root: Path.join(tmp_dir, "object_storage"))

    File.mkdir_p!(path)

    server_opts = [
      cluster: Cluster,
      otp_name: otp_name,
      id: id,
      foreman: self(),
      path: path,
      object_storage: object_storage,
      start_unlocked: true
    ]

    {:ok, path: path, server_opts: server_opts}
  end

  describe "durable_up_to on pull" do
    setup %{server_opts: opts} do
      {:ok, server: setup_server(opts)}
    end

    test "advances the trim point and is reflected in facts", %{server: pid} do
      push_transactions(pid, 1..3)
      v2 = Version.from_integer(2)

      # First pull catches up without reporting; the follow-up pull reports
      # that everything at or below v2 is durable downstream.
      assert {:ok, transactions} = GenServer.call(pid, {:pull, Version.zero(), [limit: 10]})
      assert length(transactions) == 3

      assert {:ok, [_tx3]} = GenServer.call(pid, {:pull, v2, [limit: 10, durable_up_to: v2]})

      assert :sys.get_state(pid).min_durable_version == v2

      assert {:ok, %{minimum_durable_version: ^v2}} =
               GenServer.call(pid, {:info, [:minimum_durable_version]})
    end

    test "is monotonic: an older report never regresses the trim point", %{server: pid} do
      push_transactions(pid, 1..3)
      v1 = Version.from_integer(1)
      v2 = Version.from_integer(2)

      assert {:ok, _} = GenServer.call(pid, {:pull, v2, [durable_up_to: v2]})
      assert {:ok, _} = GenServer.call(pid, {:pull, v2, [durable_up_to: v1]})

      assert :sys.get_state(pid).min_durable_version == v2
    end

    test "a pull without durable_up_to leaves the trim point untouched", %{server: pid} do
      push_transactions(pid, 1..3)
      v2 = Version.from_integer(2)

      assert {:ok, _} = GenServer.call(pid, {:pull, v2, [durable_up_to: v2]})
      assert {:ok, _} = GenServer.call(pid, {:pull, v2, []})

      assert :sys.get_state(pid).min_durable_version == v2
    end

    test "cooperates with the Demux {:min_durable_version, _} report path", %{server: pid} do
      push_transactions(pid, 1..3)
      v1 = Version.from_integer(1)
      v2 = Version.from_integer(2)

      assert {:ok, _} = GenServer.call(pid, {:pull, v2, [durable_up_to: v2]})

      # An older out-of-band report must not regress the pull-reported point.
      send(pid, {:min_durable_version, v1})
      :pong = GenServer.call(pid, :ping)
      assert :sys.get_state(pid).min_durable_version == v2
    end
  end

  describe "automatic segment trimming" do
    setup %{server_opts: opts, path: path} do
      server = setup_server(opts)
      segments = install_segments(server, path)
      {:ok, server: server, segments: segments}
    end

    test "segments entirely below the trim point are returned to the recycler", %{
      server: pid,
      segments: %{v20: v20, v30: v30, seg10_path: seg10_path, seg20_path: seg20_path, tx30: tx30}
    } do
      assert {:ok, [^tx30]} = GenServer.call(pid, {:pull, v20, [limit: 10, durable_up_to: v20]})

      state = :sys.get_state(pid)
      assert state.min_durable_version == v20
      assert Enum.map(state.segments, & &1.min_version) == [v20]
      assert state.oldest_version == v20
      assert state.active_segment.min_version == v30

      refute File.exists?(seg10_path)
      assert File.exists?(seg20_path)
    end

    test "the segment containing the trim point itself is never trimmed", %{
      server: pid,
      segments: %{v10: v10, v20: v20, seg10_path: seg10_path, seg20_path: seg20_path}
    } do
      # Everything <= v10 is durable; the segment whose last transaction is
      # exactly v10 must be retained so pull(start_after: v10) stays servable.
      assert {:ok, _} = GenServer.call(pid, {:pull, v10, [limit: 10, durable_up_to: v10]})

      state = :sys.get_state(pid)
      assert Enum.map(state.segments, & &1.min_version) == [v20, v10]
      assert state.oldest_version == v10
      assert File.exists?(seg10_path)
      assert File.exists?(seg20_path)
    end

    test "the active write segment is never recycled", %{
      server: pid,
      segments: %{v30: v30, seg10_path: seg10_path, seg20_path: seg20_path, seg30_path: seg30_path}
    } do
      v100 = Version.from_integer(100)

      # No transactions exist after v100, and no willing_to_wait_in_ms was
      # given, so the pull itself is refused - but the durability report and
      # the trimming it drives still apply.
      assert {:error, :version_too_new} =
               GenServer.call(pid, {:pull, v100, [limit: 10, durable_up_to: v100]})

      state = :sys.get_state(pid)
      assert state.min_durable_version == v100
      assert state.segments == []
      assert state.active_segment.min_version == v30
      assert state.oldest_version == v30

      refute File.exists?(seg10_path)
      refute File.exists?(seg20_path)
      assert File.exists?(seg30_path)
    end

    test "a pull for trimmed history returns :version_too_old", %{
      server: pid,
      segments: %{v10: v10, v20: v20}
    } do
      assert {:ok, _} = GenServer.call(pid, {:pull, v20, [limit: 10, durable_up_to: v20]})

      assert {:error, :version_too_old} = GenServer.call(pid, {:pull, v10, [limit: 10]})
      assert {:error, :version_too_old} = GenServer.call(pid, {:pull, Version.zero(), [limit: 10]})
    end

    test "a pull at exactly the trim point still succeeds after trimming", %{
      server: pid,
      segments: %{v20: v20, tx30: tx30}
    } do
      assert {:ok, _} = GenServer.call(pid, {:pull, v20, [limit: 10, durable_up_to: v20]})
      assert {:ok, [^tx30]} = GenServer.call(pid, {:pull, v20, [limit: 10]})
    end

    test "trimming emits telemetry with the segment count and new trim point", %{
      server: pid,
      segments: %{v20: v20}
    } do
      handler_id = "trim-telemetry-#{System.unique_integer([:positive])}"
      test_pid = self()

      :telemetry.attach(
        handler_id,
        [:bedrock, :log, :segments_trimmed],
        fn event, measurements, metadata, _ -> send(test_pid, {:trim_event, event, measurements, metadata}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler_id) end)

      assert {:ok, _} = GenServer.call(pid, {:pull, v20, [limit: 10, durable_up_to: v20]})

      assert_receive {:trim_event, [:bedrock, :log, :segments_trimmed], %{segments_trimmed: 1}, %{trim_point: ^v20}}

      # A repeated report at the same trim point trims nothing and emits nothing.
      assert {:ok, _} = GenServer.call(pid, {:pull, v20, [limit: 10, durable_up_to: v20]})
      refute_receive {:trim_event, _, _, _}
    end

    test "recovery still works after trimming", %{
      server: pid,
      segments: %{v20: v20, v30: v30}
    } do
      assert {:ok, _} = GenServer.call(pid, {:pull, v20, [limit: 10, durable_up_to: v20]})

      assert {:ok, ^pid, info} = GenServer.call(pid, {:lock_for_recovery, 1})

      assert %{
               kind: :log,
               oldest_version: ^v20,
               last_version: ^v30,
               minimum_durable_version: ^v20
             } = info

      assert {:ok, ^pid} = GenServer.call(pid, {:recover_from, [], v30, v30})

      # The recovered log accepts new pushes and serves them to pulls.
      tx31 = TransactionTestSupport.new_log_transaction(31, %{"k31" => "v31"})
      assert :ok = GenServer.call(pid, {:push, tx31, v30})
      assert {:ok, [^tx31]} = GenServer.call(pid, {:pull, v30, [limit: 10]})
    end
  end

  # Installs a realistic three-segment layout into the running server:
  #
  #   seg10 (inactive):  tx10            - fully below any trim point >= v20
  #   seg20 (inactive):  tx20            - contains version 20
  #   seg30 (active):    tx30            - the active write segment
  #
  # Segment files are real WAL files written via Writer so that recovery and
  # the segment recycler can operate on them.
  defp install_segments(pid, path) do
    v10 = Version.from_integer(10)
    v20 = Version.from_integer(20)
    v30 = Version.from_integer(30)

    tx10 = TransactionTestSupport.new_log_transaction(10, %{"k10" => "v10"})
    tx20 = TransactionTestSupport.new_log_transaction(20, %{"k20" => "v20"})
    tx30 = TransactionTestSupport.new_log_transaction(30, %{"k30" => "v30"})

    seg10_path = Path.join(path, Segment.encode_file_name(10))
    seg20_path = Path.join(path, Segment.encode_file_name(20))
    seg30_path = Path.join(path, Segment.encode_file_name(30))

    write_segment_file!(seg10_path, [{tx10, v10}])
    write_segment_file!(seg20_path, [{tx20, v20}])
    write_segment_file!(seg30_path, [{tx30, v30}])

    :sys.replace_state(pid, fn state ->
      %{
        state
        | active_segment: %Segment{path: seg30_path, min_version: v30, transactions: [tx30]},
          segments: [
            %Segment{path: seg20_path, min_version: v20, transactions: [tx20]},
            %Segment{path: seg10_path, min_version: v10, transactions: [tx10]}
          ],
          oldest_version: v10,
          last_version: v30,
          min_durable_version: nil
      }
    end)

    %{
      v10: v10,
      v20: v20,
      v30: v30,
      tx10: tx10,
      tx20: tx20,
      tx30: tx30,
      seg10_path: seg10_path,
      seg20_path: seg20_path,
      seg30_path: seg30_path
    }
  end

  defp write_segment_file!(path, transactions_with_versions) do
    File.write!(path, :binary.copy(<<0>>, 65_536))
    {:ok, writer} = Writer.open(path)

    writer =
      Enum.reduce(transactions_with_versions, writer, fn {transaction, version}, writer ->
        {:ok, writer} = Writer.append(writer, transaction, version)
        writer
      end)

    :ok = Writer.close(writer)
  end

  defp push_transactions(pid, versions) do
    Enum.each(versions, fn i ->
      transaction = TransactionTestSupport.new_log_transaction(i, %{"k#{i}" => "v#{i}"})
      expected_version = Version.from_integer(i - 1)
      :ok = GenServer.call(pid, {:push, transaction, expected_version})
    end)
  end

  defp setup_server(opts) do
    pid = start_supervised!(Server.child_spec(opts))

    eventually(fn ->
      state = :sys.get_state(pid)
      assert state.segment_recycler
    end)

    pid
  end

  defp eventually(assertion_fn, timeout \\ 1000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    eventually_loop(assertion_fn, deadline)
  end

  defp eventually_loop(assertion_fn, deadline) do
    assertion_fn.()
  rescue
    _ ->
      if System.monotonic_time(:millisecond) < deadline do
        eventually_loop(assertion_fn, deadline)
      else
        assertion_fn.()
      end
  end
end
