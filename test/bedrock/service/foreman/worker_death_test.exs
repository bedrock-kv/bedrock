defmodule Bedrock.Service.Foreman.WorkerDeathTest do
  use ExUnit.Case, async: false

  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.LocalFilesystem
  alias Bedrock.Service.Foreman.Server

  # A worker's health is recorded once, when it starts, and never
  # revisited. Nothing monitors the process, so {:ok, pid} outlives the
  # process it names: the foreman goes on reporting a dead worker as
  # running, relaying layout pushes to it, and — since the verdict is a
  # fold over these values — reporting itself healthy.
  alias Bedrock.Service.WorkerBehaviour

  @moduletag :tmp_dir

  defmodule DeathTestCluster do
    @moduledoc false
    def name, do: "death_test_cluster"
    def otp_name_for_worker(id), do: :"death_test_worker_#{id}"
    def otp_name(:worker_supervisor), do: :death_test_worker_supervisor
    def otp_name(:link), do: :death_test_link
    def otp_name(:foreman), do: :death_test_foreman
  end

  defmodule DeathTestWorker do
    @moduledoc false
    use WorkerBehaviour, kind: :log
    use GenServer

    def child_spec(opts), do: %{id: {__MODULE__, opts[:id]}, start: {__MODULE__, :start_link, [opts]}}
    def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: opts[:otp_name])

    @impl GenServer
    def init(opts), do: {:ok, opts}
  end

  # Olivine-shaped: init/1 returns immediately and the real startup runs
  # in a continue, which then casts the worker's own health to the
  # foreman. That cast names a pid, and for a small shard it lands well
  # inside the recheck window — so it, not the recheck, is what adopts a
  # replacement.
  defmodule SelfReportingWorker do
    @moduledoc false
    use WorkerBehaviour, kind: :materializer
    use GenServer

    alias Bedrock.Service.Foreman

    def child_spec(opts), do: %{id: {__MODULE__, opts[:id]}, start: {__MODULE__, :start_link, [opts]}}
    def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: opts[:otp_name])

    @impl GenServer
    def init(opts), do: {:ok, opts, {:continue, :report}}

    @impl GenServer
    def handle_continue(:report, opts) do
      Foreman.report_health(opts[:foreman], opts[:id], {:ok, self()})
      {:noreply, opts}
    end
  end

  defp write_self_reporting_worker(dir, id) do
    path = Path.join(dir, id)
    File.mkdir_p!(path)

    File.write!(
      Path.join(path, "manifest.json"),
      ~s({"id":"#{id}","cluster":"death_test_cluster",) <>
        ~s("worker":"Bedrock.Service.Foreman.WorkerDeathTest.SelfReportingWorker","params":{}})
    )
  end

  defp write_worker(dir, id) do
    path = Path.join(dir, id)
    File.mkdir_p!(path)

    File.write!(
      Path.join(path, "manifest.json"),
      ~s({"id":"#{id}","cluster":"death_test_cluster",) <>
        ~s("worker":"Bedrock.Service.Foreman.WorkerDeathTest.DeathTestWorker","params":{}})
    )
  end

  defp start_foreman(dir) do
    start_supervised!({DynamicSupervisor, strategy: :one_for_one, name: DeathTestCluster.otp_name(:worker_supervisor)})

    foreman =
      start_supervised!(%{
        id: Server,
        start:
          {GenServer, :start_link,
           [
             Server,
             %{
               cluster: DeathTestCluster,
               path: dir,
               capabilities: [:log],
               otp_name: DeathTestCluster.otp_name(:foreman),
               object_storage: ObjectStorage.backend(LocalFilesystem, root: Path.join(dir, "object_storage"))
             },
             [name: DeathTestCluster.otp_name(:foreman)]
           ]}
      })

    # Force the :spin_up continue to have run.
    :pong = GenServer.call(foreman, :ping)
    foreman
  end

  defp worker_health(foreman, id), do: :sys.get_state(foreman).workers[id].health

  # A normal exit leaves a :transient child down for good.
  defp stop_for_good(pid) do
    ref = Process.monitor(pid)
    GenServer.stop(pid, :normal)
    assert_receive {:DOWN, ^ref, :process, ^pid, _}
  end

  # The supervisor's restart is concurrent with our :DOWN; wait for the
  # name to be held by someone other than the process that just died.
  defp wait_for_replacement(original, attempts \\ 100) do
    case Process.whereis(DeathTestCluster.otp_name_for_worker("aaaaaaaa")) do
      pid when is_pid(pid) and pid != original ->
        pid

      _ when attempts > 0 ->
        Process.sleep(10)
        wait_for_replacement(original, attempts - 1)

      _ ->
        flunk("the supervisor never replaced the worker")
    end
  end

  # Adoption happens on a timer, because the :DOWN always arrives before
  # the supervisor has started the replacement.
  defp wait_for_adoption(foreman, id, original, attempts \\ 100) do
    case worker_health(foreman, id) do
      {:ok, pid} when pid != original ->
        pid

      _ when attempts > 0 ->
        Process.sleep(10)
        wait_for_adoption(foreman, id, original, attempts - 1)

      health ->
        flunk("the foreman never adopted the replacement; health is #{inspect(health)}")
    end
  end

  describe "a worker that dies" do
    test "stops being reported as running", %{tmp_dir: dir} do
      write_worker(dir, "aaaaaaaa")
      foreman = start_foreman(dir)

      assert {:ok, pid} = worker_health(foreman, "aaaaaaaa")

      ref = Process.monitor(pid)
      Process.exit(pid, :kill)
      assert_receive {:DOWN, ^ref, :process, ^pid, :killed}

      # Settle the foreman's mailbox before observing it.
      :pong = GenServer.call(foreman, :ping)

      refute match?({:ok, ^pid}, worker_health(foreman, "aaaaaaaa")),
             "the foreman must not go on naming a dead process as running"
    end

    # A normal exit does not restart a :transient child, so this worker
    # really is gone and the foreman must say so.
    test "that is not replaced takes the foreman out of :ok", %{tmp_dir: dir} do
      write_worker(dir, "aaaaaaaa")
      foreman = start_foreman(dir)

      assert %{health: :ok} = :sys.get_state(foreman)
      {:ok, pid} = worker_health(foreman, "aaaaaaaa")

      stop_for_good(pid)
      :pong = GenServer.call(foreman, :ping)

      assert worker_health(foreman, "aaaaaaaa") == :stopped

      refute :sys.get_state(foreman).health == :ok,
             "a foreman whose only worker is gone must not report healthy"
    end

    test "that is not replaced is dropped from the running-services roll call", %{tmp_dir: dir} do
      write_worker(dir, "aaaaaaaa")
      foreman = start_foreman(dir)

      {:ok, pid} = worker_health(foreman, "aaaaaaaa")
      stop_for_good(pid)
      :pong = GenServer.call(foreman, :ping)

      assert {:ok, []} = GenServer.call(foreman, :get_all_running_services)
    end
  end

  describe "a worker the supervisor replaces" do
    # Workers are :transient, so an abnormal exit is restarted under the
    # same OTP name. Reporting the replacement as gone would drop a LIVE
    # worker out of the roll call the coordinator is advertised from —
    # the original bug's mirror image.
    test "is re-adopted rather than reported gone", %{tmp_dir: dir} do
      write_worker(dir, "aaaaaaaa")
      foreman = start_foreman(dir)

      {:ok, original} = worker_health(foreman, "aaaaaaaa")

      ref = Process.monitor(original)
      Process.exit(original, :kill)
      assert_receive {:DOWN, ^ref, :process, ^original, :killed}

      wait_for_replacement(original)
      replacement = wait_for_adoption(foreman, "aaaaaaaa", original)

      assert replacement != original
      assert Process.alive?(replacement)
      assert :sys.get_state(foreman).health == :ok
      assert {:ok, [{"aaaaaaaa", :log, _}]} = GenServer.call(foreman, :get_all_running_services)
    end

    test "is monitored again, so a second death is still observed", %{tmp_dir: dir} do
      write_worker(dir, "aaaaaaaa")
      foreman = start_foreman(dir)

      {:ok, original} = worker_health(foreman, "aaaaaaaa")
      ref = Process.monitor(original)
      Process.exit(original, :kill)
      assert_receive {:DOWN, ^ref, :process, ^original, :killed}
      wait_for_replacement(original)
      replacement = wait_for_adoption(foreman, "aaaaaaaa", original)

      assert :sys.get_state(foreman).workers["aaaaaaaa"].monitor_ref

      stop_for_good(replacement)
      :pong = GenServer.call(foreman, :ping)

      assert worker_health(foreman, "aaaaaaaa") == :stopped
    end
  end

  describe "a worker that reports its own health" do
    # The self-report beats the recheck timer, so IT is what adopts the
    # replacement. If adopting that way does not also take a monitor, the
    # worker goes unwatched from then on — no further :DOWN can arrive,
    # and the foreman is back to naming a dead process as running, with
    # no timer left to correct it.
    test "is still watched after its replacement reports in", %{tmp_dir: dir} do
      write_self_reporting_worker(dir, "bbbbbbbb")
      foreman = start_foreman(dir)

      {:ok, original} = worker_health(foreman, "bbbbbbbb")

      ref = Process.monitor(original)
      Process.exit(original, :kill)
      assert_receive {:DOWN, ^ref, :process, ^original, :killed}

      replacement = wait_for_adoption(foreman, "bbbbbbbb", original)
      assert Process.alive?(replacement)

      assert :sys.get_state(foreman).workers["bbbbbbbb"].monitor_ref,
             "adopting via a self-report must take a monitor too, or the worker goes unwatched"

      # The real proof: a second death is still observed.
      stop_for_good(replacement)
      :pong = GenServer.call(foreman, :ping)

      refute match?({:ok, ^replacement}, worker_health(foreman, "bbbbbbbb")),
             "the foreman must not name the second dead process as running"
    end
  end

  describe "a worker the foreman removes" do
    # A deliberate removal is not a death. The entry is gone before the
    # exit signal lands, so a stray :DOWN would be dropped anyway — what
    # the demonitor actually buys is not leaving a monitor behind that
    # nothing can ever reach again, since the entry holding its ref is
    # deleted.
    test "does not resurface as a death, and leaves no monitor behind", %{tmp_dir: dir} do
      write_worker(dir, "aaaaaaaa")
      foreman = start_foreman(dir)

      {:ok, pid} = worker_health(foreman, "aaaaaaaa")
      {:monitors, before} = Process.info(foreman, :monitors)
      assert {:process, pid} in before

      assert :ok = GenServer.call(foreman, {:remove_worker, "aaaaaaaa"})
      :pong = GenServer.call(foreman, :ping)

      state = :sys.get_state(foreman)
      assert state.workers == %{}
      assert state.health == :ok

      {:monitors, remaining} = Process.info(foreman, :monitors)

      refute {:process, pid} in remaining,
             "removal must release the monitor, not orphan it"
    end
  end
end
