defmodule Bedrock.Service.Foreman.StartingWorkersTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog, only: [with_log: 1]

  alias Bedrock.Service.Foreman.StartingWorkers
  alias Bedrock.Service.Foreman.StartingWorkers.StartWorkerOp
  alias Bedrock.Service.WorkerBehaviour

  # Define mock modules at compile time
  defmodule MockWorker do
    @moduledoc false
    def child_spec(opts) do
      # Return the opts as the start tuple so we can inspect what was passed
      %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}}
    end
  end

  defmodule MockCluster do
    @moduledoc false
    def name, do: "starting_workers_test_cluster"
    def otp_name(:foreman), do: :test_foreman
    def otp_name(:worker_supervisor), do: :test_worker_supervisor
  end

  defmodule StartsWorker do
    @moduledoc false
    use WorkerBehaviour, kind: :log
    use GenServer

    def child_spec(opts), do: %{id: {__MODULE__, opts[:id]}, start: {__MODULE__, :start_link, [opts]}}
    def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: opts[:otp_name])

    @impl GenServer
    def init(opts), do: {:ok, opts}
  end

  defmodule RaisesWorker do
    @moduledoc false
    use WorkerBehaviour, kind: :log

    def child_spec(_opts), do: raise("this worker cannot be started")
  end

  defmodule SlowWorker do
    @moduledoc false
    use WorkerBehaviour, kind: :log

    def child_spec(_opts) do
      Process.sleep(2_000)
      %{id: __MODULE__, start: {__MODULE__, :start_link, []}}
    end
  end

  defp write_worker(dir, id, worker) do
    path = Path.join(dir, id)
    File.mkdir_p!(path)

    File.write!(
      Path.join(path, "manifest.json"),
      ~s({"id":"#{id}","cluster":"starting_workers_test_cluster",) <>
        ~s("worker":"#{inspect(worker)}","params":{}})
    )

    StartingWorkers.worker_info_for_id(id, path, &:"starting_workers_test_#{&1}")
  end

  describe "build_child_spec/1" do
    test "includes object_storage in worker options" do
      mock_manifest = %{
        worker: MockWorker,
        params: %{}
      }

      object_storage = {Bedrock.ObjectStorage.LocalFilesystem, root: "/tmp/test"}

      op = %StartWorkerOp{
        id: "test-worker",
        path: "/tmp/workers/test-worker",
        otp_name: :test_worker,
        cluster: MockCluster,
        manifest: mock_manifest,
        object_storage: object_storage,
        error: nil
      }

      result = StartingWorkers.build_child_spec(op)

      # Extract the opts that were passed to the worker's child_spec
      %{start: {_mod, :start_link, [opts]}} = result.child_spec

      assert Keyword.get(opts, :object_storage) == object_storage
    end
  end

  describe "try_to_start_workers/4" do
    setup %{tmp_dir: dir} do
      start_supervised!({DynamicSupervisor, strategy: :one_for_one, name: MockCluster.otp_name(:worker_supervisor)})
      %{dir: dir}
    end

    # Starting happens inside a linked task, so a start that raises used
    # to reach the CALLER as an exit signal — before any result was
    # mapped — and there is no result at all for the workers that started
    # fine. The exception is the one worker's, and so is the verdict.
    @tag :tmp_dir
    test "a start that raises fails that worker alone", %{dir: dir} do
      {health, _log} =
        with_log(fn ->
          [
            write_worker(dir, "aaaaaaaa", StartsWorker),
            write_worker(dir, "bbbbbbbb", RaisesWorker)
          ]
          |> StartingWorkers.try_to_start_workers(MockCluster, nil)
          |> Map.new(&{&1.id, &1.health})
        end)

      assert {:ok, pid} = health["aaaaaaaa"]
      assert Process.alive?(pid)

      assert {:failed_to_start, {:error, %RuntimeError{message: "this worker cannot be started"}}} =
               health["bbbbbbbb"]
    end

    # The timeout is the half that needs no bug at all to reach: a worker
    # that opens a WAL and preallocates segments is exactly the one that
    # overruns, and an overrun is not a diagnosis of the whole node.
    @tag :tmp_dir
    test "a start that overruns the timeout fails that worker alone", %{dir: dir} do
      {health, _log} =
        with_log(fn ->
          [
            write_worker(dir, "cccccccc", StartsWorker),
            write_worker(dir, "dddddddd", SlowWorker)
          ]
          |> StartingWorkers.try_to_start_workers(MockCluster, nil, 200)
          |> Map.new(&{&1.id, &1.health})
        end)

      assert {:ok, pid} = health["cccccccc"]
      assert Process.alive?(pid)
      assert {:failed_to_start, :timeout} = health["dddddddd"]
    end
  end
end
