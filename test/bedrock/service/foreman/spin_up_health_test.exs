defmodule Bedrock.Service.Foreman.SpinUpHealthTest do
  use ExUnit.Case, async: true

  alias Bedrock.Service.Foreman.Impl
  alias Bedrock.Service.Foreman.State

  # Spin-up is where a foreman learns what it has and starts it. If the
  # verdict is not recomputed there, the foreman keeps the :starting it
  # was born with no matter how the boot actually went — and since
  # recompute_health/1 is otherwise reachable only from a worker's own
  # health cast, and Shale never sends one, a log-only node would sit at
  # :starting forever.
  @moduletag :tmp_dir

  defmodule SpinUpTestCluster do
    @moduledoc false
    def name, do: "spin_up_test_cluster"
    def otp_name_for_worker(id), do: :"spin_up_test_worker_#{id}"
    def otp_name(:worker_supervisor), do: :spin_up_test_worker_supervisor
    def otp_name(:link), do: :spin_up_test_link
    def otp_name(:foreman), do: :spin_up_test_foreman
  end

  defmodule SpinUpTestWorker do
    @moduledoc false
    use Bedrock.Service.WorkerBehaviour, kind: :log
    use GenServer

    def child_spec(opts), do: %{id: {__MODULE__, opts[:id]}, start: {__MODULE__, :start_link, [opts]}}
    def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: opts[:otp_name])

    @impl GenServer
    def init(opts), do: {:ok, opts}
  end

  defp write_worker(dir, id) do
    path = Path.join(dir, id)
    File.mkdir_p!(path)

    File.write!(
      Path.join(path, "manifest.json"),
      ~s({"id":"#{id}","cluster":"spin_up_test_cluster",) <>
        ~s("worker":"Bedrock.Service.Foreman.SpinUpHealthTest.SpinUpTestWorker","params":{}})
    )
  end

  defp base_state(dir, overrides \\ []) do
    struct!(
      %State{
        cluster: SpinUpTestCluster,
        capabilities: [:log],
        health: :starting,
        otp_name: :spin_up_test_foreman,
        path: dir,
        waiting_for_healthy: [],
        workers: %{}
      },
      overrides
    )
  end

  describe "do_spin_up/1" do
    test "computes a verdict rather than leaving the birth state", %{tmp_dir: dir} do
      state = Impl.do_spin_up(base_state(dir))

      refute state.health == :starting,
             "spin-up must reach a verdict; :starting is what the foreman was born with"

      assert state.health == :ok
    end

    # The empty case is the log-only node in miniature: nothing will ever
    # cast a health report, so if spin-up does not settle the verdict
    # nothing else will.
    test "an empty working directory settles at :ok", %{tmp_dir: dir} do
      assert %{health: :ok} = Impl.do_spin_up(base_state(dir))
    end

    test "a directory of other components' data does not make the foreman unhealthy", %{tmp_dir: dir} do
      File.mkdir_p!(Path.join(dir, "object_storage"))
      File.mkdir_p!(Path.join(dir, "raft"))

      assert %{health: :ok} = Impl.do_spin_up(base_state(dir))
    end

    # The headline scenario, not the empty-directory one: a log worker
    # that really starts. Shale never reports its own health, so before
    # this change nothing would ever have moved the verdict off :starting
    # on a node like this.
    test "a worker that actually starts drives the verdict to :ok", %{tmp_dir: dir} do
      start_supervised!(
        {DynamicSupervisor, strategy: :one_for_one, name: SpinUpTestCluster.otp_name(:worker_supervisor)}
      )

      write_worker(dir, "aaaaaaaa")

      state = Impl.do_spin_up(base_state(dir))

      assert %{health: :ok} = state
      assert [%{id: "aaaaaaaa", health: {:ok, pid}}] = Map.values(state.workers)
      assert Process.alive?(pid)
    end

    test "a worker whose directory cannot produce one leaves the foreman unhealthy", %{tmp_dir: dir} do
      start_supervised!(
        {DynamicSupervisor, strategy: :one_for_one, name: SpinUpTestCluster.otp_name(:worker_supervisor)}
      )

      write_worker(dir, "aaaaaaaa")
      # A manifest naming a module that is not a worker at all.
      File.mkdir_p!(Path.join(dir, "bbbbbbbb"))

      File.write!(
        Path.join(dir, "bbbbbbbb/manifest.json"),
        ~s({"id":"bbbbbbbb","cluster":"spin_up_test_cluster","worker":"Enum","params":{}})
      )

      assert %{health: health} = Impl.do_spin_up(base_state(dir))

      refute health == :ok, "a worker that cannot start must not be reported as healthy"
    end
  end
end
