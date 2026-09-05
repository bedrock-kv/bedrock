defmodule Bedrock.Service.Foreman.WorkerEnumerationTest do
  use ExUnit.Case, async: true

  alias Bedrock.Service.Foreman.Impl
  alias Bedrock.Service.Foreman.StartingWorkers
  alias Bedrock.Service.Foreman.State

  defmodule EnumerationTestCluster do
    @moduledoc false
    def name, do: "enumeration_test_cluster"
    def otp_name_for_worker(id), do: :"enumeration_test_worker_#{id}"
    def otp_name(:worker_supervisor), do: :enumeration_test_worker_supervisor
    def otp_name(:link), do: :enumeration_test_link
    def otp_name(:foreman), do: :enumeration_test_foreman
  end

  # The foreman's path is not a private worker registry — the cluster
  # supervisor derives object_storage/ from the same :path it hands the
  # foreman, and the coordinator derives raft/ from the same base. Both
  # are permanent siblings of the worker directories, so "every entry
  # under path is a worker" is never true in a real deployment.
  @moduletag :tmp_dir

  defp worker_dir(dir, id, manifest_body) do
    path = Path.join(dir, id)
    :ok = File.mkdir_p(path)
    :ok = File.write(Path.join(path, "manifest.json"), manifest_body)
    path
  end

  defp bare_dir(dir, name) do
    path = Path.join(dir, name)
    :ok = File.mkdir_p(path)
    path
  end

  defp enumerate(dir), do: dir |> StartingWorkers.worker_paths_from_disk() |> Enum.sort()

  describe "worker_paths_from_disk/1" do
    test "returns worker directories", %{tmp_dir: dir} do
      a = worker_dir(dir, "aaaaaaaa", ~s({"id":"aaaaaaaa"}))
      b = worker_dir(dir, "bbbbbbbb", ~s({"id":"bbbbbbbb"}))

      assert enumerate(dir) == Enum.sort([a, b])
    end

    test "excludes the object_storage and raft siblings", %{tmp_dir: dir} do
      worker = worker_dir(dir, "aaaaaaaa", ~s({"id":"aaaaaaaa"}))
      bare_dir(dir, "object_storage")
      bare_dir(dir, "raft")

      assert enumerate(dir) == [worker]
    end

    test "excludes a directory with no manifest at all", %{tmp_dir: dir} do
      worker = worker_dir(dir, "aaaaaaaa", ~s({"id":"aaaaaaaa"}))
      bare_dir(dir, "ta2y6ro4")

      assert enumerate(dir) == [worker]
    end

    test "excludes stray files", %{tmp_dir: dir} do
      worker = worker_dir(dir, "aaaaaaaa", ~s({"id":"aaaaaaaa"}))
      :ok = File.write(Path.join(dir, "bootstrap"), "not a worker")

      assert enumerate(dir) == [worker]
    end

    # A present-but-unreadable manifest is a worker in trouble, not a
    # non-worker. Silently dropping it would hide a real failure, so it
    # must still be enumerated and surface downstream as :failed_to_start.
    test "still returns a worker whose manifest is corrupt", %{tmp_dir: dir} do
      corrupt = worker_dir(dir, "cccccccc", "{ this is not json")

      assert enumerate(dir) == [corrupt]
    end

    # Absence is the only thing that excludes. A stat that fails for any
    # other reason is ignorance, and a live worker holding a WAL must not
    # disappear from the foreman's view because of a permissions mistake.
    test "still returns a worker whose manifest cannot be stat'ed", %{tmp_dir: dir} do
      unreadable = worker_dir(dir, "dddddddd", ~s({"id":"dddddddd"}))
      on_exit(fn -> File.chmod(unreadable, 0o755) end)
      :ok = File.chmod(unreadable, 0o000)

      assert {:error, :eacces} = File.stat(Path.join(unreadable, "manifest.json"))
      assert enumerate(dir) == [unreadable]
    end

    test "leaves excluded directories on disk", %{tmp_dir: dir} do
      worker_dir(dir, "aaaaaaaa", ~s({"id":"aaaaaaaa"}))
      orphan = bare_dir(dir, "ta2y6ro4")
      storage = bare_dir(dir, "object_storage")

      _ = enumerate(dir)

      assert File.dir?(orphan), "enumeration must never delete; a WAL is real data"
      assert File.dir?(storage)
    end
  end

  describe "worker_info_from_path/2" do
    test "admits only real workers to the worker map", %{tmp_dir: dir} do
      worker_dir(dir, "aaaaaaaa", ~s({"id":"aaaaaaaa"}))
      bare_dir(dir, "object_storage")
      bare_dir(dir, "raft")
      bare_dir(dir, "ta2y6ro4")

      assert [%{id: "aaaaaaaa"}] = StartingWorkers.worker_info_from_path(dir, &:"worker_#{&1}")
    end
  end

  describe "abandoned_paths_from_disk/1" do
    test "reports a manifest-less directory", %{tmp_dir: dir} do
      worker_dir(dir, "aaaaaaaa", ~s({"id":"aaaaaaaa"}))
      orphan = bare_dir(dir, "ta2y6ro4")

      assert StartingWorkers.abandoned_paths_from_disk(dir) == [orphan]
    end

    test "says nothing about another component's directories", %{tmp_dir: dir} do
      worker_dir(dir, "aaaaaaaa", ~s({"id":"aaaaaaaa"}))
      bare_dir(dir, "object_storage")
      bare_dir(dir, "raft")

      assert StartingWorkers.abandoned_paths_from_disk(dir) == []
    end

    test "says nothing about a healthy directory of workers", %{tmp_dir: dir} do
      worker_dir(dir, "aaaaaaaa", ~s({"id":"aaaaaaaa"}))
      worker_dir(dir, "bbbbbbbb", ~s({"id":"bbbbbbbb"}))

      assert StartingWorkers.abandoned_paths_from_disk(dir) == []
    end

    test "does not report a worker whose manifest is merely corrupt", %{tmp_dir: dir} do
      worker_dir(dir, "cccccccc", "{ this is not json")

      assert StartingWorkers.abandoned_paths_from_disk(dir) == []
    end

    test "does not report stray files", %{tmp_dir: dir} do
      :ok = File.write(Path.join(dir, "bootstrap"), "not a worker")

      assert StartingWorkers.abandoned_paths_from_disk(dir) == []
    end

    test "reports every orphan, sorted", %{tmp_dir: dir} do
      a = bare_dir(dir, "aflw25ra")
      d = bare_dir(dir, "d4if54s2")
      t = bare_dir(dir, "ta2y6ro4")

      assert StartingWorkers.abandoned_paths_from_disk(dir) == Enum.sort([a, d, t])
    end
  end

  # The report has to reach an operator at the moment one is looking, and
  # boot is that moment: an unstartable directory produces no worker, so
  # it has no other voice anywhere in the system.
  describe "spin-up reporting" do
    defp spin_up(dir) do
      state = %State{
        cluster: EnumerationTestCluster,
        capabilities: [:log],
        health: :ok,
        otp_name: :enumeration_test_foreman,
        path: dir,
        workers: %{}
      }

      ExUnit.CaptureLog.capture_log(fn -> Impl.do_spin_up(state) end)
    end

    test "names the abandoned directories at boot", %{tmp_dir: dir} do
      bare_dir(dir, "ta2y6ro4")
      bare_dir(dir, "aflw25ra")

      log = spin_up(dir)

      assert log =~ "abandoned working directories"
      assert log =~ "ta2y6ro4"
      assert log =~ "aflw25ra"
      assert log =~ dir
    end

    test "stays quiet when there is nothing to report", %{tmp_dir: dir} do
      worker_dir(dir, "aaaaaaaa", ~s({"id":"aaaaaaaa"}))
      bare_dir(dir, "object_storage")
      bare_dir(dir, "raft")

      refute spin_up(dir) =~ "abandoned working"
    end
  end
end
