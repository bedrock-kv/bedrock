defmodule Bedrock.DataPlane.Log.Shale.PullBoundaryTest do
  use ExUnit.Case, async: false

  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.Log.Shale
  alias Bedrock.DataPlane.Log.Shale.Server
  alias Bedrock.DataPlane.Version
  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.LocalFilesystem
  alias Bedrock.Test.RecoveryAuthorityTestSupport

  @moduletag :tmp_dir
  @authority %{generation: 1, recovery_id: "pull-boundary"}

  setup %{tmp_dir: tmp_dir} do
    cluster = RecoveryAuthorityTestSupport.TestCluster
    otp_name = :"boundary_test_#{System.unique_integer([:positive])}"
    id = "boundary_log_#{System.unique_integer([:positive])}"
    foreman = self()
    path = Path.join(tmp_dir, "log_segments")
    object_storage = ObjectStorage.backend(LocalFilesystem, root: Path.join(tmp_dir, "object_storage"))

    File.mkdir_p!(path)
    RecoveryAuthorityTestSupport.prepare_worker!(path, id, Shale, cluster: cluster)

    # Start the Shale server
    {:ok, pid} =
      [cluster: cluster, otp_name: otp_name, id: id, foreman: foreman, path: path, object_storage: object_storage]
      |> Server.child_spec()
      |> then(fn %{start: {GenServer, :start_link, [module, args, opts]}} ->
        GenServer.start_link(module, args, opts)
      end)

    {:ok, ^pid, _} = Log.lock_for_recovery(pid, @authority)
    zero = Version.zero()
    {:ok, ^pid} = Log.recover_from(pid, @authority, [], zero, zero)
    :ok = Log.unlock_after_recovery(pid, @authority)

    on_exit(fn ->
      if Process.alive?(pid) do
        GenServer.stop(pid)
      end
    end)

    {:ok, log: pid, path: path}
  end

  describe "empty log pull boundary conditions" do
    test "pull from version 0 returns error immediately without timeout", %{log: log} do
      # Pull from version 0 on empty log should return error immediately
      # (version 0 == last_version, no transactions after it, no willing_to_wait)
      assert {:error, :version_too_new} = Log.pull(log, Version.from_integer(0), [])
    end

    test "pull from version 0 with timeout returns error immediately without waiting", %{log: log} do
      start_time = System.monotonic_time(:millisecond)
      assert {:error, :version_too_new} = Log.pull(log, Version.from_integer(0), timeout_in_ms: 100)
      elapsed = System.monotonic_time(:millisecond) - start_time

      assert elapsed < 50, "Should return immediately, but took #{elapsed}ms"
    end

    test "pull from higher version returns version_too_new", %{log: log} do
      assert {:error, :version_too_new} = Log.pull(log, Version.from_integer(1), [])
    end
  end

  describe "log info/2" do
    test "returns expected structure on empty log", %{log: log} do
      # Verify the log can provide info without crashing and returns expected structure
      assert {:ok, %{last_version: <<0, 0, 0, 0, 0, 0, 0, 0>>, oldest_version: _}} =
               Log.info(log, [:last_version, :oldest_version])
    end
  end
end
