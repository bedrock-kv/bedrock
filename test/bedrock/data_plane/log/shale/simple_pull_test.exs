defmodule Bedrock.DataPlane.Log.Shale.SimplePullTest do
  use ExUnit.Case, async: false

  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.Log.Shale
  alias Bedrock.DataPlane.Log.Shale.Server
  alias Bedrock.DataPlane.Version
  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.LocalFilesystem
  alias Bedrock.Test.RecoveryAuthorityTestSupport

  @moduletag :tmp_dir
  @authority %{generation: 1, recovery_id: "simple-pull"}

  setup %{tmp_dir: tmp_dir} do
    cluster = RecoveryAuthorityTestSupport.TestCluster
    otp_name = :"simple_test_#{System.unique_integer([:positive])}"
    id = "simple_log_#{System.unique_integer([:positive])}"
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

  test "server starts and can respond to info", %{log: log} do
    # Just test that the server is alive and responsive
    assert {:ok, %{last_version: _}} = Log.info(log, [:last_version])
  end

  test "boundary condition test - pull at exactly last_version", %{log: log} do
    # Get the initial last_version
    {:ok, initial_info} = Log.info(log, [:last_version])
    last_version = initial_info.last_version

    # Pull from current last_version should return empty immediately (correct semantics)
    start_time = System.monotonic_time(:millisecond)
    result = Log.pull(log, last_version, timeout_in_ms: 100)
    end_time = System.monotonic_time(:millisecond)
    elapsed = end_time - start_time

    # Should return error immediately when no willing_to_wait specified (correct semantics)
    assert {:error, :version_too_new} = result
    assert elapsed < 50, "Should return immediately, but took #{elapsed}ms"
  end

  test "crash fix verification - no crash on empty log", %{log: log} do
    # This test verifies that empty log pulls don't crash
    # Previously this would crash with KeyError when active_segment was nil

    # Both pull scenarios should return error immediately when no willing_to_wait
    version_0 = Version.from_integer(0)
    assert {:error, :version_too_new} = Log.pull(log, version_0, timeout_in_ms: 100)
    assert {:error, :version_too_new} = Log.pull(log, version_0, [])

    # The key verification is that we reach here without crashing
  end
end
