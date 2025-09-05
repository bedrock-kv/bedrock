defmodule Bedrock.DataPlane.Log.Shale.SimplePullTest do
  use ExUnit.Case, async: false

  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.Log.Shale.Server
  alias Bedrock.DataPlane.Version

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    cluster = Bedrock.Cluster
    otp_name = :"simple_test_#{System.unique_integer([:positive])}"
    id = "simple_log_#{System.unique_integer([:positive])}"
    foreman = self()
    path = Path.join(tmp_dir, "log_segments")

    File.mkdir_p!(path)

    # Start the Shale server
    {:ok, pid} =
      GenServer.start_link(
        Server,
        {cluster, otp_name, id, foreman, path, true},
        name: otp_name
      )

    # Wait for initialization to complete
    Process.sleep(10)

    on_exit(fn ->
      if Process.alive?(pid) do
        GenServer.stop(pid)
      end
    end)

    {:ok, log: pid, path: path}
  end

  test "server starts and can respond to info", %{log: log} do
    # Just test that the server is alive and responsive
    result = Log.info(log, [:last_version])
    assert {:ok, info} = result
    assert Map.has_key?(info, :last_version)
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

    # Pull from version 0 should return error immediately when no willing_to_wait
    result = Log.pull(log, Version.from_integer(0), timeout_in_ms: 100)
    assert {:error, :version_too_new} = result

    # Pull with no timeout should also return error immediately
    result = Log.pull(log, Version.from_integer(0), [])
    assert {:error, :version_too_new} = result

    # The key thing is we get here without crashing
    assert true
  end
end
