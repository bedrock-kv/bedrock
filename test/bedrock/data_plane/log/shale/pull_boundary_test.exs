defmodule Bedrock.DataPlane.Log.Shale.PullBoundaryTest do
  use ExUnit.Case, async: false

  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.Log.Shale.Server
  alias Bedrock.DataPlane.Version

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    cluster = Bedrock.Cluster
    otp_name = :"boundary_test_#{System.unique_integer([:positive])}"
    id = "boundary_log_#{System.unique_integer([:positive])}"
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

  test "empty log pull boundary conditions work correctly", %{log: log} do
    # Test that various pull scenarios return correct semantics

    # 1. Pull from version 0 on empty log should return error immediately
    #    (version 0 == last_version, no transactions after it, no willing_to_wait)
    result = Log.pull(log, Version.from_integer(0), [])
    assert {:error, :version_too_new} = result

    # 2. Pull from version 0 should return empty immediately (no waiting)
    start_time = System.monotonic_time(:millisecond)
    result = Log.pull(log, Version.from_integer(0), timeout_in_ms: 100)
    end_time = System.monotonic_time(:millisecond)
    elapsed = end_time - start_time

    # Should return error immediately, not wait
    assert {:error, :version_too_new} = result
    assert elapsed < 50, "Should return immediately, but took #{elapsed}ms"

    # 3. Pull from higher version without timeout should return version_too_new
    result = Log.pull(log, Version.from_integer(1), [])
    assert {:error, :version_too_new} = result
  end

  test "log info works on empty log", %{log: log} do
    # Verify the log can provide info without crashing
    {:ok, info} = Log.info(log, [:last_version, :oldest_version])

    assert Map.has_key?(info, :last_version)
    assert Map.has_key?(info, :oldest_version)

    # Last version should be 0 for empty log
    assert info.last_version == <<0, 0, 0, 0, 0, 0, 0, 0>>
  end
end
