defmodule Bedrock.DataPlane.SimpleEndToEndTest do
  use ExUnit.Case, async: false

  alias Bedrock.Cluster
  alias Bedrock.Worker
  alias Bedrock.DataPlane.LogSystem

  defmodule TestCluster do
    use Bedrock.Cluster,
      otp_app: :bedrock,
      name: "test"
  end

  defmodule TestWorker do
    use Bedrock.Worker,
      cluster: TestCluster
  end

  describe "using the TestWorker configured by macro" do
    test "fails to start if storage is specified as a service, and no path is configured" do
      assert_raise RuntimeError, fn ->
        start_supervised!({TestWorker, services: [:storage]})
      end
    end

    @tag :tmp_dir
    test "starts when storage is specified as a service, and a path is configured", %{
      tmp_dir: tmp_dir
    } do
      start_supervised!({TestWorker, services: [:storage], storage: [path: tmp_dir]})

      assert is_pid(Process.whereis(TestCluster.otp_name(:storage_system)))
    end

    test "fails to start if log_system is specified as a service, and no path is configured" do
      assert_raise RuntimeError, fn ->
        start_supervised!({TestWorker, services: [:log_system]})
      end
    end

    @tag :tmp_dir
    test "starts when log_system is specified as a service, and a path is configured", %{
      tmp_dir: tmp_dir
    } do
      _pid =
        start_supervised!({TestWorker, services: [:log_system], log_system: [path: tmp_dir]})

      assert is_pid(Process.whereis(TestCluster.otp_name(:log_system)))

      assert :ok = LogSystem.wait_for_healthy(TestCluster, 10_000)
    end
  end
end
