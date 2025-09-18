defmodule Bedrock.Service.ForemanTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Log.Shale
  alias Bedrock.DataPlane.Storage.Basalt
  alias Bedrock.Service.Foreman.Impl
  alias Bedrock.Service.Foreman.State
  alias Bedrock.Service.Foreman.WorkerInfo
  alias Bedrock.Service.Manifest

  # Test setup helpers
  defp base_state(overrides \\ %{}) do
    Map.merge(
      %State{
        cluster: TestCluster,
        capabilities: [:storage, :log],
        health: :ok,
        otp_name: :test_foreman,
        path: "/tmp/test",
        waiting_for_healthy: [],
        workers: %{}
      },
      overrides
    )
  end

  defp storage_manifest(id) do
    %Manifest{
      cluster: "test_cluster",
      id: id,
      worker: Basalt,
      params: %{}
    }
  end

  defp log_manifest(id) do
    %Manifest{
      cluster: "test_cluster",
      id: id,
      worker: Shale,
      params: %{}
    }
  end

  defp worker_info(id, opts) when is_list(opts) do
    manifest = opts[:manifest]
    health = opts[:health] || {:ok, self()}
    otp_name = opts[:otp_name] || String.to_atom("#{id}_worker")

    %WorkerInfo{
      id: id,
      path: "/tmp/test/#{id}",
      health: health,
      manifest: manifest,
      otp_name: otp_name
    }
  end

  describe "do_fetch_storage_workers/1" do
    test "returns empty list when no workers exist" do
      state = base_state()
      assert [] = Impl.do_fetch_storage_workers(state)
    end

    test "returns only storage workers when mixed workers exist" do
      state =
        base_state(%{
          workers: %{
            "storage_1" => worker_info("storage_1", manifest: storage_manifest("storage_1")),
            "log_1" => worker_info("log_1", manifest: log_manifest("log_1")),
            "storage_2" => worker_info("storage_2", manifest: storage_manifest("storage_2"))
          }
        })

      result = Impl.do_fetch_storage_workers(state)
      assert length(result) == 2
      assert :storage_1_worker in result
      assert :storage_2_worker in result
      refute :log_1_worker in result
    end

    test "handles workers with nil manifest gracefully" do
      state =
        base_state(%{
          workers: %{
            "broken_worker" =>
              worker_info("broken_worker",
                manifest: nil,
                health: :stopped,
                otp_name: :broken_worker
              )
          }
        })

      assert [] = Impl.do_fetch_storage_workers(state)
    end

    test "handles workers with manifest but nil worker module" do
      incomplete_manifest = %Manifest{
        cluster: "test_cluster",
        id: "incomplete_worker",
        worker: nil,
        params: %{}
      }

      state =
        base_state(%{
          workers: %{
            "incomplete_worker" =>
              worker_info("incomplete_worker",
                manifest: incomplete_manifest,
                health: :stopped,
                otp_name: :incomplete_worker
              )
          }
        })

      assert [] = Impl.do_fetch_storage_workers(state)
    end
  end

  describe "storage_worker?/1" do
    test "returns true for storage worker" do
      worker = worker_info("storage_1", manifest: storage_manifest("storage_1"))
      assert Impl.storage_worker?(worker)
    end

    test "returns false for log worker" do
      worker = worker_info("log_1", manifest: log_manifest("log_1"))
      refute Impl.storage_worker?(worker)
    end

    test "returns false for invalid manifests" do
      nil_manifest_worker = worker_info("broken", manifest: nil, health: :stopped)

      nil_worker_manifest =
        worker_info("incomplete",
          manifest: %Manifest{cluster: "test", id: "incomplete", worker: nil, params: %{}},
          health: :stopped
        )

      refute Impl.storage_worker?(nil_manifest_worker)
      refute Impl.storage_worker?(nil_worker_manifest)
    end
  end

  describe "do_wait_for_healthy/2" do
    test "returns :ok when foreman is already healthy" do
      state = base_state()
      assert :ok = Impl.do_wait_for_healthy(state, self())
    end

    test "adds caller to waiting list when not healthy" do
      caller_pid = self()
      state = base_state(%{health: :starting})

      assert %State{
               waiting_for_healthy: [^caller_pid],
               health: :starting
             } = Impl.do_wait_for_healthy(state, caller_pid)
    end
  end

  describe "do_get_all_running_services/1" do
    test "returns empty list when no workers exist" do
      state = base_state()
      assert [] = Impl.do_get_all_running_services(state)
    end

    test "returns all healthy workers with correct service info format" do
      state =
        base_state(%{
          workers: %{
            "storage_1" => worker_info("storage_1", manifest: storage_manifest("storage_1")),
            "log_1" => worker_info("log_1", manifest: log_manifest("log_1"))
          }
        })

      result = Impl.do_get_all_running_services(state)
      assert length(result) == 2
      assert {"storage_1", :storage, :storage_1_worker} in result
      assert {"log_1", :log, :log_1_worker} in result
    end

    test "excludes unhealthy workers" do
      state =
        base_state(%{
          workers: %{
            "storage_1" => worker_info("storage_1", manifest: storage_manifest("storage_1")),
            "log_1" => worker_info("log_1", manifest: log_manifest("log_1"), health: :stopped),
            "storage_2" =>
              worker_info("storage_2",
                manifest: storage_manifest("storage_2"),
                health: {:failed_to_start, :timeout}
              )
          }
        })

      assert [{"storage_1", :storage, :storage_1_worker}] = Impl.do_get_all_running_services(state)
    end

    test "filters out invalid workers (nil manifest or unhealthy)" do
      state =
        base_state(%{
          workers: %{
            "storage_1" => worker_info("storage_1", manifest: storage_manifest("storage_1")),
            "broken_worker" => worker_info("broken_worker", manifest: nil),
            "unhealthy_worker" =>
              worker_info("unhealthy_worker",
                manifest: storage_manifest("unhealthy_worker"),
                health: :stopped
              )
          }
        })

      assert [{"storage_1", :storage, :storage_1_worker}] = Impl.do_get_all_running_services(state)
    end
  end

  describe "service info extraction functions" do
    test "compact_service_info_from_worker_info/1 extracts compact format" do
      storage_worker = worker_info("storage_1", manifest: storage_manifest("storage_1"))
      log_worker = worker_info("log_1", manifest: log_manifest("log_1"))

      assert {"storage_1", :storage, :storage_1_worker} =
               Impl.compact_service_info_from_worker_info(storage_worker)

      assert {"log_1", :log, :log_1_worker} =
               Impl.compact_service_info_from_worker_info(log_worker)
    end

    test "service_info_from_worker_info/1 extracts full format" do
      storage_worker = worker_info("storage_1", manifest: storage_manifest("storage_1"))
      log_worker = worker_info("log_1", manifest: log_manifest("log_1"))
      current_node = Node.self()

      assert {"storage_1", :storage, {:storage_1_worker, ^current_node}} =
               Impl.service_info_from_worker_info(storage_worker)

      assert {"log_1", :log, {:log_1_worker, ^current_node}} =
               Impl.service_info_from_worker_info(log_worker)
    end
  end

  describe "worker_healthy?/1" do
    test "correctly identifies worker health status" do
      healthy_worker = worker_info("test", manifest: nil, health: {:ok, self()})
      stopped_worker = worker_info("test", manifest: nil, health: :stopped)
      failed_worker = worker_info("test", manifest: nil, health: {:failed_to_start, :timeout})

      assert Impl.worker_healthy?(healthy_worker)
      refute Impl.worker_healthy?(stopped_worker)
      refute Impl.worker_healthy?(failed_worker)
    end
  end

  describe "integration with WorkerBehaviour" do
    test "storage worker module returns correct kind" do
      assert Basalt.kind() == :storage
    end

    test "log worker module returns correct kind" do
      assert Shale.kind() == :log
    end
  end
end
