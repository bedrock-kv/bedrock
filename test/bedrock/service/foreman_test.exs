defmodule Bedrock.Service.ForemanTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Log.Shale
  alias Bedrock.DataPlane.Storage.Basalt
  alias Bedrock.Service.Foreman.Impl
  alias Bedrock.Service.Foreman.State
  alias Bedrock.Service.Foreman.WorkerInfo
  alias Bedrock.Service.Manifest

  describe "storage_workers/2" do
    test "returns empty list when no workers exist" do
      state = %State{
        cluster: TestCluster,
        capabilities: [:storage, :log],
        health: :ok,
        otp_name: :test_foreman,
        path: "/tmp/test",
        waiting_for_healthy: [],
        workers: %{}
      }

      result = Impl.do_fetch_storage_workers(state)
      assert result == []
    end

    test "returns only storage workers when mixed workers exist" do
      storage_manifest = %Manifest{
        cluster: "test_cluster",
        id: "storage_1",
        worker: Basalt,
        params: %{}
      }

      log_manifest = %Manifest{
        cluster: "test_cluster",
        id: "log_1",
        worker: Shale,
        params: %{}
      }

      state = %State{
        cluster: TestCluster,
        capabilities: [:storage, :log],
        health: :ok,
        otp_name: :test_foreman,
        path: "/tmp/test",
        waiting_for_healthy: [],
        workers: %{
          "storage_1" => %WorkerInfo{
            id: "storage_1",
            path: "/tmp/test/storage_1",
            health: {:ok, self()},
            manifest: storage_manifest,
            otp_name: :storage_1_worker
          },
          "log_1" => %WorkerInfo{
            id: "log_1",
            path: "/tmp/test/log_1",
            health: {:ok, self()},
            manifest: log_manifest,
            otp_name: :log_1_worker
          },
          "storage_2" => %WorkerInfo{
            id: "storage_2",
            path: "/tmp/test/storage_2",
            health: {:ok, self()},
            manifest: %{storage_manifest | id: "storage_2"},
            otp_name: :storage_2_worker
          }
        }
      }

      result = Impl.do_fetch_storage_workers(state)
      assert length(result) == 2
      assert :storage_1_worker in result
      assert :storage_2_worker in result
      refute :log_1_worker in result
    end

    test "handles workers with nil manifest gracefully" do
      state = %State{
        cluster: TestCluster,
        capabilities: [:storage, :log],
        health: :ok,
        otp_name: :test_foreman,
        path: "/tmp/test",
        waiting_for_healthy: [],
        workers: %{
          "broken_worker" => %WorkerInfo{
            id: "broken_worker",
            path: "/tmp/test/broken",
            health: :stopped,
            manifest: nil,
            otp_name: :broken_worker
          }
        }
      }

      result = Impl.do_fetch_storage_workers(state)
      assert result == []
    end

    test "handles workers with manifest but nil worker module" do
      state = %State{
        cluster: TestCluster,
        capabilities: [:storage, :log],
        health: :ok,
        otp_name: :test_foreman,
        path: "/tmp/test",
        waiting_for_healthy: [],
        workers: %{
          "incomplete_worker" => %WorkerInfo{
            id: "incomplete_worker",
            path: "/tmp/test/incomplete",
            health: :stopped,
            manifest: %Manifest{
              cluster: "test_cluster",
              id: "incomplete_worker",
              worker: nil,
              params: %{}
            },
            otp_name: :incomplete_worker
          }
        }
      }

      result = Impl.do_fetch_storage_workers(state)
      assert result == []
    end
  end

  describe "storage_worker?/1 helper function" do
    test "returns true for storage worker" do
      storage_manifest = %Manifest{
        cluster: "test_cluster",
        id: "storage_1",
        worker: Basalt,
        params: %{}
      }

      worker_info = %WorkerInfo{
        id: "storage_1",
        path: "/tmp/test/storage_1",
        health: {:ok, self()},
        manifest: storage_manifest,
        otp_name: :storage_1_worker
      }

      assert Impl.storage_worker?(worker_info) == true
    end

    test "returns false for log worker" do
      log_manifest = %Manifest{
        cluster: "test_cluster",
        id: "log_1",
        worker: Shale,
        params: %{}
      }

      worker_info = %WorkerInfo{
        id: "log_1",
        path: "/tmp/test/log_1",
        health: {:ok, self()},
        manifest: log_manifest,
        otp_name: :log_1_worker
      }

      assert Impl.storage_worker?(worker_info) == false
    end

    test "returns false for worker with nil manifest" do
      worker_info = %WorkerInfo{
        id: "broken_worker",
        path: "/tmp/test/broken",
        health: :stopped,
        manifest: nil,
        otp_name: :broken_worker
      }

      assert Impl.storage_worker?(worker_info) == false
    end

    test "returns false for worker with nil worker module" do
      manifest = %Manifest{
        cluster: "test_cluster",
        id: "incomplete_worker",
        worker: nil,
        params: %{}
      }

      worker_info = %WorkerInfo{
        id: "incomplete_worker",
        path: "/tmp/test/incomplete",
        health: :stopped,
        manifest: manifest,
        otp_name: :incomplete_worker
      }

      assert Impl.storage_worker?(worker_info) == false
    end
  end

  describe "wait_for_healthy functionality" do
    test "wait_for_healthy returns :ok when foreman is already healthy" do
      state = %State{
        cluster: TestCluster,
        capabilities: [:storage, :log],
        health: :ok,
        otp_name: :test_foreman,
        path: "/tmp/test",
        waiting_for_healthy: [],
        workers: %{}
      }

      result = Impl.do_wait_for_healthy(state, self())
      assert result == :ok
    end

    test "wait_for_healthy adds caller to waiting list when not healthy" do
      state = %State{
        cluster: TestCluster,
        capabilities: [:storage, :log],
        health: :starting,
        otp_name: :test_foreman,
        path: "/tmp/test",
        waiting_for_healthy: [],
        workers: %{}
      }

      caller_pid = self()
      result = Impl.do_wait_for_healthy(state, caller_pid)

      assert result.waiting_for_healthy == [caller_pid]
      assert result.health == :starting
    end
  end

  describe "get_all_running_services/1" do
    test "returns empty list when no workers exist" do
      state = %State{
        cluster: TestCluster,
        capabilities: [:storage, :log],
        health: :ok,
        otp_name: :test_foreman,
        path: "/tmp/test",
        waiting_for_healthy: [],
        workers: %{}
      }

      result = Impl.do_get_all_running_services(state)
      assert result == []
    end

    test "returns all healthy workers with correct service info format" do
      storage_manifest = %Manifest{
        cluster: "test_cluster",
        id: "storage_1",
        worker: Basalt,
        params: %{}
      }

      log_manifest = %Manifest{
        cluster: "test_cluster",
        id: "log_1",
        worker: Shale,
        params: %{}
      }

      state = %State{
        cluster: TestCluster,
        capabilities: [:storage, :log],
        health: :ok,
        otp_name: :test_foreman,
        path: "/tmp/test",
        waiting_for_healthy: [],
        workers: %{
          "storage_1" => %WorkerInfo{
            id: "storage_1",
            path: "/tmp/test/storage_1",
            health: {:ok, self()},
            manifest: storage_manifest,
            otp_name: :storage_1_worker
          },
          "log_1" => %WorkerInfo{
            id: "log_1",
            path: "/tmp/test/log_1",
            health: {:ok, self()},
            manifest: log_manifest,
            otp_name: :log_1_worker
          }
        }
      }

      result = Impl.do_get_all_running_services(state)

      assert length(result) == 2

      # Check storage service info
      storage_service = Enum.find(result, fn {id, _kind, _ref} -> id == "storage_1" end)
      assert storage_service == {"storage_1", :storage, {:storage_1_worker, Node.self()}}

      # Check log service info
      log_service = Enum.find(result, fn {id, _kind, _ref} -> id == "log_1" end)
      assert log_service == {"log_1", :log, {:log_1_worker, Node.self()}}
    end

    test "excludes unhealthy workers" do
      storage_manifest = %Manifest{
        cluster: "test_cluster",
        id: "storage_1",
        worker: Basalt,
        params: %{}
      }

      log_manifest = %Manifest{
        cluster: "test_cluster",
        id: "log_1",
        worker: Shale,
        params: %{}
      }

      state = %State{
        cluster: TestCluster,
        capabilities: [:storage, :log],
        health: :ok,
        otp_name: :test_foreman,
        path: "/tmp/test",
        waiting_for_healthy: [],
        workers: %{
          "storage_1" => %WorkerInfo{
            id: "storage_1",
            path: "/tmp/test/storage_1",
            health: {:ok, self()},
            manifest: storage_manifest,
            otp_name: :storage_1_worker
          },
          "log_1" => %WorkerInfo{
            id: "log_1",
            path: "/tmp/test/log_1",
            health: :stopped,
            manifest: log_manifest,
            otp_name: :log_1_worker
          },
          "storage_2" => %WorkerInfo{
            id: "storage_2",
            path: "/tmp/test/storage_2",
            health: {:failed_to_start, :timeout},
            manifest: %{storage_manifest | id: "storage_2"},
            otp_name: :storage_2_worker
          }
        }
      }

      result = Impl.do_get_all_running_services(state)

      assert length(result) == 1
      assert result == [{"storage_1", :storage, {:storage_1_worker, Node.self()}}]
    end

    test "handles workers with nil manifest gracefully" do
      storage_manifest = %Manifest{
        cluster: "test_cluster",
        id: "storage_1",
        worker: Basalt,
        params: %{}
      }

      state = %State{
        cluster: TestCluster,
        capabilities: [:storage, :log],
        health: :ok,
        otp_name: :test_foreman,
        path: "/tmp/test",
        waiting_for_healthy: [],
        workers: %{
          "storage_1" => %WorkerInfo{
            id: "storage_1",
            path: "/tmp/test/storage_1",
            health: {:ok, self()},
            manifest: storage_manifest,
            otp_name: :storage_1_worker
          },
          "broken_worker" => %WorkerInfo{
            id: "broken_worker",
            path: "/tmp/test/broken",
            health: {:ok, self()},
            manifest: nil,
            otp_name: :broken_worker
          }
        }
      }

      # Should exclude workers with nil manifest and only return valid ones
      result = Impl.do_get_all_running_services(state)
      assert length(result) == 1
      assert result == [{"storage_1", :storage, {:storage_1_worker, Node.self()}}]
    end

    test "only includes workers that are both healthy and have valid manifests" do
      storage_manifest = %Manifest{
        cluster: "test_cluster",
        id: "storage_1",
        worker: Basalt,
        params: %{}
      }

      state = %State{
        cluster: TestCluster,
        capabilities: [:storage, :log],
        health: :ok,
        otp_name: :test_foreman,
        path: "/tmp/test",
        waiting_for_healthy: [],
        workers: %{
          "storage_1" => %WorkerInfo{
            id: "storage_1",
            path: "/tmp/test/storage_1",
            health: {:ok, self()},
            manifest: storage_manifest,
            otp_name: :storage_1_worker
          },
          "unhealthy_worker" => %WorkerInfo{
            id: "unhealthy_worker",
            path: "/tmp/test/unhealthy",
            health: :stopped,
            manifest: %{storage_manifest | id: "unhealthy_worker"},
            otp_name: :unhealthy_worker
          }
        }
      }

      result = Impl.do_get_all_running_services(state)
      assert length(result) == 1
      assert result == [{"storage_1", :storage, {:storage_1_worker, Node.self()}}]
    end
  end

  describe "service_info_from_worker_info/1 helper function" do
    test "extracts correct service info from storage worker" do
      storage_manifest = %Manifest{
        cluster: "test_cluster",
        id: "storage_1",
        worker: Basalt,
        params: %{}
      }

      worker_info = %WorkerInfo{
        id: "storage_1",
        path: "/tmp/test/storage_1",
        health: {:ok, self()},
        manifest: storage_manifest,
        otp_name: :storage_1_worker
      }

      result = Impl.service_info_from_worker_info(worker_info)
      assert result == {"storage_1", :storage, {:storage_1_worker, Node.self()}}
    end

    test "extracts correct service info from log worker" do
      log_manifest = %Manifest{
        cluster: "test_cluster",
        id: "log_1",
        worker: Shale,
        params: %{}
      }

      worker_info = %WorkerInfo{
        id: "log_1",
        path: "/tmp/test/log_1",
        health: {:ok, self()},
        manifest: log_manifest,
        otp_name: :log_1_worker
      }

      result = Impl.service_info_from_worker_info(worker_info)
      assert result == {"log_1", :log, {:log_1_worker, Node.self()}}
    end
  end

  describe "worker_healthy?/1 helper function" do
    test "returns true for healthy worker" do
      worker_info = %WorkerInfo{
        id: "test_worker",
        path: "/tmp/test",
        health: {:ok, self()},
        manifest: nil,
        otp_name: :test_worker
      }

      assert Impl.worker_healthy?(worker_info) == true
    end

    test "returns false for stopped worker" do
      worker_info = %WorkerInfo{
        id: "test_worker",
        path: "/tmp/test",
        health: :stopped,
        manifest: nil,
        otp_name: :test_worker
      }

      assert Impl.worker_healthy?(worker_info) == false
    end

    test "returns false for failed to start worker" do
      worker_info = %WorkerInfo{
        id: "test_worker",
        path: "/tmp/test",
        health: {:failed_to_start, :timeout},
        manifest: nil,
        otp_name: :test_worker
      }

      assert Impl.worker_healthy?(worker_info) == false
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
