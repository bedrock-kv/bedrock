defmodule Bedrock.Service.Foreman.WorkerInfoTest do
  use ExUnit.Case, async: true

  alias Bedrock.Service.Foreman.WorkerInfo
  alias Bedrock.Service.Manifest

  describe "put_health/2" do
    test "sets the health field" do
      worker_info = %WorkerInfo{id: "test", path: "/tmp", health: :stopped}

      updated = WorkerInfo.put_health(worker_info, {:ok, self()})

      assert updated.health == {:ok, self()}
    end

    test "can set health to stopped" do
      worker_info = %WorkerInfo{id: "test", path: "/tmp", health: {:ok, self()}}

      updated = WorkerInfo.put_health(worker_info, :stopped)

      assert updated.health == :stopped
    end

    test "can set health to failed_to_start" do
      worker_info = %WorkerInfo{id: "test", path: "/tmp", health: :stopped}

      updated = WorkerInfo.put_health(worker_info, {:failed_to_start, :timeout})

      assert updated.health == {:failed_to_start, :timeout}
    end
  end

  describe "put_manifest/2" do
    test "sets the manifest field" do
      worker_info = %WorkerInfo{id: "test", path: "/tmp", health: :stopped}

      manifest = %Manifest{
        cluster: :test_cluster,
        id: "test_log",
        worker: SomeWorker,
        params: %{}
      }

      updated = WorkerInfo.put_manifest(worker_info, manifest)

      assert updated.manifest == manifest
    end

    test "replaces existing manifest" do
      old_manifest = %Manifest{cluster: :test, id: "old", worker: OldWorker, params: %{}}
      new_manifest = %Manifest{cluster: :test, id: "new", worker: NewWorker, params: %{}}

      worker_info = %WorkerInfo{
        id: "test",
        path: "/tmp",
        health: :stopped,
        manifest: old_manifest
      }

      updated = WorkerInfo.put_manifest(worker_info, new_manifest)

      assert updated.manifest == new_manifest
      refute updated.manifest == old_manifest
    end
  end

  describe "put_otp_name/2" do
    test "sets the otp_name field" do
      worker_info = %WorkerInfo{id: "test", path: "/tmp", health: :stopped}

      updated = WorkerInfo.put_otp_name(worker_info, :my_worker)

      assert updated.otp_name == :my_worker
    end

    test "replaces existing otp_name" do
      worker_info = %WorkerInfo{
        id: "test",
        path: "/tmp",
        health: :stopped,
        otp_name: :old_name
      }

      updated = WorkerInfo.put_otp_name(worker_info, :new_name)

      assert updated.otp_name == :new_name
    end
  end
end
