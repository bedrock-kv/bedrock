defmodule Bedrock.Service.Foreman.StateTest do
  use ExUnit.Case, async: true

  alias Bedrock.Service.Foreman.State
  alias Bedrock.Service.Foreman.WorkerInfo

  describe "new_state/1" do
    test "creates a new state with required params" do
      # Simple cluster identifier
      cluster = :test_cluster
      object_storage = {Bedrock.ObjectStorage.LocalFilesystem, root: "/tmp/object_storage"}

      params = %{
        cluster: cluster,
        capabilities: [:log, :materializer],
        path: "/tmp/test",
        otp_name: :test_foreman,
        object_storage: object_storage
      }

      assert {:ok, state} = State.new_state(params)
      assert state.cluster == cluster
      assert state.capabilities == [:log, :materializer]
      assert state.path == "/tmp/test"
      assert state.otp_name == :test_foreman
      assert state.object_storage == object_storage
      assert state.health == :starting
      assert state.waiting_for_healthy == []
      assert state.workers == %{}
    end

    test "returns error when missing required params" do
      assert {:error, :missing_required_params} = State.new_state(%{})
      assert {:error, :missing_required_params} = State.new_state(%{cluster: nil})
      assert {:error, :missing_required_params} = State.new_state(nil)
    end
  end

  describe "update_workers/2" do
    test "updates workers using an updater function" do
      state = %State{workers: %{}}

      worker_info = %WorkerInfo{id: "worker1", path: "/tmp/worker1", health: :starting}

      updated =
        State.update_workers(state, fn workers ->
          Map.put(workers, "worker1", worker_info)
        end)

      assert Map.has_key?(updated.workers, "worker1")
    end
  end

  describe "update_health/2" do
    test "updates health using an updater function" do
      state = %State{health: :starting}

      updated = State.update_health(state, fn _health -> :ok end)

      assert updated.health == :ok
    end
  end

  describe "put_health/2" do
    test "sets health to a new value" do
      state = %State{health: :starting}

      updated = State.put_health(state, :ok)
      assert updated.health == :ok

      updated = State.put_health(updated, :error)
      assert updated.health == :error
    end
  end

  describe "update_waiting_for_healthy/2" do
    test "updates waiting list using an updater function" do
      state = %State{waiting_for_healthy: []}

      updated =
        State.update_waiting_for_healthy(state, fn list ->
          [self() | list]
        end)

      assert self() in updated.waiting_for_healthy
    end
  end

  describe "put_waiting_for_healthy/2" do
    test "sets the waiting for healthy list" do
      state = %State{waiting_for_healthy: []}
      pids = [self(), spawn(fn -> :ok end)]

      updated = State.put_waiting_for_healthy(state, pids)

      assert updated.waiting_for_healthy == pids
    end
  end

  describe "put_health_for_worker/3" do
    test "updates health for a specific worker" do
      worker_id = "test_worker"
      worker_info = %WorkerInfo{id: worker_id, path: "/tmp/worker", health: :starting}
      state = %State{workers: %{worker_id => worker_info}}

      updated = State.put_health_for_worker(state, worker_id, :ok)

      assert updated.workers[worker_id].health == :ok
    end
  end
end
