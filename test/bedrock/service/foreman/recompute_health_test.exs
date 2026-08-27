defmodule Bedrock.Service.Foreman.RecomputeHealthTest do
  use ExUnit.Case, async: true

  alias Bedrock.Service.Foreman.Impl
  alias Bedrock.Service.Foreman.State
  alias Bedrock.Service.Foreman.WorkerInfo

  # The verdict summarises the worker set, so every path that changes
  # that set has to recompute it. Each of these covers a path that once
  # did not: removal and batch removal recomputed but told nobody (that
  # half is gone with the waiter list), and creation did not recompute at
  # all.

  defmodule RecomputeTestCluster do
    @moduledoc false
    def name, do: "recompute_test_cluster"
    def otp_name_for_worker(id), do: :"recompute_test_worker_#{id}"
    def otp_name(:worker_supervisor), do: :recompute_test_worker_supervisor
    def otp_name(:link), do: :recompute_test_link
    def otp_name(:foreman), do: :recompute_test_foreman
  end

  defp failed_worker(id) do
    %WorkerInfo{
      id: id,
      path: "/nonexistent/#{id}",
      otp_name: :"recompute_test_worker_#{id}",
      health: {:failed_to_start, :manifest_does_not_exist}
    }
  end

  defp healthy_worker(id) do
    %WorkerInfo{
      id: id,
      path: "/nonexistent/#{id}",
      otp_name: :"recompute_test_worker_#{id}",
      health: {:ok, self()}
    }
  end

  defp state_with(workers) do
    %State{
      cluster: RecomputeTestCluster,
      capabilities: [:log],
      health: {:failed_to_start, :at_least_one_failed_to_start},
      otp_name: :recompute_test_foreman,
      path: "/nonexistent",
      workers: Map.new(workers, &{&1.id, &1})
    }
  end

  describe "do_remove_worker/2" do
    test "removing the last failing worker makes the foreman healthy" do
      state = state_with([healthy_worker("aaaa"), failed_worker("bbbb")])

      {settled, _result} = Impl.do_remove_worker(state, "bbbb")

      assert settled.health == :ok
    end

    test "removing one of several failing workers leaves it unhealthy" do
      state = state_with([failed_worker("aaaa"), failed_worker("bbbb")])

      {settled, _result} = Impl.do_remove_worker(state, "bbbb")

      refute settled.health == :ok
    end

    test "removing an absent worker changes nothing" do
      state = state_with([failed_worker("aaaa")])

      assert {^state, {:error, :worker_not_found}} = Impl.do_remove_worker(state, "zzzz")
    end
  end

  describe "do_remove_workers/2" do
    test "a batch removal that clears every failure makes the foreman healthy" do
      state = state_with([healthy_worker("aaaa"), failed_worker("bbbb"), failed_worker("cccc")])

      {settled, _results} = Impl.do_remove_workers(state, ["bbbb", "cccc"])

      assert settled.health == :ok
    end
  end

  describe "do_worker_health/3" do
    # The path production actually drives: Olivine casts its own health
    # after startup, and it is the only sender. Nothing else covered it,
    # so the recompute here could be deleted with the whole suite still
    # green.
    test "a worker reporting itself gone takes the foreman out of :ok" do
      state = %{state_with([healthy_worker("aaaa")]) | health: :ok}

      settled = Impl.do_worker_health(state, "aaaa", :stopped)

      assert settled.workers["aaaa"].health == :stopped
      refute settled.health == :ok
    end

    test "a worker reporting itself running makes the foreman healthy" do
      state = state_with([failed_worker("aaaa")])

      settled = Impl.do_worker_health(state, "aaaa", {:ok, self()})

      assert settled.health == :ok
    end
  end

  describe "do_new_worker/4" do
    # The same gap in the optimistic direction: adding a worker changes
    # the set the verdict summarises, so a worker that fails to start
    # must be able to take the foreman OUT of :ok. A fresh node folds an
    # empty worker set to :ok, so without a recompute here it would go on
    # reporting healthy while hosting a worker that never started.
    test "a worker that fails to start takes the foreman out of :ok" do
      # max_children: 0 makes every start_child fail, which is the
      # simplest honest way to get {:failed_to_start, _} from the real
      # code path rather than a hand-built worker_info.
      start_supervised!(
        {DynamicSupervisor,
         strategy: :one_for_one, max_children: 0, name: RecomputeTestCluster.otp_name(:worker_supervisor)}
      )

      dir = Path.join(System.tmp_dir!(), "recompute_new_worker_#{System.unique_integer([:positive])}")
      File.mkdir_p!(dir)
      on_exit(fn -> File.rm_rf!(dir) end)

      state = %{state_with([]) | path: dir, health: :ok}

      {settled, _ref} = Impl.do_new_worker(state, "eeeeeeee", :log, %{})

      assert [%{health: {:failed_to_start, _}}] = Map.values(settled.workers)

      refute settled.health == :ok,
             "a foreman hosting a worker that failed to start must not report healthy"
    end
  end
end
