defmodule Bedrock.Service.Foreman.SettleHealthTest do
  use ExUnit.Case, async: true

  alias Bedrock.Service.Foreman.Impl
  alias Bedrock.Service.Foreman.State
  alias Bedrock.Service.Foreman.WorkerInfo

  # Recomputing the verdict and waking the callers waiting on it are one
  # act, not two. Every place that does the first must do the second —
  # otherwise a caller parked in wait_for_healthy/2 (default timeout
  # :infinity) sleeps through the very moment its condition came true.

  defmodule SettleTestCluster do
    @moduledoc false
    def name, do: "settle_test_cluster"
    def otp_name_for_worker(id), do: :"settle_test_worker_#{id}"
    def otp_name(:worker_supervisor), do: :settle_test_worker_supervisor
    def otp_name(:link), do: :settle_test_link
    def otp_name(:foreman), do: :settle_test_foreman
  end

  defp failed_worker(id) do
    %WorkerInfo{
      id: id,
      path: "/nonexistent/#{id}",
      otp_name: :"settle_test_worker_#{id}",
      health: {:failed_to_start, :manifest_does_not_exist}
    }
  end

  defp healthy_worker(id) do
    %WorkerInfo{
      id: id,
      path: "/nonexistent/#{id}",
      otp_name: :"settle_test_worker_#{id}",
      health: {:ok, self()}
    }
  end

  defp state_with(workers, waiting) do
    %State{
      cluster: SettleTestCluster,
      capabilities: [:log],
      health: {:failed_to_start, :at_least_one_failed_to_start},
      otp_name: :settle_test_foreman,
      path: "/nonexistent",
      waiting_for_healthy: waiting,
      workers: Map.new(workers, &{&1.id, &1})
    }
  end

  describe "do_remove_worker/2" do
    # Removing the last failing worker is exactly when a parked caller's
    # condition becomes true, and exactly when it was never told.
    test "wakes waiters when removal makes the foreman healthy" do
      tag = make_ref()
      state = state_with([healthy_worker("aaaa"), failed_worker("bbbb")], [{self(), tag}])

      {settled, _result} = Impl.do_remove_worker(state, "bbbb")

      assert settled.health == :ok
      assert settled.waiting_for_healthy == []
      assert_receive {^tag, :ok}
    end

    test "leaves waiters parked while the foreman is still unhealthy" do
      tag = make_ref()
      state = state_with([failed_worker("aaaa"), failed_worker("bbbb")], [{self(), tag}])

      {settled, _result} = Impl.do_remove_worker(state, "bbbb")

      # Deliberately not asserting WHICH unhealthy verdict: that depends
      # on the fold, which bedrock-287 corrects on a separate branch.
      # What matters here is that a waiter is only woken by :ok.
      refute settled.health == :ok
      assert settled.waiting_for_healthy == [{self(), tag}]
      refute_receive {^tag, :ok}, 50
    end

    test "removing an absent worker changes nothing" do
      tag = make_ref()
      state = state_with([failed_worker("aaaa")], [{self(), tag}])

      assert {^state, {:error, :worker_not_found}} = Impl.do_remove_worker(state, "zzzz")
      refute_receive {^tag, :ok}, 50
    end
  end

  describe "do_new_worker/4" do
    # The same gap in the optimistic direction: adding a worker changes
    # the set the verdict summarises, so a worker that fails to start
    # must be able to take the foreman OUT of :ok. Left unsettled, a
    # fresh node reports healthy while hosting a dead worker, and
    # do_wait_for_healthy/2 short-circuits :ok immediately.
    test "a worker that fails to start takes the foreman out of :ok" do
      # max_children: 0 makes every start_child fail, which is the
      # simplest honest way to get {:failed_to_start, _} from the real
      # code path rather than a hand-built worker_info.
      start_supervised!(
        {DynamicSupervisor,
         strategy: :one_for_one, max_children: 0, name: SettleTestCluster.otp_name(:worker_supervisor)}
      )

      dir = Path.join(System.tmp_dir!(), "settle_new_worker_#{System.unique_integer([:positive])}")
      File.mkdir_p!(dir)
      on_exit(fn -> File.rm_rf!(dir) end)

      state = %{state_with([], []) | path: dir, health: :ok}

      {settled, _ref} = Impl.do_new_worker(state, "eeeeeeee", :log, %{})

      assert [%{health: {:failed_to_start, _}}] = Map.values(settled.workers)

      refute settled.health == :ok,
             "a foreman hosting a worker that failed to start must not report healthy"
    end
  end

  describe "do_remove_workers/2" do
    test "wakes waiters when a batch removal makes the foreman healthy" do
      tag = make_ref()

      state =
        state_with(
          [healthy_worker("aaaa"), failed_worker("bbbb"), failed_worker("cccc")],
          [{self(), tag}]
        )

      {settled, _results} = Impl.do_remove_workers(state, ["bbbb", "cccc"])

      assert settled.health == :ok
      assert settled.waiting_for_healthy == []
      assert_receive {^tag, :ok}
    end
  end
end
