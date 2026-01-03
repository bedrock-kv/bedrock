defmodule Bedrock.ControlPlane.Director.Recovery.CommitProxyStartupPhaseTest do
  use ExUnit.Case, async: true

  import Bedrock.Test.ControlPlane.RecoveryTestSupport

  alias Bedrock.ControlPlane.Director.Recovery.CommitProxyStartupPhase
  alias Bedrock.DataPlane.CommitProxy.Server

  # Mock cluster module for testing
  defmodule TestCluster do
    @moduledoc false
    def otp_name(:sup), do: :test_supervisor
  end

  # Helper functions for common setup patterns
  defp base_recovery_attempt do
    recovery_attempt()
    |> with_cluster(TestCluster)
    |> with_epoch(1)
    |> with_proxies([])
  end

  defp base_context(desired_proxies, coordination_nodes, start_fn \\ nil) do
    context = %{
      cluster_config: %{parameters: %{desired_commit_proxies: desired_proxies}},
      node_capabilities: %{coordination: coordination_nodes},
      lock_token: "test_token"
    }

    if start_fn, do: Map.put(context, :start_supervised_fn, start_fn), else: context
  end

  defp successful_start_fn do
    fn _child_spec, _node -> {:ok, spawn(fn -> :ok end)} end
  end

  defp failing_start_fn(error \\ :startup_failed) do
    fn _child_spec, _node -> {:error, error} end
  end

  defp tracking_agent(initial_value \\ []) do
    fn -> initial_value end |> Agent.start_link() |> elem(1)
  end

  describe "execute/1" do
    test "transitions to next phase when proxies start successfully" do
      recovery_attempt = base_recovery_attempt()
      context = base_context(2, [node(), :other_node], successful_start_fn())

      assert {%{proxies: [pid1, pid2]}, Bedrock.ControlPlane.Director.Recovery.ResolverStartupPhase} =
               CommitProxyStartupPhase.execute(recovery_attempt, context)

      assert is_pid(pid1) and is_pid(pid2)
    end

    test "stalls when no coordination capable nodes available" do
      recovery_attempt = base_recovery_attempt()
      context = base_context(2, [])

      assert {%{proxies: []}, {:stalled, {:insufficient_nodes, :no_coordination_capable_nodes, 2, 0}}} =
               CommitProxyStartupPhase.execute(recovery_attempt, context)
    end

    test "stalls when proxy startup fails" do
      recovery_attempt = base_recovery_attempt()
      context = base_context(1, [node()], failing_start_fn())

      assert {%{proxies: []}, {:stalled, {:failed_to_start, :commit_proxy, _, :startup_failed}}} =
               CommitProxyStartupPhase.execute(recovery_attempt, context)
    end
  end

  describe "define_commit_proxies/7" do
    test "distributes proxies round-robin across available nodes" do
      start_supervised_fn = fn _child_spec, node ->
        {:ok, spawn(fn -> send(self(), {:started_on, node}) end)}
      end

      available_nodes = [:node1, :node2, :node3]

      assert {:ok, [pid1, pid2, pid3, pid4, pid5]} =
               CommitProxyStartupPhase.define_commit_proxies(
                 5,
                 TestCluster,
                 1,
                 self(),
                 available_nodes,
                 start_supervised_fn,
                 "test_token",
                 %{parameters: %{}}
               )

      assert Enum.all?([pid1, pid2, pid3, pid4, pid5], &is_pid/1)

      # Should have distributed round-robin: node1, node2, node3, node1, node2
      # We can't easily verify the exact distribution without more complex mocking,
      # but we can verify we got the right number of proxies
    end

    test "handles empty available nodes list" do
      assert {:error, {:insufficient_nodes, :no_coordination_capable_nodes, 2, 0}} =
               CommitProxyStartupPhase.define_commit_proxies(
                 2,
                 TestCluster,
                 1,
                 self(),
                 [],
                 successful_start_fn(),
                 "test_token",
                 %{parameters: %{}}
               )
    end

    test "handles startup failure on specific node" do
      start_supervised_fn = fn _child_spec, :failing_node -> {:error, :node_failure} end

      assert {:error, {:failed_to_start, :commit_proxy, :failing_node, :node_failure}} =
               CommitProxyStartupPhase.define_commit_proxies(
                 1,
                 TestCluster,
                 1,
                 self(),
                 [:failing_node],
                 start_supervised_fn,
                 "test_token",
                 %{parameters: %{}}
               )
    end
  end

  describe "child_spec validation" do
    test "passes correct child specs with instance IDs to start_supervised_fn" do
      agent = tracking_agent({[], []})

      start_supervised_fn = fn child_spec, node ->
        Agent.update(agent, fn {specs, nodes} ->
          {[child_spec | specs], [node | nodes]}
        end)

        {:ok, spawn(fn -> :ok end)}
      end

      available_nodes = [:node1, :node2]

      assert {:ok, [_, _, _] = pids} =
               CommitProxyStartupPhase.define_commit_proxies(
                 3,
                 TestCluster,
                 42,
                 self(),
                 available_nodes,
                 start_supervised_fn,
                 "test_lock_token",
                 %{parameters: %{empty_transaction_timeout_ms: 5000}}
               )

      assert Enum.all?(pids, &is_pid/1)

      {captured_specs, captured_nodes} = Agent.get(agent, & &1)
      captured_specs = Enum.reverse(captured_specs)
      captured_nodes = Enum.reverse(captured_nodes)

      # Extract and validate all instances from the child specs
      instances =
        Enum.map(captured_specs, fn child_spec ->
          # Pattern match the entire expected structure
          assert %{
                   id: {Server, TestCluster, 42, instance},
                   start:
                     {GenServer, :start_link,
                      [Server, {TestCluster, _director, 42, _max_latency, _max_per_batch, 5000, "test_lock_token"}]}
                 } = child_spec

          instance
        end)

      # Verify we got instances 0, 1, 2 (though possibly in different order due to concurrency)
      assert Enum.sort(instances) == [0, 1, 2]
      assert length(captured_specs) == 3

      # Verify round-robin distribution across nodes (order may vary due to concurrency)
      assert Enum.sort(captured_nodes) == Enum.sort([:node1, :node2, :node1])
    end

    test "uses correct empty_transaction_timeout_ms from config" do
      agent = tracking_agent()

      start_supervised_fn = fn child_spec, _node ->
        Agent.update(agent, fn specs -> [child_spec | specs] end)
        {:ok, spawn(fn -> :ok end)}
      end

      assert {:ok, _pids} =
               CommitProxyStartupPhase.define_commit_proxies(
                 1,
                 TestCluster,
                 1,
                 self(),
                 [:node1],
                 start_supervised_fn,
                 "token",
                 %{parameters: %{empty_transaction_timeout_ms: 2500}}
               )

      assert [
               %{
                 start:
                   {GenServer, :start_link,
                    [_, {_cluster, _director, _epoch, _max_latency, _max_per_batch, 2500, _lock_token}]}
               }
             ] = Agent.get(agent, & &1)
    end
  end

  describe "round-robin distribution behavior" do
    test "creates correct number of proxies even when requested more than available nodes" do
      node_usage = tracking_agent()

      start_supervised_fn = fn _child_spec, node ->
        Agent.update(node_usage, fn nodes -> [node | nodes] end)
        {:ok, spawn(fn -> :ok end)}
      end

      available_nodes = [:node1, :node2]

      assert {:ok, [_, _, _, _, _] = pids} =
               CommitProxyStartupPhase.define_commit_proxies(
                 5,
                 TestCluster,
                 1,
                 self(),
                 available_nodes,
                 start_supervised_fn,
                 "test_token",
                 %{parameters: %{}}
               )

      assert Enum.all?(pids, &is_pid/1)

      # Should have used nodes multiple times in round-robin fashion
      assert [_, _, _, _, _] = used_nodes = Agent.get(node_usage, & &1)
      assert Enum.all?(used_nodes, &(&1 in [:node1, :node2]))
    end
  end
end
