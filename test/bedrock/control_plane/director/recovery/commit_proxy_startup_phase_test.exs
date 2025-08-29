defmodule Bedrock.ControlPlane.Director.Recovery.CommitProxyStartupPhaseTest do
  use ExUnit.Case, async: true

  import RecoveryTestSupport

  alias Bedrock.ControlPlane.Director.Recovery.CommitProxyStartupPhase
  alias Bedrock.DataPlane.CommitProxy.Server

  # Mock cluster module for testing
  defmodule TestCluster do
    @moduledoc false
    def otp_name(:sup), do: :test_supervisor
  end

  describe "execute/1" do
    test "transitions to next phase when proxies start successfully" do
      # Mock successful proxy startup
      start_supervised_fn = fn _child_spec, _node ->
        {:ok, spawn(fn -> :ok end)}
      end

      recovery_attempt =
        recovery_attempt()
        |> with_cluster(TestCluster)
        |> with_epoch(1)
        |> with_proxies([])

      context = %{
        cluster_config: %{parameters: %{desired_commit_proxies: 2}},
        node_capabilities: %{coordination: [node(), :other_node]},
        lock_token: "test_token",
        start_supervised_fn: start_supervised_fn
      }

      {result, next_phase} = CommitProxyStartupPhase.execute(recovery_attempt, context)

      assert next_phase == Bedrock.ControlPlane.Director.Recovery.ResolverStartupPhase
      assert length(result.proxies) == 2
      assert Enum.all?(result.proxies, &is_pid/1)
    end

    test "stalls when no coordination capable nodes available" do
      recovery_attempt =
        recovery_attempt()
        |> with_cluster(TestCluster)
        |> with_epoch(1)
        |> with_proxies([])

      context = %{
        cluster_config: %{parameters: %{desired_commit_proxies: 2}},
        node_capabilities: %{coordination: []},
        lock_token: "test_token"
      }

      {result, stall_reason} = CommitProxyStartupPhase.execute(recovery_attempt, context)

      assert {:stalled, {:insufficient_nodes, :no_coordination_capable_nodes, 2, 0}} =
               stall_reason

      assert result.proxies == []
    end

    test "stalls when proxy startup fails" do
      # Mock failing proxy startup
      start_supervised_fn = fn _child_spec, _node ->
        {:error, :startup_failed}
      end

      recovery_attempt =
        recovery_attempt()
        |> with_cluster(TestCluster)
        |> with_epoch(1)
        |> with_proxies([])

      context = %{
        cluster_config: %{parameters: %{desired_commit_proxies: 1}},
        node_capabilities: %{coordination: [node()]},
        lock_token: "test_token",
        start_supervised_fn: start_supervised_fn
      }

      {result, stall_reason} = CommitProxyStartupPhase.execute(recovery_attempt, context)

      assert {:stalled, {:failed_to_start, :commit_proxy, _, :startup_failed}} = stall_reason
      assert result.proxies == []
    end
  end

  describe "define_commit_proxies/7" do
    test "distributes proxies round-robin across available nodes" do
      start_supervised_fn = fn _child_spec, node ->
        {:ok, spawn(fn -> send(self(), {:started_on, node}) end)}
      end

      available_nodes = [:node1, :node2, :node3]

      {:ok, pids} =
        CommitProxyStartupPhase.define_commit_proxies(
          # Want 5 proxies
          5,
          TestCluster,
          # epoch
          1,
          self(),
          available_nodes,
          start_supervised_fn,
          "test_token",
          %{parameters: %{}}
        )

      assert length(pids) == 5
      assert Enum.all?(pids, &is_pid/1)

      # Should have distributed round-robin: node1, node2, node3, node1, node2
      # We can't easily verify the exact distribution without more complex mocking,
      # but we can verify we got the right number of proxies
    end

    test "handles empty available nodes list" do
      result =
        CommitProxyStartupPhase.define_commit_proxies(
          2,
          TestCluster,
          1,
          self(),
          # No available nodes
          [],
          fn _, _ -> {:ok, spawn(fn -> :ok end)} end,
          "test_token",
          %{parameters: %{}}
        )

      assert {:error, {:insufficient_nodes, :no_coordination_capable_nodes, 2, 0}} = result
    end

    test "handles startup failure on specific node" do
      start_supervised_fn = fn _child_spec, :failing_node ->
        {:error, :node_failure}
      end

      result =
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

      assert {:error, {:failed_to_start, :commit_proxy, :failing_node, :node_failure}} = result
    end
  end

  describe "child_spec validation" do
    test "passes correct child specs with instance IDs to start_supervised_fn" do
      agent = fn -> {[], []} end |> Agent.start_link() |> elem(1)

      start_supervised_fn = fn child_spec, node ->
        Agent.update(agent, fn {specs, nodes} ->
          {[child_spec | specs], [node | nodes]}
        end)

        {:ok, spawn(fn -> :ok end)}
      end

      available_nodes = [:node1, :node2]

      {:ok, pids} =
        CommitProxyStartupPhase.define_commit_proxies(
          # Want 3 proxies
          3,
          TestCluster,
          # epoch
          42,
          self(),
          available_nodes,
          start_supervised_fn,
          "test_lock_token",
          %{parameters: %{empty_transaction_timeout_ms: 5000}}
        )

      assert length(pids) == 3

      {captured_specs, captured_nodes} = Agent.get(agent, & &1)
      captured_specs = Enum.reverse(captured_specs)
      captured_nodes = Enum.reverse(captured_nodes)

      # Check that we got 3 child specs
      assert length(captured_specs) == 3

      # Check each child spec has the correct tuple-based ID format
      # Extract all instances from the child specs
      instances =
        Enum.map(captured_specs, fn child_spec ->
          # The ID should be {CommitProxy.Server, cluster, epoch, instance}
          assert %{id: {Server, TestCluster, 42, instance}} = child_spec

          # Check the start tuple contains the correct parameters
          assert %{start: {GenServer, :start_link, [Server, start_args]}} = child_spec

          # Verify the start args are correct
          assert {TestCluster, _director, 42, _max_latency, _max_per_batch, 5000, "test_lock_token"} = start_args

          instance
        end)

      # Verify we got instances 0, 1, 2 (though possibly in different order due to concurrency)
      assert Enum.sort(instances) == [0, 1, 2]

      # Verify round-robin distribution across nodes (order may vary due to concurrency)
      assert Enum.sort(captured_nodes) == Enum.sort([:node1, :node2, :node1])
    end

    test "uses correct empty_transaction_timeout_ms from config" do
      agent = fn -> [] end |> Agent.start_link() |> elem(1)

      start_supervised_fn = fn child_spec, _node ->
        Agent.update(agent, fn specs -> [child_spec | specs] end)
        {:ok, spawn(fn -> :ok end)}
      end

      {:ok, _pids} =
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

      [child_spec] = Agent.get(agent, & &1)
      assert %{start: {GenServer, :start_link, [_, start_args]}} = child_spec
      # Check that empty_transaction_timeout_ms is correctly passed through
      assert {_cluster, _director, _epoch, _max_latency, _max_per_batch, 2500, _lock_token} = start_args
    end
  end

  describe "round-robin distribution behavior" do
    test "creates correct number of proxies even when requested more than available nodes" do
      # Track which nodes were used
      node_usage = fn -> [] end |> Agent.start_link() |> elem(1)

      start_supervised_fn = fn _child_spec, node ->
        Agent.update(node_usage, fn nodes -> [node | nodes] end)
        {:ok, spawn(fn -> :ok end)}
      end

      available_nodes = [:node1, :node2]

      {:ok, pids} =
        CommitProxyStartupPhase.define_commit_proxies(
          # Want 5 proxies across 2 nodes
          5,
          TestCluster,
          1,
          self(),
          available_nodes,
          start_supervised_fn,
          "test_token",
          %{parameters: %{}}
        )

      assert length(pids) == 5

      used_nodes = Agent.get(node_usage, & &1)
      # Should have used nodes multiple times in round-robin fashion
      assert length(used_nodes) == 5
      assert Enum.all?(used_nodes, fn node -> node in [:node1, :node2] end)
    end
  end
end
