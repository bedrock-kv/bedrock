defmodule Bedrock.ControlPlane.Director.Recovery.ResolverStartupPhaseTest do
  use ExUnit.Case, async: true

  import Bedrock.Test.ControlPlane.RecoveryTestSupport

  alias Bedrock.ControlPlane.Config.ResolverDescriptor
  alias Bedrock.ControlPlane.Director.Recovery.ResolverStartupPhase
  alias Bedrock.DataPlane.Resolver.Server

  # Mock cluster module for testing
  defmodule TestCluster do
    @moduledoc false
    def otp_name(:sup), do: :test_supervisor
  end

  # Helper to capture child specs
  defp setup_capture_agent do
    fn -> [] end |> Agent.start_link() |> elem(1)
  end

  defp capture_start_supervised_fn(agent) do
    fn child_spec, node ->
      Agent.update(agent, fn list -> [{child_spec, node} | list] end)
      {:ok, spawn(fn -> :ok end)}
    end
  end

  defp get_captured_calls(agent) do
    agent |> Agent.get(& &1) |> Enum.reverse()
  end

  describe "execute/1" do
    test "transitions to next phase when resolvers start successfully" do
      agent = setup_capture_agent()
      start_supervised_fn = capture_start_supervised_fn(agent)

      resolver_descriptors = [
        ResolverDescriptor.resolver_descriptor("a", {:vacancy, 1}),
        ResolverDescriptor.resolver_descriptor("m", {:vacancy, 2})
      ]

      recovery_attempt =
        recovery_attempt()
        |> with_cluster(TestCluster)
        |> with_epoch(42)
        |> with_version_vector({5, 100})
        |> with_resolvers(resolver_descriptors)

      context = %{
        node_capabilities: %{coordination: [:node1, :node2]},
        lock_token: "test_lock_token",
        start_supervised_fn: start_supervised_fn
      }

      {result, next_phase} = ResolverStartupPhase.execute(recovery_attempt, context)

      # Should transition to next phase with expected results
      assert {%{resolvers: resolvers}, Bedrock.ControlPlane.Director.Recovery.TopologyPhase} =
               {result, next_phase}

      assert length(resolvers) == 2
      assert Enum.all?(resolvers, fn {_start_key, pid} -> is_pid(pid) end)

      # Verify the child specs and nodes
      captured_calls = get_captured_calls(agent)
      assert length(captured_calls) == 2

      # Check each resolver child spec
      Enum.each(captured_calls, fn {child_spec, node} ->
        # Verify child spec structure and start args in one pattern match
        assert %{
                 id: {Server, TestCluster, _key_range, 42},
                 start: {GenServer, :start_link, [Server, {_lock_token, 100, 42, _director, 1000, 6000}]}
               } = child_spec

        # Verify node assignment (order may vary due to non-deterministic iteration)
        assert node in [:node1, :node2]
      end)

      # Verify all nodes are used (order may vary due to non-deterministic iteration)
      nodes_used = Enum.map(captured_calls, fn {_child_spec, node} -> node end)
      assert Enum.sort(nodes_used) == [:node1, :node2]
    end

    test "stalls when no coordination capable nodes available" do
      resolver_descriptors = [ResolverDescriptor.resolver_descriptor("a", {:vacancy, 1})]

      recovery_attempt =
        recovery_attempt()
        |> with_cluster(TestCluster)
        |> with_epoch(1)
        |> with_resolvers(resolver_descriptors)

      context = %{
        # No nodes available
        node_capabilities: %{coordination: []},
        lock_token: "test_token"
      }

      {result, stall_reason} = ResolverStartupPhase.execute(recovery_attempt, context)

      assert {{:stalled, {:insufficient_nodes, :no_coordination_capable_nodes, 1, 0}}, ^resolver_descriptors} =
               {stall_reason, result.resolvers}
    end

    test "stalls when resolver startup fails" do
      start_supervised_fn = fn _child_spec, _node ->
        {:error, :startup_failed}
      end

      resolver_descriptors = [ResolverDescriptor.resolver_descriptor("a", {:vacancy, 1})]

      recovery_attempt =
        recovery_attempt()
        |> with_cluster(TestCluster)
        |> with_epoch(1)
        |> with_resolvers(resolver_descriptors)

      context = %{
        node_capabilities: %{coordination: [:node1]},
        lock_token: "test_token",
        start_supervised_fn: start_supervised_fn
      }

      {result, stall_reason} = ResolverStartupPhase.execute(recovery_attempt, context)

      assert {{:stalled, {:failed_to_start, :resolver, :node1, :startup_failed}}, ^resolver_descriptors} =
               {stall_reason, result.resolvers}
    end

    test "uses correct lock token and version from context" do
      agent = fn -> nil end |> Agent.start_link() |> elem(1)

      start_supervised_fn = fn child_spec, _node ->
        Agent.update(agent, fn _ -> child_spec end)
        {:ok, spawn(fn -> :ok end)}
      end

      resolver_descriptors = [ResolverDescriptor.resolver_descriptor("x", {:vacancy, 1})]

      recovery_attempt =
        recovery_attempt()
        |> with_cluster(TestCluster)
        |> with_epoch(7)
        # last_committed_version should be 200
        |> with_version_vector({20, 200})
        |> with_resolvers(resolver_descriptors)

      context = %{
        node_capabilities: %{coordination: [:node1]},
        lock_token: "special_lock_token",
        start_supervised_fn: start_supervised_fn
      }

      {_result, _next_phase} = ResolverStartupPhase.execute(recovery_attempt, context)

      assert %{
               start: {GenServer, :start_link, [_, {"special_lock_token", 200, 7, _director, 1000, 6000}]}
             } = Agent.get(agent, & &1)
    end
  end

  describe "define_resolvers/1" do
    test "generates correct key ranges for multiple resolvers" do
      agent = setup_capture_agent()
      start_supervised_fn = capture_start_supervised_fn(agent)

      resolver_descriptors = [
        ResolverDescriptor.resolver_descriptor("", {:vacancy, 1}),
        ResolverDescriptor.resolver_descriptor("m", {:vacancy, 2}),
        ResolverDescriptor.resolver_descriptor("z", {:vacancy, 3})
      ]

      context = %{
        resolvers: resolver_descriptors,
        epoch: 1,
        available_nodes: [:node1, :node2, :node3],
        start_supervised_fn: start_supervised_fn,
        lock_token: "token",
        last_committed_version: 50,
        cluster: TestCluster
      }

      {:ok, resolvers} = ResolverStartupPhase.define_resolvers(context)

      assert length(resolvers) == 3
      # Should be sorted by start_key
      assert [{"", _pid1}, {"m", _pid2}, {"z", _pid3}] = resolvers

      # Verify all nodes are used (order may vary due to non-deterministic iteration)
      captured_calls = get_captured_calls(agent)
      nodes_used = Enum.map(captured_calls, fn {_child_spec, node} -> node end)
      assert Enum.sort(nodes_used) == [:node1, :node2, :node3]
    end

    test "handles empty resolver list" do
      context = %{
        resolvers: [],
        epoch: 1,
        available_nodes: [:node1],
        start_supervised_fn: fn _, _ -> {:ok, spawn(fn -> :ok end)} end,
        lock_token: "token",
        last_committed_version: 0,
        cluster: TestCluster
      }

      {:ok, resolvers} = ResolverStartupPhase.define_resolvers(context)
      assert resolvers == []
    end

    test "returns error when no nodes available" do
      context = %{
        resolvers: [ResolverDescriptor.resolver_descriptor("a", {:vacancy, 1})],
        epoch: 1,
        available_nodes: [],
        start_supervised_fn: fn _, _ -> {:ok, spawn(fn -> :ok end)} end,
        lock_token: "token",
        last_committed_version: 0,
        cluster: TestCluster
      }

      assert {:error, {:insufficient_nodes, :no_coordination_capable_nodes, 1, 0}} =
               ResolverStartupPhase.define_resolvers(context)
    end
  end
end
