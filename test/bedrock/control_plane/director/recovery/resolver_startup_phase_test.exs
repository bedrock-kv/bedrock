defmodule Bedrock.ControlPlane.Director.Recovery.ResolverStartupPhaseTest do
  use ExUnit.Case, async: true

  import RecoveryTestSupport

  alias Bedrock.ControlPlane.Config.ResolverDescriptor
  alias Bedrock.ControlPlane.Director.Recovery.ResolverStartupPhase
  alias Bedrock.DataPlane.Resolver.Server

  # Mock cluster module for testing
  defmodule TestCluster do
    @moduledoc false
    def otp_name(:sup), do: :test_supervisor
  end

  describe "execute/1" do
    test "transitions to next phase when resolvers start successfully" do
      agent = fn -> [] end |> Agent.start_link() |> elem(1)

      start_supervised_fn = fn child_spec, node ->
        Agent.update(agent, fn list -> [{child_spec, node} | list] end)
        {:ok, spawn(fn -> :ok end)}
      end

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

      # Should transition to next phase
      assert next_phase == Bedrock.ControlPlane.Director.Recovery.TransactionSystemLayoutPhase
      assert length(result.resolvers) == 2
      assert Enum.all?(result.resolvers, fn {_start_key, pid} -> is_pid(pid) end)

      # Verify the child specs and nodes
      captured_calls = agent |> Agent.get(& &1) |> Enum.reverse()
      assert length(captured_calls) == 2

      # Check each resolver child spec
      Enum.each(captured_calls, fn {child_spec, node} ->
        # Verify child spec has correct tuple-based ID format
        assert %{id: {Server, TestCluster, _key_range, 42}} = child_spec
        # Verify start args contain correct parameters
        assert %{start: {GenServer, :start_link, [Server, start_args]}} = child_spec
        # last_version=100, epoch=42
        assert {_lock_token, 100, 42, _director, 1000, 6000} = start_args
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

      assert {:stalled, {:insufficient_nodes, :no_coordination_capable_nodes, 1, 0}} = stall_reason
      assert result.resolvers == resolver_descriptors
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

      assert {:stalled, {:failed_to_start, :resolver, :node1, :startup_failed}} = stall_reason
      assert result.resolvers == resolver_descriptors
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

      captured_child_spec = Agent.get(agent, & &1)
      assert %{start: {GenServer, :start_link, [_, start_args]}} = captured_child_spec
      assert {"special_lock_token", 200, 7, _director, 1000, 6000} = start_args
    end
  end

  describe "define_resolvers/1" do
    test "generates correct key ranges for multiple resolvers" do
      agent = fn -> [] end |> Agent.start_link() |> elem(1)

      start_supervised_fn = fn child_spec, node ->
        Agent.update(agent, fn list -> [{child_spec, node} | list] end)
        {:ok, spawn(fn -> :ok end)}
      end

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
      captured_calls = agent |> Agent.get(& &1) |> Enum.reverse()
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

      result = ResolverStartupPhase.define_resolvers(context)
      assert {:error, {:insufficient_nodes, :no_coordination_capable_nodes, 1, 0}} = result
    end
  end
end
