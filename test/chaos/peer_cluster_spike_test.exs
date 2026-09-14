defmodule Bedrock.Chaos.PeerClusterSpikeTest do
  @moduledoc """
  Phase 0 of the chaos-testing harness (bedrock-80f).

  Answers one question: can three `:peer` nodes form a real Bedrock cluster,
  commit a transaction, read it back, and tear down cleanly, all from ExUnit?
  Everything the harness will eventually do — fault injection, workload
  generation, invariant checking — rests on this, so it is proven on its own
  before any of it is built.
  """
  use ExUnit.Case, async: false

  alias Bedrock.Test.Chaos.Cluster
  alias Bedrock.Test.Chaos.Ops
  alias Bedrock.Test.Chaos.PeerCluster

  @moduletag :chaos
  @moduletag timeout: 180_000

  setup do
    cluster = PeerCluster.start!(count: 3)
    on_exit(fn -> PeerCluster.stop(cluster) end)
    {:ok, cluster: cluster}
  end

  test "three peer nodes form one cluster and serve a transaction", %{cluster: cluster} do
    [first, second, third] = PeerCluster.node_names(cluster)

    for node <- [first, second, third] do
      assert {:ok, coordinator_nodes} = PeerCluster.call(node, Cluster, :fetch_coordinator_nodes, [])

      assert Enum.sort(coordinator_nodes) == Enum.sort([first, second, third]),
             "#{node} does not see the full cluster; it may have fallen back to a single-node descriptor"
    end

    key = "spike/hello"
    value = "world"

    assert :ok = PeerCluster.call(first, Ops, :put, [key, value])

    # Read from a different node than the one that committed: a value visible
    # only on its writer would mean the commit never reached the shared
    # transaction system.
    assert ^value = PeerCluster.call(second, Ops, :get, [key])
  end
end
