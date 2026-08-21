defmodule Bedrock.Cluster.Link.RoutingCacheTest do
  use ExUnit.Case, async: true

  alias Bedrock.Cluster.Link.Server
  alias Bedrock.Cluster.Link.State

  defmodule TestCluster do
    @moduledoc false
    def otp_name(component) when is_atom(component), do: :"routing_cache_test_#{component}"
  end

  @projection %{shard_layout: %{<<0xFF, 0xFF>> => {0, <<>>}}, materializers: %{0 => {"wkr_sys", "n1@host"}}}

  defp state(overrides \\ []) do
    struct!(%State{node: node(), cluster: TestCluster, capabilities: []}, overrides)
  end

  describe "routing cache" do
    test "empty cache misses" do
      assert {:reply, {:error, :unavailable}, _} = Server.handle_call(:get_routing, self_from(), state())
    end

    test "cache_routing fills, get_routing hits" do
      assert {:noreply, cached} = Server.handle_cast({:cache_routing, @projection}, state())
      assert {:reply, {:ok, @projection}, _} = Server.handle_call(:get_routing, self_from(), cached)
    end

    test "invalidate_routing empties the cache" do
      {:noreply, cached} = Server.handle_cast({:cache_routing, @projection}, state())
      assert {:noreply, invalidated} = Server.handle_cast(:invalidate_routing, cached)
      assert {:reply, {:error, :unavailable}, _} = Server.handle_call(:get_routing, self_from(), invalidated)
    end

    test "a wiring push drops the cached routing - new-epoch wiring must never pair with old-epoch routing" do
      {:noreply, cached} = Server.handle_cast({:cache_routing, @projection}, state())

      new_tsl = %{epoch: 2, sequencer: self(), proxies: [self()]}
      assert {:noreply, updated} = Server.handle_info({:tsl_updated, new_tsl}, cached)

      assert updated.transaction_system_layout == new_tsl
      assert {:reply, {:error, :unavailable}, _} = Server.handle_call(:get_routing, self_from(), updated)
    end
  end

  defp self_from, do: {self(), make_ref()}
end
