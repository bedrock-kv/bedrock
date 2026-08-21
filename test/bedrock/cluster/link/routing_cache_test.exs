defmodule Bedrock.Cluster.Link.RoutingCacheTest do
  use ExUnit.Case, async: true

  alias Bedrock.Cluster.Link.Server
  alias Bedrock.Cluster.Link.State

  defmodule TestCluster do
    @moduledoc false
    def otp_name(component) when is_atom(component), do: :"routing_cache_test_#{component}"
  end

  @entry {<<>>, <<0xFF, 0xFF>>, {"wkr_sys", "n1@host"}}

  defp state(overrides \\ []) do
    struct!(%State{node: node(), cluster: TestCluster, capabilities: []}, overrides)
  end

  defp cache(state, entry), do: elem(Server.handle_cast({:cache_routing_entry, entry}, state), 1)

  describe "routing cache" do
    test "empty cache misses" do
      assert {:reply, {:error, :not_cached}, _} =
               Server.handle_call({:get_covering_entry, "a"}, self_from(), state())
    end

    test "a cached covering entry answers keys in its range, and only those" do
      cached = cache(state(), {"m", "z", {"wkr_a", "n1@host"}})

      assert {:reply, {:ok, {{"m", "z"}, {"wkr_a", "n1@host"}}}, _} =
               Server.handle_call({:get_covering_entry, "pear"}, self_from(), cached)

      # Keys outside the fetched range are honest misses — the cache is
      # partial by design, never a whole-map projection.
      assert {:reply, {:error, :not_cached}, _} =
               Server.handle_call({:get_covering_entry, "apple"}, self_from(), cached)

      assert {:reply, {:error, :not_cached}, _} =
               Server.handle_call({:get_covering_entry, "z"}, self_from(), cached)
    end

    test "entries coalesce: each fetched shard adds coverage" do
      cached =
        state()
        |> cache({"m", "z", {"wkr_a", "n1@host"}})
        |> cache({<<>>, "m", {"wkr_b", "n2@host"}})

      assert {:reply, {:ok, {{<<>>, "m"}, {"wkr_b", "n2@host"}}}, _} =
               Server.handle_call({:get_covering_entry, "apple"}, self_from(), cached)

      assert {:reply, {:ok, {{"m", "z"}, {"wkr_a", "n1@host"}}}, _} =
               Server.handle_call({:get_covering_entry, "pear"}, self_from(), cached)
    end

    test "invalidate_routing empties the whole cache synchronously — coarse by design" do
      cached = cache(state(), @entry)
      assert {:reply, :ok, invalidated} = Server.handle_call(:invalidate_routing, self_from(), cached)

      assert {:reply, {:error, :not_cached}, _} =
               Server.handle_call({:get_covering_entry, "a"}, self_from(), invalidated)
    end

    test "a wiring push drops the cached routing - new-epoch wiring must never pair with old-epoch routing" do
      cached = cache(state(), @entry)

      new_tsl = %{epoch: 2, sequencer: self(), proxies: [self()]}
      assert {:noreply, updated} = Server.handle_info({:tsl_updated, new_tsl}, cached)

      assert updated.transaction_system_layout == new_tsl

      assert {:reply, {:error, :not_cached}, _} =
               Server.handle_call({:get_covering_entry, "a"}, self_from(), updated)
    end
  end

  defp self_from, do: {self(), make_ref()}
end
