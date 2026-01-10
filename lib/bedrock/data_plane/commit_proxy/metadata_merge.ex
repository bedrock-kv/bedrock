defmodule Bedrock.DataPlane.CommitProxy.MetadataMerge do
  @moduledoc """
  Merges non-routing metadata mutations from resolver.

  Handles shard details and resolver layout updates. Routing mutations
  (shard_key, layout_log) are handled by RoutingData.apply_mutations/2.
  """

  alias Bedrock.SystemKeys
  alias Bedrock.SystemKeys.OtpRef
  alias Bedrock.SystemKeys.ShardMetadata

  @doc """
  Merges non-routing metadata updates.

  Handles:
  - shard: Detailed shard metadata (storage team info)
  - layout_resolver: Resolver service references

  Routing mutations (shard_key, layout_log) should be processed separately
  via RoutingData.apply_mutations/2.

  ## Parameters

  - `metadata` - Existing metadata map (may be empty)
  - `updates` - List of `{version, [mutations]}` tuples from resolver

  ## Returns

  Updated metadata map with accumulated shards, resolvers, etc.
  """
  @spec merge(map(), [term()]) :: map()
  def merge(metadata, updates) do
    Enum.reduce(updates, metadata, fn {_version, mutations}, acc ->
      Enum.reduce(mutations, acc, &apply_mutation/2)
    end)
  end

  # Handle {:set, key, value} mutations
  defp apply_mutation({:set, key, value}, metadata) do
    case SystemKeys.parse_key(key) do
      {:shard, tag_str} ->
        {:ok, shard_meta} = ShardMetadata.read(value)
        put_in(metadata, [Access.key(:shards, %{}), tag_str], shard_meta)

      {:layout_resolver, end_key} ->
        service_ref = OtpRef.to_tuple(value)
        put_in(metadata, [Access.key(:resolvers, %{}), end_key], service_ref)

      _ ->
        # Routing keys (shard_key, layout_log) handled by RoutingData
        # Unknown keys ignored
        metadata
    end
  end

  # Handle {:clear, key} mutations
  defp apply_mutation({:clear, key}, metadata) do
    case SystemKeys.parse_key(key) do
      {:shard, tag_str} ->
        update_in(metadata, [Access.key(:shards, %{})], &Map.delete(&1, tag_str))

      {:layout_resolver, end_key} ->
        update_in(metadata, [Access.key(:resolvers, %{})], &Map.delete(&1, end_key))

      _ ->
        metadata
    end
  end

  # Ignore other mutation types (clear_range, atomic, etc.)
  defp apply_mutation(_mutation, metadata), do: metadata
end
