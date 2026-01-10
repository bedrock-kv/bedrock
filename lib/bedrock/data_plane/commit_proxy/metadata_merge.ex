defmodule Bedrock.DataPlane.CommitProxy.MetadataMerge do
  @moduledoc """
  Merges metadata mutations from resolver into routing tables.

  Called during finalization to update shard_table (ETS) and accumulate
  log/resolver metadata before log push happens.
  """

  alias Bedrock.DataPlane.CommitProxy.RoutingData
  alias Bedrock.SystemKeys
  alias Bedrock.SystemKeys.OtpRef
  alias Bedrock.SystemKeys.ShardMetadata

  @doc """
  Merges metadata updates into existing metadata, updating routing tables.

  - Inserts shard_key mutations into ETS shard_table
  - Accumulates layout_log mutations into metadata.log_services
  - Stores shard metadata for reference

  ## Parameters

  - `metadata` - Existing metadata map (may be empty)
  - `updates` - List of `{version, [mutations]}` tuples from resolver
  - `routing_data` - RoutingData struct containing shard_table, log_map, replication_factor

  ## Returns

  Updated metadata map with accumulated log_services, shards, etc.
  """
  @spec merge(map(), [term()], RoutingData.t()) :: map()
  def merge(metadata, updates, %RoutingData{} = routing_data) do
    Enum.reduce(updates, metadata, fn {_version, mutations}, acc ->
      Enum.reduce(mutations, acc, &apply_mutation(&1, &2, routing_data))
    end)
  end

  # Handle {:set, key, value} mutations
  defp apply_mutation({:set, key, value}, metadata, routing_data) do
    case SystemKeys.parse_key(key) do
      {:shard_key, end_key} ->
        tag = :erlang.binary_to_term(value)
        RoutingData.insert_shard(routing_data, end_key, tag)
        metadata

      {:layout_log, log_id} ->
        service_ref = OtpRef.to_tuple(value)
        put_in(metadata, [Access.key(:log_services, %{}), log_id], service_ref)

      {:shard, tag_str} ->
        {:ok, shard_meta} = ShardMetadata.read(value)
        put_in(metadata, [Access.key(:shards, %{}), tag_str], shard_meta)

      {:layout_resolver, end_key} ->
        service_ref = OtpRef.to_tuple(value)
        put_in(metadata, [Access.key(:resolvers, %{}), end_key], service_ref)

      _ ->
        # Unknown or non-routable key type - ignore
        metadata
    end
  end

  # Handle {:clear, key} mutations
  defp apply_mutation({:clear, key}, metadata, routing_data) do
    case SystemKeys.parse_key(key) do
      {:shard_key, end_key} ->
        RoutingData.delete_shard(routing_data, end_key)
        metadata

      {:layout_log, log_id} ->
        update_in(metadata, [Access.key(:log_services, %{})], &Map.delete(&1, log_id))

      {:shard, tag_str} ->
        update_in(metadata, [Access.key(:shards, %{})], &Map.delete(&1, tag_str))

      {:layout_resolver, end_key} ->
        update_in(metadata, [Access.key(:resolvers, %{})], &Map.delete(&1, end_key))

      _ ->
        metadata
    end
  end

  # Ignore other mutation types (clear_range, atomic, etc.)
  defp apply_mutation(_mutation, metadata, _routing_data), do: metadata
end
