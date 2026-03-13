defmodule Bedrock.Internal.LayoutRouting do
  @moduledoc false

  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.ShardRouter

  @spec ordered_log_ids(%{Log.id() => term()}) :: [Log.id()]
  def ordered_log_ids(logs) when is_map(logs) do
    logs
    |> Map.keys()
    |> Enum.sort()
  end

  @spec build_log_map(%{Log.id() => term()} | [Log.id()]) :: %{non_neg_integer() => Log.id()}
  def build_log_map(logs) when is_map(logs) do
    logs
    |> ordered_log_ids()
    |> build_log_map()
  end

  def build_log_map(log_ids) when is_list(log_ids) do
    log_ids
    |> Enum.sort()
    |> Enum.with_index()
    |> Map.new(fn {log_id, index} -> {index, log_id} end)
  end

  @spec effective_replication_factor(non_neg_integer(), pos_integer() | nil) :: pos_integer()
  def effective_replication_factor(log_count, desired_replication_factor)
      when is_integer(log_count) and log_count >= 0 do
    desired_replication_factor =
      if is_integer(desired_replication_factor) and desired_replication_factor > 0 do
        desired_replication_factor
      else
        log_count
      end

    log_count
    |> min(desired_replication_factor)
    |> max(1)
  end

  @spec log_ids_for_shard(%{Log.id() => term()}, Bedrock.range_tag(), pos_integer() | nil) :: [Log.id()]
  def log_ids_for_shard(logs, _shard_tag, _desired_replication_factor) when map_size(logs) == 0, do: []

  def log_ids_for_shard(logs, shard_tag, desired_replication_factor) when is_map(logs) do
    log_map = build_log_map(logs)
    log_count = map_size(log_map)
    replication_factor = effective_replication_factor(log_count, desired_replication_factor)

    shard_tag
    |> ShardRouter.get_log_indices(log_count, replication_factor)
    |> Enum.map(&Map.fetch!(log_map, &1))
  end

  @spec log_subset_for_shard(%{Log.id() => term()}, Bedrock.range_tag(), pos_integer() | nil) ::
          %{Log.id() => term()}
  def log_subset_for_shard(logs, shard_tag, desired_replication_factor) when is_map(logs) do
    Map.take(logs, log_ids_for_shard(logs, shard_tag, desired_replication_factor))
  end
end
