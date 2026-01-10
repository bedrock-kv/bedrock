defmodule Bedrock.DataPlane.CommitProxy.RoutingData do
  @moduledoc """
  Manages shard routing data for the commit proxy.

  Encapsulates the ETS shard table, log map, and replication factor used to
  route mutations to the appropriate logs during finalization.

  ## Lifecycle

  - `new/1` - Creates routing data from a transaction system layout
  - `cleanup/1` - Deletes the ETS table when the commit proxy terminates

  ## Shard Updates

  - `insert_shard/3` - Adds or updates a shard entry (called from MetadataMerge)
  - `delete_shard/2` - Removes a shard entry (called from MetadataMerge)
  """

  alias Bedrock.DataPlane.Log

  @type t :: %__MODULE__{
          shard_table: :ets.table(),
          log_map: %{non_neg_integer() => Log.id()},
          replication_factor: pos_integer()
        }

  defstruct [:shard_table, :log_map, :replication_factor]

  @doc """
  Creates routing data from a transaction system layout.

  Builds an ETS ordered_set table for shard ceiling search and a log map
  for golden ratio log selection.
  """
  @spec new(map()) :: t()
  def new(transaction_system_layout) do
    storage_teams = transaction_system_layout.storage_teams
    logs = transaction_system_layout.logs

    %__MODULE__{
      shard_table: build_shard_table(storage_teams),
      log_map: build_log_map(logs),
      replication_factor: infer_replication_factor(storage_teams, logs)
    }
  end

  @doc """
  Cleans up routing data by deleting the ETS table.

  Safe to call with nil or if the table has already been deleted.
  """
  @spec cleanup(t() | nil) :: true
  def cleanup(nil), do: true

  def cleanup(%__MODULE__{shard_table: table}) do
    :ets.delete(table)
  rescue
    ArgumentError -> true
  end

  @doc """
  Inserts or updates a shard entry in the routing table.

  Called from MetadataMerge when processing shard_key mutations.
  """
  @spec insert_shard(t(), binary(), term()) :: true
  def insert_shard(%__MODULE__{shard_table: table}, end_key, tag) do
    :ets.insert(table, {end_key, tag})
  end

  @doc """
  Deletes a shard entry from the routing table.

  Called from MetadataMerge when processing shard_key clear mutations.
  """
  @spec delete_shard(t(), binary()) :: true
  def delete_shard(%__MODULE__{shard_table: table}, end_key) do
    :ets.delete(table, end_key)
  end

  # Build an ETS ordered_set table for ceiling search from storage_teams
  @spec build_shard_table([map()]) :: :ets.table()
  defp build_shard_table(storage_teams) do
    table = :ets.new(:shard_keys, [:ordered_set, :public])

    Enum.each(storage_teams, fn team ->
      %{tag: tag, key_range: {_start_key, end_key}} = team
      :ets.insert(table, {end_key, tag})
    end)

    table
  end

  # Build a map from log index to log_id for golden ratio lookup
  @spec build_log_map(map()) :: map()
  defp build_log_map(logs) do
    logs
    |> Map.keys()
    |> Enum.sort()
    |> Enum.with_index()
    |> Map.new(fn {log_id, index} -> {index, log_id} end)
  end

  # Infer replication factor from the data
  @spec infer_replication_factor([map()], map()) :: pos_integer()
  defp infer_replication_factor([], logs) do
    max(1, map_size(logs))
  end

  defp infer_replication_factor([first_team | _], logs) do
    case Map.get(first_team, :storage_ids) do
      nil -> max(1, map_size(logs))
      storage_ids -> max(1, length(storage_ids))
    end
  end
end
