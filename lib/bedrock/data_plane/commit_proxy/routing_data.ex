defmodule Bedrock.DataPlane.CommitProxy.RoutingData do
  @moduledoc """
  Manages routing data for the commit proxy.

  Encapsulates all information needed to route mutations to logs:
  - `shard_table` - ETS ordered_set for key → tag ceiling search
  - `log_map` - Map of index → log_id for golden ratio routing
  - `log_services` - Map of log_id → {otp_name, node} for contacting logs
  - `replication_factor` - Number of logs per mutation

  ## Lifecycle

  - `new/1` - Creates routing data from a transaction system layout
  - `new_empty/0` - Creates empty routing data for dynamic population
  - `cleanup/1` - Deletes the ETS table when the commit proxy terminates

  ## Shard Updates

  - `insert_shard/3` - Adds or updates a shard entry
  - `delete_shard/2` - Removes a shard entry

  ## Log Updates

  - `insert_log/2` - Adds a log to log_map at next index
  - `remove_log/2` - Removes a log and reindexes
  - `put_log_service/3` - Adds or updates a log service reference
  - `delete_log_service/2` - Removes a log service reference
  - `set_replication_factor/2` - Updates the replication factor
  """

  alias Bedrock.DataPlane.Log

  @type t :: %__MODULE__{
          shard_table: :ets.table(),
          log_map: %{non_neg_integer() => Log.id()},
          log_services: %{Log.id() => {atom(), node()}},
          replication_factor: pos_integer()
        }

  defstruct [:shard_table, :log_map, :log_services, :replication_factor]

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
      log_services: %{},
      replication_factor: infer_replication_factor(storage_teams, logs)
    }
  end

  @doc """
  Creates empty routing data for dynamic population via metadata.

  Starts with an empty shard table, no logs, and replication factor of 1.
  All fields are populated incrementally as metadata mutations arrive.
  """
  @spec new_empty() :: t()
  def new_empty do
    %__MODULE__{
      shard_table: :ets.new(:shard_keys, [:ordered_set, :public]),
      log_map: %{},
      log_services: %{},
      replication_factor: 1
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

  @doc """
  Adds a log to the log_map at the next available index.
  """
  @spec insert_log(t(), Log.id()) :: t()
  def insert_log(%__MODULE__{log_map: log_map} = routing_data, log_id) do
    next_index = map_size(log_map)
    %{routing_data | log_map: Map.put(log_map, next_index, log_id)}
  end

  @doc """
  Removes a log from the log_map and reindexes remaining entries.

  Maintains contiguous indices starting from 0.
  """
  @spec remove_log(t(), Log.id()) :: t()
  def remove_log(%__MODULE__{log_map: log_map} = routing_data, log_id) do
    new_map =
      log_map
      |> Enum.reject(fn {_index, id} -> id == log_id end)
      |> Enum.sort_by(fn {index, _id} -> index end)
      |> Enum.with_index()
      |> Map.new(fn {{_old_index, id}, new_index} -> {new_index, id} end)

    %{routing_data | log_map: new_map}
  end

  @doc """
  Adds or updates a log service reference.
  """
  @spec put_log_service(t(), Log.id(), {atom(), node()}) :: t()
  def put_log_service(%__MODULE__{log_services: log_services} = routing_data, log_id, service_ref) do
    %{routing_data | log_services: Map.put(log_services, log_id, service_ref)}
  end

  @doc """
  Removes a log service reference.
  """
  @spec delete_log_service(t(), Log.id()) :: t()
  def delete_log_service(%__MODULE__{log_services: log_services} = routing_data, log_id) do
    %{routing_data | log_services: Map.delete(log_services, log_id)}
  end

  @doc """
  Updates the replication factor.
  """
  @spec set_replication_factor(t(), pos_integer()) :: t()
  def set_replication_factor(%__MODULE__{} = routing_data, factor) do
    %{routing_data | replication_factor: factor}
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
