defmodule Bedrock.DataPlane.CommitProxy.RoutingData do
  @moduledoc """
  Manages routing data for the commit proxy.

  Encapsulates all information needed to route mutations to logs:
  - `shard_table` - ETS ordered_set for key → tag ceiling search
  - `log_map` - Map of index → log_id for golden ratio routing
  - `log_services` - Map of log_id → {otp_name, node} for contacting logs
  - `replication_factor` - Number of logs per mutation

  ## Lifecycle

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
  alias Bedrock.SystemKeys
  alias Bedrock.SystemKeys.Values

  @type t :: %__MODULE__{
          shard_table: :ets.table(),
          log_map: %{non_neg_integer() => Log.id()},
          log_services: %{Log.id() => {atom(), node()}},
          replication_factor: pos_integer()
        }

  defstruct [:shard_table, :log_map, :log_services, :replication_factor]

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

  Called from apply_mutations/2 when processing shard_key mutations.
  """
  @spec insert_shard(t(), binary(), term()) :: true
  def insert_shard(%__MODULE__{shard_table: table}, end_key, tag) do
    :ets.insert(table, {end_key, tag})
  end

  @doc """
  Deletes a shard entry from the routing table.

  Called from apply_mutations/2 when processing shard_key clear mutations.
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

  @doc """
  Applies metadata mutations to update routing data.

  Handles shard_key and layout_log mutations:
  - shard_key: Updates ETS shard_table
  - layout_log: Updates both log_map and log_services

  ## Parameters

  - `routing_data` - Current routing data
  - `updates` - List of `{version, [mutations]}` tuples from resolver

  ## Returns

  Updated routing data with applied mutations.
  """
  @spec apply_mutations(t(), [{term(), [term()]}]) :: t()
  def apply_mutations(%__MODULE__{} = routing_data, updates) do
    Enum.reduce(updates, routing_data, fn {_version, mutations}, acc ->
      Enum.reduce(mutations, acc, &apply_mutation/2)
    end)
  end

  defp apply_mutation({:set, key, value}, routing_data) do
    case SystemKeys.parse_key(key) do
      {:shard_key, end_key} ->
        # Undecodable values are ignored; the routing table keeps its last
        # good entry rather than crashing the commit proxy.
        case Values.decode_shard_key_entry(value) do
          {:ok, {tag, _start_key}} -> insert_shard(routing_data, end_key, tag)
          {:error, _} -> :ignored
        end

        routing_data

      {:layout_log, log_id} ->
        # The value (log descriptor tags) is not needed for routing; service
        # refs are populated at runtime by the director, not from persisted data.
        insert_log(routing_data, log_id)

      _ ->
        routing_data
    end
  end

  defp apply_mutation({:clear, key}, routing_data) do
    case SystemKeys.parse_key(key) do
      {:shard_key, end_key} ->
        delete_shard(routing_data, end_key)
        routing_data

      {:layout_log, log_id} ->
        routing_data
        |> remove_log(log_id)
        |> delete_log_service(log_id)

      _ ->
        routing_data
    end
  end

  # Mirrors Metadata's clear_range semantics: delete every known entry whose
  # full system key falls in [start_key, end_key). Recovery rewrites use this
  # to drop stale shard/log entries before re-writing the current layout.
  defp apply_mutation({:clear_range, start_key, end_key}, routing_data) do
    routing_data
    |> clear_shards_in_range(start_key, end_key)
    |> clear_logs_in_range(start_key, end_key)
  end

  defp apply_mutation(_mutation, routing_data), do: routing_data

  defp clear_shards_in_range(%__MODULE__{shard_table: table} = routing_data, start_key, end_key) do
    for {shard_end_key, _tag} <- :ets.tab2list(table),
        full_key = SystemKeys.shard_key(shard_end_key),
        full_key >= start_key and full_key < end_key do
      :ets.delete(table, shard_end_key)
    end

    routing_data
  end

  defp clear_logs_in_range(%__MODULE__{log_map: log_map} = routing_data, start_key, end_key) do
    log_map
    |> Map.values()
    |> Enum.filter(fn log_id ->
      full_key = SystemKeys.layout_log(log_id)
      full_key >= start_key and full_key < end_key
    end)
    |> Enum.reduce(routing_data, fn log_id, routing_data ->
      routing_data
      |> remove_log(log_id)
      |> delete_log_service(log_id)
    end)
  end
end
