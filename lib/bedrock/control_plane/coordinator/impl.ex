defmodule Bedrock.ControlPlane.Coordinator.Impl do
  @moduledoc """
  Implementation logic for the Coordinator, separated from GenServer concerns.
  """

  alias Bedrock.ControlPlane.Config
  alias Bedrock.DataPlane.Storage
  alias Bedrock.Raft.Log
  alias Bedrock.SystemKeys

  import Bedrock.ControlPlane.Config,
    only: [
      config: 1
    ]

  @doc """
  Read the latest configuration from committed raft log transactions.

  Scans the raft log for config-related transactions and returns the
  most recent committed configuration, if any exists.
  """
  @spec read_latest_config_from_raft_log(term()) ::
          {:ok, {non_neg_integer(), map()}} | {:error, term()}
  def read_latest_config_from_raft_log(raft_log) do
    raft_log
    |> Log.transactions_to(:newest)
    |> case do
      [] ->
        {:error, :empty_log}

      transactions when is_list(transactions) ->
        case find_latest_config_transaction(transactions) do
          {:ok, {version, config}} -> {:ok, {version, config}}
          {:error, reason} -> {:error, reason}
        end
    end
  rescue
    error -> {:error, {:exception, error}}
  end

  @spec find_latest_config_transaction([term()]) ::
          {:ok, {non_neg_integer(), map()}} | {:error, term()}
  defp find_latest_config_transaction(transactions) do
    # Scan transactions in reverse order (newest first) looking for config data
    # Prioritize rich configs over empty ones
    rich_config_result =
      transactions
      |> Enum.reverse()
      |> Enum.with_index()
      |> Enum.find_value(fn {transaction, index} ->
        case extract_config_from_transaction(transaction) do
          {:ok, config} when is_map(config) -> {:ok, {length(transactions) - index, config}}
          # Skip empty configs in first pass
          {:ok, {:empty_config, _config}} -> nil
          {:error, _} -> nil
        end
      end)

    case rich_config_result do
      {:ok, result} ->
        {:ok, result}

      nil ->
        # No rich config found, try again looking for any config (including empty ones)
        transactions
        |> Enum.reverse()
        |> Enum.with_index()
        |> Enum.find_value(fn {transaction, index} ->
          case extract_config_from_transaction(transaction) do
            {:ok, config} when is_map(config) -> {:ok, {length(transactions) - index, config}}
            {:ok, {:empty_config, config}} -> {:ok, {length(transactions) - index, config}}
            {:error, _} -> nil
          end
        end)
        |> case do
          {:ok, result} -> {:ok, result}
          nil -> {:error, :no_config_found}
        end
    end
  end

  @spec extract_config_from_transaction(term()) ::
          {:ok, Config.t() | {:empty_config, Config.t()}} | {:error, term()}
  defp extract_config_from_transaction(transaction) do
    # Try to extract config data from transaction
    # Raft transactions should contain the config map
    case transaction do
      config when is_map(config) ->
        # Check if this looks like a bedrock config with actual services
        if Map.has_key?(config, :coordinators) and Map.has_key?(config, :epoch) and
             Map.has_key?(config, :transaction_system_layout) do
          # Prefer configs with populated transaction_system_layout (non-empty services)
          case get_in(config, [:transaction_system_layout, :services]) do
            services when is_map(services) and map_size(services) > 0 ->
              {:ok, config}

            _empty_or_nil ->
              # This is a default/empty config, mark as lower priority
              {:ok, {:empty_config, config}}
          end
        else
          {:error, :not_config}
        end

      _other ->
        {:error, :invalid_format}
    end
  end

  @doc """
  Query all storage workers for durable version and try to read config from highest.

  This function implements a sequential fallback strategy where it tries storage
  workers in order of highest durable version first, falling back to the next
  highest if config reading fails.
  """
  @spec find_highest_version_storage_and_read_config([pid()], [node()]) ::
          {non_neg_integer(), map()}
  def find_highest_version_storage_and_read_config(storage_workers, coordinator_nodes) do
    # Get durable versions from all storage workers
    storage_versions =
      storage_workers
      |> Enum.map(&get_storage_durable_version/1)
      |> Enum.filter(fn {status, _} -> status == :ok end)
      |> Enum.map(fn {:ok, {worker, version}} -> {worker, version} end)

    case storage_versions do
      [] ->
        {0, config(coordinator_nodes)}

      versions ->
        # Sort by durable version (highest first) and try in sequence
        versions
        |> Enum.sort_by(fn {_worker, version} -> version end, :desc)
        |> try_read_config_in_sequence(coordinator_nodes)
    end
  end

  @doc """
  Get durable version from a single storage worker using the info API.
  """
  @spec get_storage_durable_version(pid()) ::
          {:ok, {pid(), non_neg_integer()}} | {:error, :timeout | :unavailable}
  def get_storage_durable_version(storage_worker) do
    case Storage.info(storage_worker, [:durable_version], timeout_in_ms: 200) do
      {:ok, info} ->
        durable_version = Map.get(info, :durable_version, 0)
        {:ok, {storage_worker, durable_version}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Try to read config from storage workers in sequence (highest version first).

  This function implements the sequential fallback strategy, trying each storage
  worker in order until one successfully provides a config, or falling back to
  defaults if all fail.
  """
  @spec try_read_config_in_sequence([{pid(), non_neg_integer()}], [node()]) ::
          {non_neg_integer(), map()}
  def try_read_config_in_sequence([], coordinator_nodes), do: {0, config(coordinator_nodes)}

  def try_read_config_in_sequence([{storage_worker, durable_version} | rest], coordinator_nodes) do
    case Storage.fetch(storage_worker, SystemKeys.config_monolithic(), durable_version,
           timeout: 200
         ) do
      {:ok, config_data} ->
        try do
          {_stored_version, config} = :erlang.binary_to_term(config_data)

          # Try to fetch transaction system layout (if it exists)
          complete_config =
            case Storage.fetch(storage_worker, SystemKeys.layout_monolithic(), durable_version,
                   timeout: 200
                 ) do
              {:ok, layout_data} ->
                transaction_system_layout = :erlang.binary_to_term(layout_data)
                Map.put(config, :transaction_system_layout, transaction_system_layout)

              {:error, _} ->
                # Layout key doesn't exist (backward compatibility) - use config as is
                config
            end

          # Use durable_version from info API, not stored version
          {durable_version, complete_config}
        rescue
          _ ->
            # Config corrupted, try next storage worker
            try_read_config_in_sequence(rest, coordinator_nodes)
        end

      {:error, _reason} ->
        # Config read failed, try next storage worker
        try_read_config_in_sequence(rest, coordinator_nodes)
    end
  end
end
