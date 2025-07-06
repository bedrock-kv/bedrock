defmodule Bedrock.ControlPlane.Coordinator.Impl do
  @moduledoc """
  Implementation logic for the Coordinator, separated from GenServer concerns.
  """

  alias Bedrock.DataPlane.Storage
  alias Bedrock.Service.Foreman

  import Bedrock.ControlPlane.Config,
    only: [
      config: 1
    ]

  @doc """
  Bootstrap coordinator configuration from storage or fall back to defaults.

  This function queries local storage workers for their durable versions,
  finds the storage worker with the highest version, and attempts to read
  the system configuration from it. If any step fails, it gracefully falls
  back to the default configuration.
  """
  @spec bootstrap_from_storage(module(), [node()]) :: {non_neg_integer(), term()}
  def bootstrap_from_storage(cluster, coordinator_nodes) do
    foreman = cluster.otp_name(:foreman)

    with :ok <- Foreman.wait_for_healthy(foreman, timeout: 500),
         {:ok, storage_workers} <- Foreman.storage_workers(foreman, timeout: 200) do
      find_highest_version_storage_and_read_config(storage_workers, coordinator_nodes)
    else
      _ ->
        {0, config(coordinator_nodes)}
    end
  end

  @doc """
  Query all storage workers for durable version and try to read config from highest.

  This function implements a sequential fallback strategy where it tries storage
  workers in order of highest durable version first, falling back to the next
  highest if config reading fails.
  """
  @spec find_highest_version_storage_and_read_config([term()], [node()]) ::
          {non_neg_integer(), term()}
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
  @spec get_storage_durable_version(term()) ::
          {:ok, {term(), non_neg_integer()}} | {:error, term()}
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
  @spec try_read_config_in_sequence([{term(), non_neg_integer()}], [node()]) ::
          {non_neg_integer(), term()}
  def try_read_config_in_sequence([], coordinator_nodes), do: {0, config(coordinator_nodes)}

  def try_read_config_in_sequence([{storage_worker, durable_version} | rest], coordinator_nodes) do
    case Storage.fetch(storage_worker, "\xff/system/config", durable_version, timeout: 200) do
      {:ok, bert_data} ->
        try do
          {_stored_version, config} = :erlang.binary_to_term(bert_data)
          # Use durable_version from info API, not stored version
          {durable_version, config}
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
