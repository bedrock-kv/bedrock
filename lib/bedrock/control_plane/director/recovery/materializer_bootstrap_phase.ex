defmodule Bedrock.ControlPlane.Director.Recovery.MaterializerBootstrapPhase do
  @moduledoc """
  Bootstraps the metadata shard materializer for recovery.

  The metadata materializer holds the authoritative shard layout - the mapping from
  key ranges to shard tags. This phase ensures the materializer is available and
  queries it for the current shard layout.

  ## Fresh Cluster

  For a fresh cluster (no old logs), creates a default shard layout with two shards:
  - System shard (tag 0): Keys from 0xFF to end-of-keyspace (system metadata)
  - User shard (tag 1): Keys from empty string to 0xFF (user data)

  ## Existing Cluster

  For an existing cluster, finds the metadata materializer in available_services,
  locks it for recovery, and queries the shard layout via get_range on
  \\xff/system/shard_keys/*.

  Stalls if the materializer is unavailable for an existing cluster since the shard
  layout is required for proper transaction routing.

  Transitions to CommitProxyStartupPhase with the materializer pid and shard layout.
  """

  use Bedrock.ControlPlane.Director.Recovery.RecoveryPhase

  import Bedrock, only: [end_of_keyspace: 0]

  alias Bedrock.ControlPlane.Director.Recovery.CommitProxyStartupPhase
  alias Bedrock.DataPlane.Storage

  require Logger

  @impl true
  def execute(%RecoveryAttempt{} = recovery_attempt, context) do
    if fresh_cluster?(context) do
      handle_fresh_cluster(recovery_attempt)
    else
      handle_existing_cluster(recovery_attempt, context)
    end
  end

  @doc """
  Returns the default shard layout for a fresh cluster.

  Layout has two shards:
  - Tag 0: System keys (0xFF to end_of_keyspace)
  - Tag 1: User keys (empty string to 0xFF)

  The map is keyed by end_key, with values of {tag, start_key}.
  """
  @spec default_shard_layout() :: RecoveryAttempt.shard_layout()
  def default_shard_layout do
    %{
      # User shard: "" to 0xFF
      <<0xFF>> => {1, <<>>},
      # System shard: 0xFF to end
      end_of_keyspace() => {0, <<0xFF>>}
    }
  end

  # Private implementation

  defp fresh_cluster?(%{old_transaction_system_layout: %{logs: logs}}) when map_size(logs) == 0, do: true

  defp fresh_cluster?(_context), do: false

  defp handle_fresh_cluster(recovery_attempt) do
    Logger.debug("Fresh cluster detected, using default shard layout")

    updated_attempt =
      recovery_attempt
      |> Map.put(:metadata_materializer, nil)
      |> Map.put(:shard_layout, default_shard_layout())

    {updated_attempt, CommitProxyStartupPhase}
  end

  defp handle_existing_cluster(recovery_attempt, context) do
    with {:ok, materializer_service} <- find_materializer_service(context),
         {:ok, materializer_pid} <- lock_materializer(materializer_service, recovery_attempt.epoch, context),
         {:ok, shard_layout} <- get_shard_layout(materializer_pid, context) do
      updated_attempt =
        recovery_attempt
        |> Map.put(:metadata_materializer, materializer_pid)
        |> Map.put(:shard_layout, shard_layout)

      {updated_attempt, CommitProxyStartupPhase}
    else
      {:error, reason} ->
        {recovery_attempt, {:stalled, reason}}
    end
  end

  defp find_materializer_service(%{available_services: services}) do
    case Map.get(services, "metadata_materializer") do
      nil -> {:error, {:materializer_unavailable, :not_in_available_services}}
      service -> {:ok, service}
    end
  end

  defp lock_materializer(service, epoch, context) do
    lock_fn = Map.get(context, :lock_materializer_fn, &default_lock_materializer/2)
    lock_fn.(service, epoch)
  end

  defp default_lock_materializer({:storage, name}, epoch) do
    name
    |> Storage.lock_for_recovery(epoch)
    |> case do
      {:ok, pid, _info} -> {:ok, pid}
      {:error, reason} -> {:error, {:materializer_lock_failed, reason}}
    end
  end

  defp get_shard_layout(materializer_pid, context) do
    get_layout_fn = Map.get(context, :get_shard_layout_fn, &default_get_shard_layout/1)
    get_layout_fn.(materializer_pid)
  end

  defp default_get_shard_layout(materializer_pid) do
    # Read at maximum possible version to get latest shard layout
    max_version = <<0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF>>

    # Query the materializer for shard layout via get_range on shard_keys prefix
    prefix = Bedrock.SystemKeys.shard_keys_prefix()
    end_key = prefix <> <<0xFF, 0xFF, 0xFF, 0xFF>>

    case Storage.get_range(materializer_pid, prefix, end_key, max_version, limit: 1000) do
      {:ok, entries} ->
        shard_layout =
          Map.new(entries, fn {key, value} ->
            # Key format: \xff/system/shard_keys/<end_key>
            # Value format: {tag, start_key}
            end_key = extract_end_key_from_shard_key(key)
            {tag, start_key} = decode_shard_value(value)
            {end_key, {tag, start_key}}
          end)

        {:ok, shard_layout}

      {:error, reason} ->
        {:error, {:shard_layout_query_failed, reason}}
    end
  end

  defp extract_end_key_from_shard_key(key) do
    prefix = Bedrock.SystemKeys.shard_keys_prefix()
    prefix_len = byte_size(prefix)
    binary_part(key, prefix_len, byte_size(key) - prefix_len)
  end

  defp decode_shard_value(value) when is_binary(value) do
    :erlang.binary_to_term(value)
  end
end
