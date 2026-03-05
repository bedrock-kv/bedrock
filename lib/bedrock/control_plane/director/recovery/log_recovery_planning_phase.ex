defmodule Bedrock.ControlPlane.Director.Recovery.LogRecoveryPlanningPhase do
  @moduledoc """
  Determines which logs from the previous layout should be copied to preserve committed transactions,
  and establishes the minimum durable version across all logs.

  Runs when logs existed in the previous layout. Uses simple majority quorum - if more than half
  of the logs from the previous layout are available (locked), recovery can proceed. The version
  vector is computed as `{max(oldest), min(newest)}` across all available logs.

  Also calculates `durable_version` as the minimum of all logs' `minimum_durable_version` values,
  representing the highest version guaranteed to be persisted to durable storage (ObjectStorage)
  via Demux across all logs.

  With consistent hashing, shard→log mapping is computed (not stored), so per-shard quorum
  validation is no longer needed. Replacement logs can reconstruct their personalized transaction
  stream by pulling from all survivors and filtering by the shard index embedded in transactions.

  Stalls with `:unable_to_meet_log_quorum` if majority quorum cannot be established
  or no valid version range exists.

  Transitions to log recruitment with the selected logs, version vector, and durable version.

  """

  use Bedrock.ControlPlane.Director.Recovery.RecoveryPhase

  import Bedrock.ControlPlane.Director.Recovery.Telemetry

  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.Version

  @impl true
  def execute(%RecoveryAttempt{} = recovery_attempt, context) do
    old_log_ids = Map.keys(context.old_transaction_system_layout[:logs] || %{})
    log_recovery_info = recovery_attempt.log_recovery_info_by_id
    locked_count = map_size(log_recovery_info)
    total_count = length(old_log_ids)

    # Simple majority quorum
    if locked_count > total_count / 2 do
      case compute_version_vector(log_recovery_info) do
        {:ok, version_vector} ->
          survivor_ids = Map.keys(log_recovery_info)
          trace_recovery_suitable_logs_chosen(survivor_ids, version_vector)

          durable_version = calculate_durable_version(log_recovery_info)

          updated_recovery_attempt =
            recovery_attempt
            |> Map.put(:old_log_ids_to_copy, survivor_ids)
            |> Map.put(:survivor_log_ids, survivor_ids)
            |> Map.put(:version_vector, version_vector)
            |> Map.put(:durable_version, durable_version)

          {updated_recovery_attempt, Bedrock.ControlPlane.Director.Recovery.LogRecruitmentPhase}

        {:error, :invalid_version_range} ->
          {recovery_attempt, {:stalled, :unable_to_meet_log_quorum}}
      end
    else
      {recovery_attempt, {:stalled, :unable_to_meet_log_quorum}}
    end
  end

  @doc """
  Computes the version vector from log recovery info.

  Returns `{max(oldest), min(newest)}` across all logs, which represents the
  range of transactions that are guaranteed to be complete across all survivors.
  """
  @spec compute_version_vector(%{Log.id() => Log.recovery_info()}) ::
          {:ok, Bedrock.version_vector()} | {:error, :invalid_version_range}
  def compute_version_vector(log_recovery_info) when map_size(log_recovery_info) == 0 do
    {:error, :invalid_version_range}
  end

  def compute_version_vector(log_recovery_info) do
    {max_oldest, min_newest} =
      log_recovery_info
      |> Map.values()
      |> Enum.reduce({Version.zero(), nil}, fn info, {current_max_oldest, current_min_newest} ->
        oldest = info[:oldest_version]
        newest = info[:last_version]

        new_max_oldest = if oldest > current_max_oldest, do: oldest, else: current_max_oldest

        new_min_newest =
          cond do
            current_min_newest == nil -> newest
            newest < current_min_newest -> newest
            true -> current_min_newest
          end

        {new_max_oldest, new_min_newest}
      end)

    # Validate the range
    if valid_range?({max_oldest, min_newest}) do
      {:ok, {max_oldest, min_newest}}
    else
      {:error, :invalid_version_range}
    end
  end

  @doc """
  Validates a version range is valid (oldest <= newest, with special handling for zero).
  """
  @spec valid_range?(Bedrock.version_vector()) :: boolean()
  def valid_range?({oldest, newest}) do
    zero_version = Version.zero()

    cond do
      oldest == zero_version -> true
      newest == zero_version -> false
      true -> newest >= oldest
    end
  end

  @doc """
  Calculates the minimum durable version across all logs.

  Takes the minimum of all available `minimum_durable_version` values from
  log recovery info. Returns `Version.zero()` if no logs have durability info.
  """
  @spec calculate_durable_version(%{Log.id() => Log.recovery_info()}) :: Bedrock.version()
  def calculate_durable_version(log_recovery_info) do
    log_recovery_info
    |> Map.values()
    |> Enum.map(&Map.get(&1, :minimum_durable_version))
    |> Enum.reject(&(&1 == :unavailable))
    |> Enum.min(fn -> Version.zero() end)
  end
end
