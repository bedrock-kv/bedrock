defmodule Bedrock.ControlPlane.Director.Recovery.VersionDeterminationPhase do
  @moduledoc """
  Determines the highest durable version across storage teams and identifies degraded teams.

  Queries all storage teams to find the highest version that is durably committed
  across the required replication factor. Storage teams that cannot meet the
  durable version are marked as degraded and will need data repair.

  The durable version represents the recovery baseline - all transactions at or
  below this version are guaranteed to be persistent. Transactions above this
  version may need to be replayed from logs.

  Degraded teams are identified early so later phases can prioritize their repair
  during data distribution. Teams missing too many replicas cannot contribute to
  the durable version calculation.

  Transitions to :create_vacancies with the established durable version and list
  of teams requiring repair.
  """

  @behaviour Bedrock.ControlPlane.Director.Recovery.RecoveryPhase

  alias Bedrock.ControlPlane.Config.StorageTeamDescriptor
  alias Bedrock.DataPlane.Storage

  import Bedrock.ControlPlane.Director.Recovery.Telemetry

  @impl true
  def execute(recovery_attempt, context) do
    determine_durable_version(
      context.old_transaction_system_layout.storage_teams,
      recovery_attempt.storage_recovery_info_by_id,
      context.cluster_config.parameters.desired_replication_factor |> determine_quorum()
    )
    |> case do
      {:error, {:insufficient_replication, _failed_tags} = reason} ->
        {recovery_attempt, {:stalled, reason}}

      {:ok, durable_version, healthy_teams, degraded_teams} ->
        trace_recovery_durable_version_chosen(durable_version)
        trace_recovery_team_health(healthy_teams, degraded_teams)

        updated_recovery_attempt =
          recovery_attempt
          |> Map.put(:durable_version, durable_version)
          |> Map.put(:degraded_teams, degraded_teams)

        {updated_recovery_attempt, Bedrock.ControlPlane.Director.Recovery.LogRecruitmentPhase}
    end
  end

  @spec determine_durable_version(
          teams :: [StorageTeamDescriptor.t()],
          info_by_id :: %{Storage.id() => Storage.recovery_info()},
          quorum :: non_neg_integer()
        ) ::
          {:ok, Bedrock.version(), healthy_teams :: [Bedrock.range_tag()],
           degraded_teams :: [Bedrock.range_tag()]}
          | {:error, {:insufficient_replication, failed_tags :: [Bedrock.range_tag()]}}
  def determine_durable_version(teams, info_by_id, quorum) do
    Enum.zip(
      teams |> Enum.map(& &1.tag),
      teams
      |> Enum.map(&determine_durable_version_and_status_for_storage_team(&1, info_by_id, quorum))
    )
    |> Enum.reduce({nil, [], [], []}, fn
      {tag, {:ok, version, :healthy}}, {min_version, healthy, degraded, failed} ->
        {smallest_version(version, min_version), [tag | healthy], degraded, failed}

      {tag, {:ok, version, :degraded}}, {min_version, healthy, degraded, failed} ->
        {smallest_version(version, min_version), healthy, [tag | degraded], failed}

      {tag, {:error, :insufficient_replication}}, {min_version, healthy, degraded, failed} ->
        {min_version, healthy, degraded, [tag | failed]}
    end)
    |> case do
      {_, _, _, [_at_least_one | _rest] = failed} -> {:error, {:insufficient_replication, failed}}
      {min_version, healthy, degraded, []} -> {:ok, min_version, healthy, degraded}
    end
  end

  @spec smallest_version(Bedrock.version() | nil, Bedrock.version() | nil) ::
          Bedrock.version() | nil
  def smallest_version(nil, b), do: b
  def smallest_version(a, b), do: min(a, b)

  @spec determine_durable_version_and_status_for_storage_team(
          team :: StorageTeamDescriptor.t(),
          info_by_id :: %{
            Storage.id() => %{
              durable_version: Bedrock.version(),
              oldest_durable_version: Bedrock.version()
            }
          },
          quorum :: non_neg_integer()
        ) ::
          {:ok, Bedrock.version(), status :: :healthy | :degraded}
          | {:error, :insufficient_replication}
  def determine_durable_version_and_status_for_storage_team(team, info_by_id, quorum) do
    durable_versions =
      team.storage_ids
      |> Enum.map(&Map.get(info_by_id, &1))
      |> Enum.reject(&is_nil/1)
      |> Enum.map(&Map.get(&1, :durable_version))

    durable_versions
    |> Enum.sort()
    |> Enum.at(-quorum)
    |> case do
      nil ->
        {:error, :insufficient_replication}

      version ->
        {:ok, version, durability_status_for_storage_team(length(durable_versions), quorum)}
    end
  end

  @spec determine_quorum(non_neg_integer()) :: pos_integer()
  defp determine_quorum(n) when is_integer(n), do: 1 + div(n, 2)

  @spec durability_status_for_storage_team(non_neg_integer(), non_neg_integer()) ::
          :healthy | :degraded
  defp durability_status_for_storage_team(durable_versions, quorum)
       when durable_versions == quorum,
       do: :healthy

  defp durability_status_for_storage_team(_, _), do: :degraded
end
