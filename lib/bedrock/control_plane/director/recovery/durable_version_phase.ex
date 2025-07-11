defmodule Bedrock.ControlPlane.Director.Recovery.DurableVersionPhase do
  @moduledoc """
  Handles the :determine_durable_version phase of recovery.

  This phase is responsible for determining the highest durable version
  across all storage teams and identifying which teams are healthy vs degraded.
  """

  @behaviour Bedrock.ControlPlane.Director.Recovery.RecoveryPhase

  alias Bedrock.ControlPlane.Config.StorageTeamDescriptor
  alias Bedrock.DataPlane.Storage

  import Bedrock.ControlPlane.Director.Recovery.Telemetry

  @doc """
  Execute the durable version determination phase of recovery.

  Analyzes storage team health and determines the highest version that
  can be considered durably committed across the cluster.
  """
  @impl true
  def execute(%{state: :determine_durable_version} = recovery_attempt, _context) do
    determine_durable_version(
      recovery_attempt.last_transaction_system_layout.storage_teams,
      recovery_attempt.storage_recovery_info_by_id,
      recovery_attempt.parameters.desired_replication_factor |> determine_quorum()
    )
    |> case do
      {:error, {:insufficient_replication, _failed_tags} = reason} ->
        recovery_attempt |> Map.put(:state, {:stalled, reason})

      {:ok, durable_version, healthy_teams, degraded_teams} ->
        trace_recovery_durable_version_chosen(durable_version)
        trace_recovery_team_health(healthy_teams, degraded_teams)

        recovery_attempt
        |> Map.put(:durable_version, durable_version)
        |> Map.put(:degraded_teams, degraded_teams)
        |> Map.put(:state, :recruit_logs_to_fill_vacancies)
    end
  end

  @doc """
  Determines the latest durable version for a given set of storage teams.

  For each storage team, the function checks if a quorum of storage nodes
  agree on a version that should be considered the latest durable version.
  If any storage team does not have enough supporting nodes to reach a
  quorum, it is classified as failed.

  ## Parameters

    - `teams`: A list of `StorageTeamDescriptor` structs, each representing a
      storage team whose durable version needs to be determined.

    - `info_by_id`: A map where each storage identifier is mapped to its
      `durable_version` and `oldest_version` information.

    - `quorum`: The minimum number of nodes that must agree on a durable
      version for consensus.

  ## Returns

    - `{:ok, durable_version, degraded_teams}`: On successful determination of
      the durable version, where `durable_version` is the latest version
      agreed upon, and `degraded_teams` lists teams not reaching the full
      healthy quorum.

    - `{:error, {:insufficient_replication, failed_tags}}`: If any storage team
      lacks sufficient storage servers to meet the quorum requirements. We
      return the full set of failed tags in this case.
  """
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

  def smallest_version(nil, b), do: b
  def smallest_version(a, b), do: min(a, b)

  @doc """
  Determine the most recent durable version available among a list of storage
  servers, based on the provided quorum. It's also important that we discard
  any storage servers from consideration that do not have a full-copy (back to
  the initial transaction) of the data. We also use the quorum to determine
  whether or not the team is healthy or degraded.

  """
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

  defp determine_quorum(n) when is_integer(n), do: 1 + div(n, 2)

  defp durability_status_for_storage_team(durable_versions, quorum)
       when durable_versions == quorum,
       do: :healthy

  defp durability_status_for_storage_team(_, _), do: :degraded
end
