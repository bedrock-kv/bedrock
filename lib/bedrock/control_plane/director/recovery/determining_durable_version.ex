defmodule Bedrock.ControlPlane.Director.Recovery.DeterminingDurableVersion do
  alias Bedrock.ControlPlane.Config.StorageTeamDescriptor
  alias Bedrock.DataPlane.Storage

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
          {:ok, Bedrock.version(), degraded_teams :: [Bedrock.range_tag()]}
          | {:error, {:insufficient_replication, failed_tags :: [Bedrock.range_tag()]}}
  def determine_durable_version(teams, info_by_id, quorum) do
    Enum.zip(
      teams |> Enum.map(& &1.tag),
      teams
      |> Enum.map(&determine_durable_version_and_status_for_storage_team(&1, info_by_id, quorum))
    )
    |> Enum.reduce({nil, [], []}, fn
      {_tag, {:ok, version, :healthy}}, {min_version, degraded, failed} ->
        {min(version, min_version), degraded, failed}

      {tag, {:ok, version, :degraded}}, {min_version, degraded, failed} ->
        {min(version, min_version), [tag | degraded], failed}

      {tag, {:error, :insufficient_replication}}, {min_version, degraded, failed} ->
        {min_version, degraded, [tag | failed]}
    end)
    |> case do
      {_, _, [_at_least_one | _rest] = failed} -> {:error, {:insufficient_replication, failed}}
      {min_version, degraded, []} -> {:ok, min_version, degraded}
    end
  end

  @doc """
  Determine the most recent durable version available among a list of storage
  servers, based on the provided quorum. It's also important that we discard
  any storage servers from consideration that do not have a full-copy (back to
  the initial transaction) of the data. We also use the quorum to determine
  whether or not the team is healthy or degraded.

  ## Parameters

    - `team`: A `StorageTeamDescriptor` struct representing a storage
      teams involved in the operation.

    - `info_by_id`: A map where each key is a storage identifier and each value
      is a map (hopefully) containing the `:durable_version` and
      `:oldest_version`.

    - `quorum`: The minimum number of storage servers that must agree on the
      version to form a consensus.

  ## Returns

    - `{:ok, durable_version, status}`: The most recent durable version of the
      storage where consensus was reached and an indicator of the team's status,
      both based on the `quorum`.

    - `{:error, :insufficient_replication}`: Indicates that there aren't enough
      storage servers available to meet the quorum requirements for a consensus
      on the durable version.
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
      |> Enum.filter(&(Map.get(&1, :oldest_durable_version) == 0))
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

  defp durability_status_for_storage_team(durable_versions, quorum)
       when durable_versions == quorum,
       do: :healthy

  defp durability_status_for_storage_team(_, _), do: :degraded
end
