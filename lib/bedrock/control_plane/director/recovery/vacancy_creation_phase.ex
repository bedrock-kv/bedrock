defmodule Bedrock.ControlPlane.Director.Recovery.VacancyCreationPhase do
  @moduledoc """
  Creates vacancy placeholders for logs and storage teams to meet desired replication levels.

  Analyzes current log and storage configurations against desired counts and
  replication factors. Creates vacancy entries for missing services that need
  to be recruited by later phases.

  Vacancies are placeholder entries that mark where new services should be
  assigned. This separation allows recruitment phases to see all available
  services before making optimal placement decisions.

  For logs, creates vacancies when the current count is below the desired
  number. For storage teams, creates vacancies when teams have insufficient
  replicas to meet the replication factor.

  Always succeeds since it only modifies in-memory structures. Transitions to
  :recruit_logs_to_fill_vacancies to begin service assignment.
  """

  @behaviour Bedrock.ControlPlane.Director.Recovery.RecoveryPhase

  alias Bedrock.ControlPlane.Config.LogDescriptor
  alias Bedrock.ControlPlane.Config.StorageTeamDescriptor
  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.Storage

  import Bedrock.ControlPlane.Director.Recovery.Telemetry

  @impl true
  def execute(recovery_attempt, context) do
    with {:ok, logs, n_log_vacancies} <-
           create_vacancies_for_logs(
             context.old_transaction_system_layout.logs,
             context.cluster_config.parameters.desired_logs
           ),
         {:ok, storage_teams, n_storage_team_vacancies} <-
           create_vacancies_for_storage_teams(
             context.old_transaction_system_layout.storage_teams,
             context.cluster_config.parameters.desired_replication_factor
           ) do
      trace_recovery_creating_vacancies(n_log_vacancies, n_storage_team_vacancies)

      updated_recovery_attempt =
        recovery_attempt
        |> Map.put(:logs, logs)
        |> Map.put(:storage_teams, storage_teams)

      {updated_recovery_attempt, Bedrock.ControlPlane.Director.Recovery.VersionDeterminationPhase}
    end
  end

  @spec create_vacancies_for_logs(%{Log.id() => LogDescriptor.t()}, pos_integer()) ::
          {:ok, %{Log.id() => LogDescriptor.t()}, non_neg_integer()}
  def create_vacancies_for_logs(logs, desired_logs) do
    {updated_logs, _} =
      logs
      |> Enum.group_by(&Enum.sort(elem(&1, 1)), &elem(&1, 0))
      |> Enum.reduce({%{}, 1}, fn {tags, _ids}, {acc_map, vacancy_counter} ->
        new_vacancies =
          vacancy_counter..(vacancy_counter + desired_logs - 1)
          |> Enum.map(&{{:vacancy, &1}, tags})
          |> Map.new()

        {Map.merge(acc_map, new_vacancies), vacancy_counter + desired_logs}
      end)

    {:ok, updated_logs, map_size(updated_logs)}
  end

  @type tag_set_roster ::
          %{[Bedrock.range_tag()] => [Storage.id()]}
  @type expanded_tag_set_roster ::
          %{[Bedrock.range_tag()] => [Storage.id() | StorageTeamDescriptor.vacancy()]}

  @spec create_vacancies_for_storage_teams(
          [StorageTeamDescriptor.t()],
          pos_integer()
        ) :: {:ok, [StorageTeamDescriptor.t()], non_neg_integer()}
  def create_vacancies_for_storage_teams(storage_teams, desired_replication) do
    rosters_by_tag_set = tag_set_rosters_from_storage_teams(storage_teams)

    case expand_rosters_and_add_vacancies(rosters_by_tag_set, desired_replication) do
      {_expanded_rosters_by_tag_set, 0} ->
        {:ok, storage_teams, 0}

      {expanded_rosters_by_tag_set, n_storage_team_vacancies} ->
        {:ok, apply_expanded_roster_to_storage_teams(storage_teams, expanded_rosters_by_tag_set),
         n_storage_team_vacancies}
    end
  end

  @spec tag_set_rosters_from_storage_teams([StorageTeamDescriptor.t()]) ::
          %{[tag :: Bedrock.range_tag()] => [Storage.id()]}
  def tag_set_rosters_from_storage_teams(storage_teams) do
    storage_teams
    |> Enum.flat_map(fn storage_team ->
      Enum.map(storage_team.storage_ids, &{&1, storage_team.tag})
    end)
    |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))
    |> Enum.group_by(&Enum.sort(elem(&1, 1)), &elem(&1, 0))
  end

  @spec expand_rosters_and_add_vacancies(tag_set_roster(), pos_integer()) ::
          {expanded_tag_set_roster(), non_neg_integer()}
  def expand_rosters_and_add_vacancies(rosters_by_tag_set, desired_replication) do
    rosters_by_tag_set
    |> Enum.reduce(
      {[], 0},
      fn {tags, storage_ids}, {expanded_sets, vacancy_count} ->
        n_vacancies = desired_replication - Enum.count(storage_ids)

        if n_vacancies > 0 do
          new_vacancy_count = vacancy_count + n_vacancies - 1
          new_vacancies = vacancy_count..new_vacancy_count |> Enum.map(&{:vacancy, &1})
          {[{tags, new_vacancies ++ storage_ids} | expanded_sets], new_vacancy_count}
        else
          {[{tags, storage_ids} | expanded_sets], vacancy_count}
        end
      end
    )
    |> then(fn {expanded_sets, vacancy_count} ->
      {expanded_sets |> Map.new(), vacancy_count}
    end)
  end

  @spec apply_expanded_roster_to_storage_teams([StorageTeamDescriptor.t()], tag_set_roster()) ::
          [StorageTeamDescriptor.t()]
  def apply_expanded_roster_to_storage_teams(storage_teams, expanded_rosters_by_tag_set) do
    new_rosters_by_tag =
      expanded_rosters_by_tag_set
      |> Enum.flat_map(fn {tags, storage_ids} ->
        Enum.map(tags, fn tag ->
          {tag, storage_ids |> Enum.sort()}
        end)
      end)
      |> Map.new()

    Enum.map(
      storage_teams,
      &Map.put(&1, :storage_ids, Map.get(new_rosters_by_tag, &1.tag))
    )
  end
end
