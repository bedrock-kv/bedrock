defmodule Bedrock.ControlPlane.Director.Recovery.VacancyCreationPhase do
  @moduledoc """
  Handles the :create_vacancies phase of recovery.

  This phase is responsible for creating vacancies for logs and storage teams
  to ensure the desired replication levels are met.

  See: [Recovery Guide](docs/knowledge_base/01-guides/recovery-guide.md#recovery-process)
  """

  @behaviour Bedrock.ControlPlane.Director.Recovery.RecoveryPhase

  alias Bedrock.ControlPlane.Config.LogDescriptor
  alias Bedrock.ControlPlane.Config.StorageTeamDescriptor
  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.Storage

  import Bedrock.ControlPlane.Director.Recovery.Telemetry

  @doc """
  Execute the vacancy creation phase of recovery.

  Creates vacancies for both logs and storage teams based on the desired
  configuration parameters.
  """

  @impl true
  def execute(%{state: :create_vacancies} = recovery_attempt, _context) do
    with {:ok, logs, n_log_vacancies} <-
           create_vacancies_for_logs(
             recovery_attempt.last_transaction_system_layout.logs,
             recovery_attempt.parameters.desired_logs
           ),
         {:ok, storage_teams, n_storage_team_vacancies} <-
           create_vacancies_for_storage_teams(
             recovery_attempt.last_transaction_system_layout.storage_teams,
             recovery_attempt.parameters.desired_replication_factor
           ) do
      trace_recovery_creating_vacancies(n_log_vacancies, n_storage_team_vacancies)

      recovery_attempt
      |> Map.put(:logs, logs)
      |> Map.put(:storage_teams, storage_teams)
      |> Map.put(:state, :determine_durable_version)
    end
  end

  @doc """
  Creates vacancies for the specified logs to ensure the desired number of log instances.

  This function takes a list of logs and a desired number of logs, generating
  the necessary "vacancy" entries to reach the desired count for each distinct
  set of log tags. Each vacancy is represented as a `LogDescriptor` with the
  placeholder data.
  """
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

  @doc """
  Creates vacancies for the given storage teams to achieve the desired replication.

  This function calculates the necessary vacancies needed to reach the desired
  replication factor for storage teams with specific tags. If no vacancies are
  needed (i.e., if the current storage teams already meet the desired replication),
  the original storage teams list is returned.
  """
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
