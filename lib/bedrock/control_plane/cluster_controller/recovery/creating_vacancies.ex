defmodule Bedrock.ControlPlane.ClusterController.Recovery.CreatingVacancies do
  alias Bedrock.ControlPlane.Config.LogDescriptor
  alias Bedrock.ControlPlane.Config.StorageTeamDescriptor
  alias Bedrock.DataPlane.Storage

  @spec create_vacancies_for_logs([LogDescriptor.t()], desired_logs :: pos_integer()) ::
          [LogDescriptor.t()]
  def create_vacancies_for_logs(logs, desired_logs) do
    logs
    |> Enum.group_by(&Enum.sort(&1.tags))
    |> Enum.map(fn {tags, _descriptors} ->
      1..desired_logs
      |> Enum.map(&{:vacancy, &1})
      |> Enum.map(fn vacancy ->
        LogDescriptor.log_descriptor(vacancy, tags)
      end)
    end)
    |> List.flatten()
  end

  @type tag_set_roster ::
          %{[Bedrock.range_tag()] => [Storage.id()]}
  @type expanded_tag_set_roster ::
          %{[Bedrock.range_tag()] => [Storage.id() | StorageTeamDescriptor.vacancy()]}

  @spec create_vacancies_for_storage_teams(
          [StorageTeamDescriptor.t()],
          desired_replication :: pos_integer()
        ) :: [StorageTeamDescriptor.t()]
  def create_vacancies_for_storage_teams(storage_teams, desired_replication) do
    rosters_by_tag_set = tag_set_rosters_from_storage_teams(storage_teams)

    {expanded_rosters_by_tag_set, total_vacancies_added} =
      expand_rosters_and_add_vacancies(rosters_by_tag_set, desired_replication)

    if total_vacancies_added == 0 do
      storage_teams
    else
      apply_expanded_roster_to_storage_teams(storage_teams, expanded_rosters_by_tag_set)
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

  @spec expand_rosters_and_add_vacancies(tag_set_roster(), desired_replication :: pos_integer()) ::
          {expanded_tag_set_roster(), vacancies_added :: non_neg_integer()}
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
      &StorageTeamDescriptor.put_storage_ids(&1, Map.get(new_rosters_by_tag, &1.tag))
    )
  end
end
