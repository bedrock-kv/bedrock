defmodule Bedrock.ControlPlane.ClusterController.Recovery.FillingStorageTeamVacancies do
  alias Bedrock.ControlPlane.Config.StorageTeamDescriptor
  alias Bedrock.DataPlane.Storage

  @spec fill_storage_team_vacancies(
          storage_teams :: [StorageTeamDescriptor.t()],
          all_storage_ids :: MapSet.t(Storage.id())
        ) ::
          {:ok, [StorageTeamDescriptor.t()]}
          | {:error, :no_vacancies_to_fill}
          | {:error, {:need_storage_workers, pos_integer()}}
  def fill_storage_team_vacancies(storage_teams, all_storage_ids) do
    assigned_storage_ids =
      Enum.reduce(storage_teams, MapSet.new(), &Enum.into(&1.storage_ids, &2))

    vacancies = assigned_storage_ids |> MapSet.filter(&vacancy?/1)
    n_vacancies = MapSet.size(vacancies)

    candidate_ids = all_storage_ids |> MapSet.difference(assigned_storage_ids)
    n_candidates = MapSet.size(candidate_ids)

    cond do
      0 == n_vacancies ->
        {:error, :no_vacancies_to_fill}

      n_vacancies > n_candidates ->
        {:error, {:need_storage_workers, n_vacancies - n_candidates}}

      true ->
        {:ok,
         replace_vacancies_with_storage_ids(
           storage_teams,
           vacancies |> Enum.zip(candidate_ids) |> Map.new()
         )}
    end
  end

  @spec vacancy?(Storage.id() | StorageTeamDescriptor.vacancy()) :: boolean()
  def vacancy?({:vacancy, _}), do: true
  def vacancy?(_), do: false

  @spec replace_vacancies_with_storage_ids(
          storage_teams :: [StorageTeamDescriptor.t()],
          storage_id_for_vacancy :: %{StorageTeamDescriptor.vacancy() => Storage.id()}
        ) :: [StorageTeamDescriptor.t()]
  def replace_vacancies_with_storage_ids(storage_teams, storage_id_for_vacancy) do
    storage_teams
    |> Enum.map(fn descriptor ->
      descriptor
      |> StorageTeamDescriptor.update_storage_ids(fn storage_ids ->
        storage_ids
        |> Enum.map(&Map.get(storage_id_for_vacancy, &1, &1))
      end)
    end)
  end
end
