defmodule Bedrock.ControlPlane.ClusterController.Recovery.FillingStorageTeamVacancies do
  alias Bedrock.ControlPlane.Config.StorageTeamDescriptor
  alias Bedrock.DataPlane.Storage

  @spec fill_storage_team_vacancies(
          storage_teams :: [StorageTeamDescriptor.t()],
          recovery_info_by_id :: %{Storage.id() => Storage.recovery_info()}
        ) ::
          {:ok, [StorageTeamDescriptor.t()]}
          | {:error, :no_vacancies_to_fill}
          | {:error, :no_unassigned_storage}
  def fill_storage_team_vacancies(storage_teams, recovery_info_by_id) do
    assigned_storage_ids =
      Enum.reduce(storage_teams, MapSet.new(), &Enum.into(&1.storage_ids, &2))

    vacancies = assigned_storage_ids |> MapSet.filter(&vacancy?/1)

    candidate_ids =
      recovery_info_by_id
      |> Map.keys()
      |> MapSet.new()
      |> MapSet.difference(assigned_storage_ids)

    cond do
      Enum.empty?(vacancies) ->
        {:error, :no_vacancies_to_fill}

      MapSet.size(candidate_ids) < MapSet.size(vacancies) ->
        {:error, :no_unassigned_storage}

      true ->
        candidate_id_for_vacancy =
          vacancies
          |> Enum.zip(candidate_ids)
          |> Map.new()

        updated_storage_teams =
          storage_teams
          |> Enum.map(fn descriptor ->
            descriptor
            |> StorageTeamDescriptor.update_storage_ids(fn storage_ids ->
              storage_ids
              |> Enum.map(&Map.get(candidate_id_for_vacancy, &1, &1))
            end)
          end)

        {:ok, updated_storage_teams}
    end
  end

  def vacancy?({:vacancy, _}), do: true
  def vacancy?(_), do: false
end
