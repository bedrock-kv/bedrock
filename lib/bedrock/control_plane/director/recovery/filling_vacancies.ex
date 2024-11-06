defmodule Bedrock.ControlPlane.Director.Recovery.FillingVacancies do
  alias Bedrock.ControlPlane.Config.LogDescriptor
  alias Bedrock.DataPlane.Log
  alias Bedrock.ControlPlane.Config.StorageTeamDescriptor
  alias Bedrock.DataPlane.Storage

  @doc """
  Fills vacancies in logs with log IDs that are not part of the previous
  transaction system. If there are not enough log workers to fill all vacancies,
  an error is returned.
  """
  @spec fill_log_vacancies(
          logs :: [LogDescriptor.t()],
          assigned_log_ids :: MapSet.t(Log.id()),
          all_log_ids :: MapSet.t(Log.id())
        ) ::
          {:ok, [LogDescriptor.t()]}
          | {:error, {:need_log_workers, pos_integer()}}
  def fill_log_vacancies(logs, assigned_log_ids, all_log_ids) do
    vacancies = all_vacancies(logs)
    n_vacancies = MapSet.size(vacancies)

    candidates_ids = all_log_ids |> MapSet.difference(assigned_log_ids)
    n_candidates = MapSet.size(candidates_ids)

    if n_vacancies > n_candidates do
      {:error, {:need_log_workers, n_vacancies - n_candidates}}
    else
      {:ok,
       replace_vacancies_with_log_ids(
         logs,
         Enum.zip(vacancies, candidates_ids) |> Map.new()
       )}
    end
  end

  @spec all_vacancies([LogDescriptor.t()]) :: MapSet.t()
  def all_vacancies(logs) do
    Enum.reduce(logs, [], fn
      %{log_id: {:vacancy, _} = vacancy}, list -> [vacancy | list]
      _, list -> list
    end)
    |> MapSet.new()
  end

  def replace_vacancies_with_log_ids(logs, log_id_for_vacancy) do
    logs
    |> Enum.map(fn descriptor ->
      case Map.get(log_id_for_vacancy, descriptor.log_id) do
        nil -> descriptor
        candidate_id -> LogDescriptor.put_log_id(descriptor, candidate_id)
      end
    end)
  end

  @doc """
  Fills vacancies in storage teams by assigning IDs of storage workers that are
  not currently part of the transaction system. If there are not enough storage
  workers to fill all vacancies, an error is returned.
  """
  @spec fill_storage_team_vacancies(
          storage_teams :: [StorageTeamDescriptor.t()],
          all_storage_ids :: MapSet.t(Storage.id())
        ) ::
          {:ok, [StorageTeamDescriptor.t()]}
          | {:error, {:need_storage_workers, pos_integer()}}
  def fill_storage_team_vacancies(storage_teams, all_storage_ids) do
    assigned_storage_ids =
      Enum.reduce(storage_teams, MapSet.new(), &Enum.into(&1.storage_ids, &2))

    vacancies = assigned_storage_ids |> MapSet.filter(&vacancy?/1)
    n_vacancies = MapSet.size(vacancies)

    candidate_ids = all_storage_ids |> MapSet.difference(assigned_storage_ids)
    n_candidates = MapSet.size(candidate_ids)

    if n_vacancies > n_candidates do
      {:error, {:need_storage_workers, n_vacancies - n_candidates}}
    else
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
