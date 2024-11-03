defmodule Bedrock.ControlPlane.Director.Recovery.FillingVacancies do
  alias Bedrock.ControlPlane.Config.LogDescriptor
  alias Bedrock.DataPlane.Log
  alias Bedrock.ControlPlane.Config.StorageTeamDescriptor
  alias Bedrock.DataPlane.Storage

  @doc """
  Fills vacancies in logs with available log IDs.

  Takes a list of logs and assigned log IDs, and attempts to fill
  any vacancies with unassigned log IDs from the pool of all log IDs.

  ## Parameters

    - logs: List of `LogDescriptor` structs representing logs.
    - assigned_log_ids: A `MapSet` of log IDs that are already assigned.
    - all_log_ids: A `MapSet` of all possible log IDs.

  ## Returns

    - `{:ok, updated_logs}` if vacancies are filled successfully.
    - `{:error, :no_vacancies_to_fill}` if there are no vacancies to fill.
    - `{:error, {:need_log_workers, num}}` if there are not enough log IDs to fill all vacancies.

  """
  @spec fill_log_vacancies(
          logs :: [LogDescriptor.t()],
          assigned_log_ids :: MapSet.t(Log.id()),
          all_log_ids :: MapSet.t(Log.id())
        ) ::
          {:ok, [LogDescriptor.t()]}
          | {:error, :no_vacancies_to_fill}
          | {:error, {:need_log_workers, pos_integer()}}
  def fill_log_vacancies(logs, assigned_log_ids, all_log_ids) do
    vacancies = all_vacancies(logs)
    n_vacancies = MapSet.size(vacancies)

    candidates_ids = all_log_ids |> MapSet.difference(assigned_log_ids)
    n_candidates = MapSet.size(candidates_ids)

    cond do
      0 == n_vacancies ->
        {:error, :no_vacancies_to_fill}

      n_vacancies > n_candidates ->
        {:error, {:need_log_workers, n_vacancies - n_candidates}}

      true ->
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
  Fills vacancies in storage teams with available storage IDs.

  This function attempts to fill storage team vacancies by assigning
  unassigned storage IDs to the vacancies present in the storage teams.

  ## Parameters

    - storage_teams: A list of `StorageTeamDescriptor` structs representing the current
      state of storage teams, some of which may have vacancies.
    - all_storage_ids: A `MapSet` containing all available storage IDs that can be assigned
      to fill vacancies.

  ## Returns

    - `{:ok, filled_teams}`: A tuple indicating successful filling of vacancies, with
      `filled_teams` representing the updated list of storage team descriptors with
      no vacancies.
    - `{:error, :no_vacancies_to_fill}`: An error indicating that there are no vacancies
      to fill in the storage teams.
    - `{:error, {:need_storage_workers, n}}`: An error indicating that there are not enough
      available storage workers to fill existing vacancies, with `n` being the number
      of additional storage IDs needed.

  """
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
