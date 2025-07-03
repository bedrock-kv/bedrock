defmodule Bedrock.ControlPlane.Director.Recovery.FillingVacancies do
  alias Bedrock.ControlPlane.Config.LogDescriptor
  alias Bedrock.DataPlane.Log
  alias Bedrock.ControlPlane.Config.StorageTeamDescriptor
  alias Bedrock.DataPlane.Storage

  @doc """
  Fills vacancies in logs with log IDs that are not part of the previous
  transaction system. If there are not enough log workers to fill all vacancies,
  it will attempt to create new workers on available nodes.
  """
  @spec fill_log_vacancies(
          logs :: %{Log.id() => LogDescriptor.t()},
          assigned_log_ids :: MapSet.t(Log.id()),
          all_log_ids :: MapSet.t(Log.id()),
          available_nodes :: [node()]
        ) ::
          {:ok, %{Log.id() => LogDescriptor.t()}, [Log.id()]}
          | {:error, term()}
  def fill_log_vacancies(logs, assigned_log_ids, all_log_ids, available_nodes) do
    vacancies = all_vacancies(logs)
    n_vacancies = MapSet.size(vacancies)

    candidates_ids = all_log_ids |> MapSet.difference(assigned_log_ids)
    n_candidates = MapSet.size(candidates_ids)

    if n_vacancies <= n_candidates do
      # We have enough existing workers
      {:ok,
       replace_vacancies_with_log_ids(
         logs,
         Enum.zip(vacancies, candidates_ids) |> Map.new()
       ), []}
    else
      # We need to create new workers
      needed_workers = n_vacancies - n_candidates

      if length(available_nodes) < needed_workers do
        {:error, {:insufficient_nodes, needed_workers, length(available_nodes)}}
      else
        # Create new worker IDs
        new_worker_ids =
          1..needed_workers
          |> Enum.map(&"log_#{System.unique_integer([:positive])}_#{&1}")

        # Use existing candidates plus new worker IDs
        all_worker_ids = Enum.concat(candidates_ids, new_worker_ids)

        {:ok,
         replace_vacancies_with_log_ids(
           logs,
           Enum.zip(vacancies, all_worker_ids) |> Map.new()
         ), new_worker_ids}
      end
    end
  end

  @spec all_vacancies(%{Log.id() => LogDescriptor.t()}) :: MapSet.t()
  def all_vacancies(logs) do
    Enum.reduce(logs, [], fn
      {{:vacancy, _} = vacancy, _}, list -> [vacancy | list]
      _, list -> list
    end)
    |> MapSet.new()
  end

  @spec replace_vacancies_with_log_ids(
          logs :: %{Log.id() => LogDescriptor.t()},
          log_id_for_vacancy :: %{LogDescriptor.vacancy() => Log.id()}
        ) :: %{Log.id() => LogDescriptor.t()}
  def replace_vacancies_with_log_ids(logs, log_id_for_vacancy) do
    logs
    |> Enum.map(fn {log_id, descriptor} ->
      case Map.get(log_id_for_vacancy, log_id) do
        nil -> {log_id, descriptor}
        candidate_id -> {candidate_id, descriptor}
      end
    end)
    |> Map.new()
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
      |> Map.update!(:storage_ids, fn storage_ids ->
        storage_ids
        |> Enum.map(&Map.get(storage_id_for_vacancy, &1, &1))
      end)
    end)
  end
end
