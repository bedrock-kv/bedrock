defmodule Bedrock.ControlPlane.Director.Recovery.StorageRecruitmentPhase do
  @moduledoc """
  Handles the :recruit_storage_to_fill_vacancies phase of recovery.

  This phase is responsible for filling storage team vacancies with
  available storage workers.
  """

  alias Bedrock.DataPlane.Storage
  alias Bedrock.ControlPlane.Config.StorageTeamDescriptor

  import Bedrock.ControlPlane.Director.Recovery.Telemetry

  @doc """
  Execute the storage recruitment phase of recovery.

  Fills storage team vacancies with available storage workers.
  """
  @spec execute(map()) :: map()
  def execute(%{state: :recruit_storage_to_fill_vacancies} = recovery_attempt) do
    fill_storage_team_vacancies(
      recovery_attempt.storage_teams,
      recovery_attempt.storage_recovery_info_by_id |> Map.keys() |> MapSet.new()
    )
    |> case do
      {:error, {:need_storage_workers, _} = reason} ->
        recovery_attempt |> Map.put(:state, {:stalled, reason})

      {:ok, storage_teams} ->
        trace_recovery_all_storage_team_vacancies_filled()

        recovery_attempt
        |> Map.put(:storage_teams, storage_teams)
        |> Map.put(:state, :replay_old_logs)
    end
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
