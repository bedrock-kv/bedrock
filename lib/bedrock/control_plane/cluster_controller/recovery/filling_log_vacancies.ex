defmodule Bedrock.ControlPlane.ClusterController.Recovery.FillingLogVacancies do
  alias Bedrock.ControlPlane.Config.LogDescriptor
  alias Bedrock.DataPlane.Log

  @spec fill_log_vacancies(
          logs :: [LogDescriptor.t()],
          old_logs :: [LogDescriptor.t()],
          recovery_info_by_id :: %{Log.id() => Log.recovery_info()}
        ) ::
          {:ok, [LogDescriptor.t()]}
          | {:error, :no_vacancies_to_fill}
          | {:error, :no_unassigned_logs}
  def fill_log_vacancies(logs, old_logs, recovery_info_by_id) do
    vacancies = all_vacancies(logs)

    assigned_log_ids = old_logs |> MapSet.new(& &1.log_id)

    candidates_ids =
      recovery_info_by_id
      |> Map.keys()
      |> MapSet.new()
      |> MapSet.difference(assigned_log_ids)

    cond do
      Enum.empty?(vacancies) ->
        {:error, :no_vacancies_to_fill}

      MapSet.size(candidates_ids) < MapSet.size(vacancies) ->
        {:error, :no_unassigned_logs}

      true ->
        candidate_id_for_vacancy =
          Enum.zip(vacancies, candidates_ids) |> Map.new()

        updated_logs =
          logs
          |> Enum.map(fn log_descriptor ->
            case Map.get(candidate_id_for_vacancy, log_descriptor.log_id) do
              nil -> log_descriptor
              candidate_id -> LogDescriptor.put_log_id(log_descriptor, candidate_id)
            end
          end)

        {:ok, updated_logs}
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
end
