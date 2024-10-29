defmodule Bedrock.ControlPlane.ClusterController.Recovery.FillingLogVacancies do
  alias Bedrock.ControlPlane.Config.LogDescriptor
  alias Bedrock.DataPlane.Log

  @spec fill_log_vacancies(
          logs :: [LogDescriptor.t()],
          old_logs :: [LogDescriptor.t()],
          recovery_info_by_id :: %{Log.id() => Log.recovery_info()}
        ) ::
          {:ok, [LogDescriptor.t()]} | {:error, :no_unassigned_logs | :no_vacancies_to_fill}
  def fill_log_vacancies(logs, old_logs, recovery_info_by_id) do
    vacancies = all_vacancies(logs)

    if Enum.empty?(vacancies) do
      {:error, :no_vacancies_to_fill}
    else
      old_log_ids = old_logs |> Enum.map(& &1.log_id) |> Enum.uniq()

      candidates_by_id =
        recovery_info_by_id |> Map.drop(old_log_ids)

      if map_size(candidates_by_id) < MapSet.size(vacancies) do
        {:error, :no_unassigned_logs}
      else
        candidate_id_for_vacancy =
          Enum.zip(vacancies, Map.keys(candidates_by_id)) |> Map.new()

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
  end

  @spec all_vacancies([LogDescriptor.t()]) :: MapSet.t()
  def all_vacancies(logs) do
    Enum.reduce(logs, [], fn
      %{log_id: {:vacancy, _} = vacancy}, list -> [vacancy | list]
      _, list -> list
    end)
    |> MapSet.new()
  end

  @spec log_ids_in_use([LogDescriptor.t()]) :: [Log.id()]
  def log_ids_in_use(logs),
    do: logs |> Enum.group_by(& &1.log_id, & &1.tags) |> Map.keys()

  def find_candidate_log_info(log_info, assigned_ids),
    do: log_info |> Enum.reject(fn {id, _} -> id in assigned_ids end)

  def vacancies?(log_ids), do: Enum.member?(log_ids, :vacant)
end
