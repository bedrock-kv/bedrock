defmodule Bedrock.ControlPlane.ClusterController.Recovery.FillingLogVacancies do
  alias Bedrock.ControlPlane.Config.LogDescriptor
  alias Bedrock.DataPlane.Log

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
end
