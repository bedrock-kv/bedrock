defmodule Bedrock.ControlPlane.ClusterController.Recovery.FillingVacancies do
  alias Bedrock.ControlPlane.Config.LogDescriptor
  alias Bedrock.ControlPlane.Config.StorageTeamDescriptor
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.DataPlane.Log

  @spec try_to_fill_log_vacancies(
          TransactionSystemLayout.t(),
          [{Log.id(), [Bedrock.range_tag()]}]
        ) ::
          {:ok, TransactionSystemLayout.t()}
  def try_to_fill_log_vacancies(transaction_system_layout, []),
    do: {:ok, transaction_system_layout}

  def try_to_fill_log_vacancies(transaction_system_layout, log_info) do
    with log_ids_in_use <- log_ids_in_use(transaction_system_layout.logs),
         true <- vacancies?(log_ids_in_use) || :no_vacancies_to_fill,
         assigned_ids <- log_ids_in_use |> Enum.reject(&(&1 == :vacant)),
         candidates <- log_info |> find_candidate_log_info(assigned_ids),
         true <- not Enum.empty?(candidates) || :no_unassigned_logs do
      IO.inspect({assigned_ids, candidates})
      {:ok, transaction_system_layout}
    else
      reason ->
        IO.inspect(reason)
        {:ok, transaction_system_layout}
    end
  end

  @spec try_to_fill_storage_team_vacancies(
          TransactionSystemLayout.t(),
          [StorageTeamDescriptor.t()]
        ) ::
          {:ok, TransactionSystemLayout.t()}
  def try_to_fill_storage_team_vacancies(transaction_system_layout, _storage_info) do
    {:ok, transaction_system_layout}
  end

  @spec log_ids_in_use([LogDescriptor.t()]) :: [Log.id()]
  def log_ids_in_use(logs),
    do: logs |> Enum.group_by(& &1.log_id, & &1.tags) |> Map.keys()

  def find_candidate_log_info(log_info, assigned_ids),
    do: log_info |> Enum.reject(fn {id, _} -> id in assigned_ids end)

  def vacancies?(log_ids), do: Enum.member?(log_ids, :vacant)
end
