defmodule Bedrock.Service.Foreman.Health do
  alias Bedrock.Service.Foreman
  alias Bedrock.Service.Foreman.WorkerInfo

  @spec compute_health_from_worker_info([WorkerInfo.t()]) :: Foreman.health()
  def compute_health_from_worker_info(worker_info) do
    worker_info
    |> Enum.map(& &1.health)
    |> Enum.reduce(:ok, fn
      {:ok, _}, :ok -> :ok
      {:ok, _}, _ -> :starting
      {:failed_to_start, _}, :ok -> :starting
      {:failed_to_start, _}, _ -> {:failed_to_start, :at_least_one_failed_to_start}
      _, _ -> :unknown
    end)
  end
end
