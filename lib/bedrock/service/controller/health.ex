defmodule Bedrock.Service.Controller.Health do
  alias Bedrock.Service.Controller.State
  alias Bedrock.Service.Worker

  @spec update_health_for_worker(State.t(), Worker.id(), Worker.health()) :: State.t()
  def update_health_for_worker(t, worker_id, health),
    do: put_in(t, [:workers, worker_id, :health], health)

  @spec recompute_controller_health(State.t()) :: State.t()
  def recompute_controller_health(t), do: %{t | health: compute_health(t)}

  @spec compute_health(State.t()) :: Worker.health()
  defp compute_health(t) do
    t.workers
    |> Map.values()
    |> Enum.map(& &1.health)
    |> Enum.reduce(:ok, fn
      {:ok, _}, :ok -> :ok
      {:ok, _}, _ -> :starting
      {:failed_to_start, _}, :ok -> :starting
      {:failed_to_start, _}, _ -> {:failed_to_start, :at_least_one_failed_to_start}
    end)
  end
end
