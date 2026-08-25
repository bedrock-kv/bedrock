defmodule Bedrock.Service.Foreman.Health do
  @moduledoc false

  alias Bedrock.Service.Foreman
  alias Bedrock.Service.Foreman.WorkerInfo

  @doc """
  Summarises a set of workers into one foreman verdict.

  The verdict describes a SET, so it cannot depend on the order the
  members arrive in — `recompute_health/1` folds `Map.values/1`, whose
  order follows however worker ids happen to sort. Stated as precedence
  over the whole collection rather than as a pairwise fold, which is what
  makes the independence obvious:

    * any worker failed to start — the foreman is failed, and no number
      of healthy siblings may soften that
    * any health we do not recognise — `:unknown`, because "we cannot
      tell" must not be reported as progress
    * any worker still stopped — `:starting`
    * otherwise every worker is running, so `:ok` (vacuously so when
      there are no workers at all)
  """
  @spec compute_health_from_worker_info([WorkerInfo.t()]) :: Foreman.health()
  def compute_health_from_worker_info(worker_info) do
    healths = Enum.map(worker_info, & &1.health)

    cond do
      Enum.any?(healths, &match?({:failed_to_start, _}, &1)) ->
        {:failed_to_start, :at_least_one_failed_to_start}

      Enum.any?(healths, &(not recognised?(&1))) ->
        :unknown

      Enum.any?(healths, &(&1 == :stopped)) ->
        :starting

      true ->
        :ok
    end
  end

  # Deliberately `term()`, not `WorkerInfo.health()`: this exists to
  # catch a value that is NOT one of those three, and typing the argument
  # as the very type that excludes them would make the catch-all
  # statically unreachable. It is reachable in fact —
  # `Foreman.report_health/3` accepts `Worker.health()`, which includes
  # `{:error, :timeout | :unavailable}`, and that lands in this field.
  @spec recognised?(term()) :: boolean()
  defp recognised?({:ok, _pid}), do: true
  defp recognised?(:stopped), do: true
  defp recognised?({:failed_to_start, _reason}), do: true
  defp recognised?(_health), do: false
end
