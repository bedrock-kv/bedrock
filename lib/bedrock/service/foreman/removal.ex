defmodule Bedrock.Service.Foreman.Removal do
  @moduledoc false

  alias Bedrock.Service.Foreman.State
  alias Bedrock.Service.Foreman.WorkerInfo

  @spec stop(State.t(), WorkerInfo.t()) :: :ok | {:error, :worker_shutdown_unresolved}
  def stop(state, worker), do: attempt(state, worker, 3)

  defp attempt(state, worker, remaining) do
    supervisor = state.cluster.otp_name(:worker_supervisor)
    pid = registered(worker) || incarnation(worker)
    result = if is_pid(pid), do: DynamicSupervisor.terminate_child(supervisor, pid), else: {:error, :not_found}

    case result do
      result when result in [:ok, {:error, :not_found}] ->
        if ownership_removed?(supervisor, state.workers, worker) do
          :ok
        else
          retry(state, worker, remaining)
        end

      _ ->
        {:error, :worker_shutdown_unresolved}
    end
  catch
    :exit, _ -> {:error, :worker_shutdown_unresolved}
  end

  defp retry(state, worker, remaining) when remaining > 1, do: attempt(state, worker, remaining - 1)
  defp retry(_state, _worker, _remaining), do: {:error, :worker_shutdown_unresolved}

  # DynamicSupervisor serializes this snapshot with EXIT/restart/start callbacks.
  # A nil name is insufficient: unaccounted children or :restarting retain ownership.
  # Foreman serializes explicit starts/removals; all normal worker specs have fixed
  # registered names and are direct children. Timed-out external starts are outside
  # this guarantee and must never be guessed away as unrelated children.
  defp ownership_removed?(supervisor, workers, target) do
    other_pids = for {id, info} <- workers, id != target.id, pid = incarnation(info), is_pid(pid), do: pid
    children = DynamicSupervisor.which_children(supervisor)

    is_nil(registered(target)) and
      Enum.all?(children, fn {_id, pid, _type, _modules} -> is_pid(pid) and pid in other_pids end)
  end

  defp registered(%{otp_name: name}) when is_atom(name) and not is_nil(name), do: Process.whereis(name)
  defp registered(_), do: nil
  defp incarnation(%{incarnation_pid: pid}) when is_pid(pid), do: pid
  defp incarnation(%{health: {:ok, pid}}), do: pid
  defp incarnation(_), do: nil
end
