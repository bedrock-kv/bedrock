defmodule Bedrock.ControlPlane.ClusterController.Recovery.ReplayingOldLogs do
  alias Bedrock.DataPlane.Log

  @spec replay_old_logs_into_new_logs(
          old_log_ids :: [Log.id()] | :nothing,
          new_log_ids :: [Log.id()],
          version_vector :: Bedrock.version_vector(),
          pid_for_id :: (Log.id() -> pid())
        ) ::
          :ok
          | {:error,
             {:failed_to_copy_some_logs,
              [{reason :: term(), new_log_id :: Log.id(), old_log_id :: Log.id()}]}}
  def replay_old_logs_into_new_logs(old_log_ids, new_log_ids, version_vector, pid_for_id) do
    new_log_ids
    |> pair_with_old_log_ids(old_log_ids)
    |> Task.async_stream(
      fn {new_log_id, old_log_id} ->
        {new_log_id,
         Log.recover_from(
           pid_for_id.(new_log_id),
           old_log_id && pid_for_id.(old_log_id),
           version_vector
         )}
      end,
      ordered: false,
      zip_input_on_exit: true
    )
    |> Enum.reduce_while([], fn
      {:ok, {_, {:error, :newer_epoch_exists} = error}}, _ ->
        {:halt, error}

      {:ok, {_log_id, :ok}}, failures ->
        {:cont, failures}

      {:ok, {log_id, {:error, reason}}}, failures ->
        {:cont, [{reason, log_id} | failures]}

      {:exit, {log_id, reason}}, failures ->
        {:cont, [{reason, log_id} | failures]}
    end)
    |> case do
      [] -> :ok
      failures -> {:error, {:failed_to_copy_some_logs, failures}}
    end
  end

  def pair_with_old_log_ids(new_log_ids, old_log_ids) do
    new_log_ids
    |> Stream.zip(
      if :nothing == old_log_ids do
        [nil]
      else
        old_log_ids
      end
      |> Stream.cycle()
    )
  end
end
