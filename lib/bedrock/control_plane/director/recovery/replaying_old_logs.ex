defmodule Bedrock.ControlPlane.Director.Recovery.ReplayingOldLogs do
  alias Bedrock.DataPlane.Log

  @doc """
  Replays a selected set of transactions from the old log servers into the new
  ones. We use the provided `version_vector` and `pid_for_id` function to find
  the pid for each log. Logs are processed in parallel to decrease the recovery
  time.
  """
  @spec replay_old_logs_into_new_logs(
          old_log_ids :: [Log.id()],
          new_log_ids :: [Log.id()],
          version_vector :: Bedrock.version_vector(),
          pid_for_id :: (Log.id() -> pid() | :none)
        ) ::
          :ok
          | {:error,
             {:failed_to_copy_some_logs,
              [{reason :: term(), new_log_id :: Log.id(), old_log_id :: Log.id()}]}}
  def replay_old_logs_into_new_logs(
        old_log_ids,
        new_log_ids,
        {first_version, last_version},
        pid_for_id
      ) do
    new_log_ids
    |> pair_with_old_log_ids(old_log_ids)
    |> Task.async_stream(
      fn {new_log_id, old_log_id} ->
        {new_log_id,
         Log.recover_from(
           pid_for_id.(new_log_id),
           old_log_id && pid_for_id.(old_log_id),
           first_version,
           last_version
         )}
      end,
      ordered: false,
      zip_input_on_exit: true
    )
    |> Enum.reduce_while(%{}, fn
      {:ok, {_, {:error, :newer_epoch_exists} = error}}, _ ->
        {:halt, error}

      {:ok, {_log_id, :ok}}, failures ->
        {:cont, failures}

      {:ok, {log_id, {:error, reason}}}, failures ->
        {:cont, Map.put(failures, log_id, reason)}

      {:exit, {log_id, reason}}, failures ->
        {:cont, Map.put(failures, log_id, reason)}
    end)
    |> case do
      failures when failures == %{} -> :ok
      failures -> {:error, {:failed_to_copy_some_logs, failures}}
    end
  end

  def pair_with_old_log_ids(new_log_ids, []),
    do: new_log_ids |> Stream.zip([:none] |> Stream.cycle())

  def pair_with_old_log_ids(new_log_ids, old_log_ids),
    do: new_log_ids |> Stream.zip(old_log_ids |> Stream.cycle())
end
