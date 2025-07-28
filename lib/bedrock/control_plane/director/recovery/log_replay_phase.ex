defmodule Bedrock.ControlPlane.Director.Recovery.LogReplayPhase do
  @moduledoc """
  Replays transactions from old logs into the new log configuration.

  Copies committed transactions from logs identified in the log discovery phase
  to the newly recruited logs. This ensures data consistency when the log layout
  changes between recovery attempts.

  Only replays transactions that were committed in the previous configuration.
  Uncommitted transactions are discarded since they were never guaranteed to
  be durable.

  The replay process maintains transaction ordering and ensures all committed
  data remains accessible in the new layout. Logs are replayed concurrently
  to minimize recovery time.

  Can stall if source logs are unavailable or if the replay process fails.
  Transitions to :repair_data_distribution once all required data is copied.
  """

  alias Bedrock.DataPlane.Log

  @behaviour Bedrock.ControlPlane.Director.Recovery.RecoveryPhase

  import Bedrock.ControlPlane.Director.Recovery.Telemetry

  @impl true
  def execute(%{state: :replay_old_logs} = recovery_attempt, _context) do
    replay_old_logs_into_new_logs(
      recovery_attempt.old_log_ids_to_copy,
      Map.keys(recovery_attempt.logs),
      recovery_attempt.version_vector,
      fn log_id ->
        case Map.get(recovery_attempt.transaction_services, log_id) do
          %{status: {:up, pid}} -> pid
          _ -> :none
        end
      end
    )
    |> case do
      :ok ->
        trace_recovery_old_logs_replayed()

        recovery_attempt
        |> Map.put(:state, :repair_data_distribution)

      {:error, reason} ->
        recovery_attempt |> Map.put(:state, {:stalled, reason})
    end
  end

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

  @spec pair_with_old_log_ids([Log.id()], [Log.id()]) ::
          Enumerable.t({Log.id(), Log.id() | :none})
  def pair_with_old_log_ids(new_log_ids, []),
    do: new_log_ids |> Stream.zip([:none] |> Stream.cycle())

  def pair_with_old_log_ids(new_log_ids, old_log_ids),
    do: new_log_ids |> Stream.zip(old_log_ids |> Stream.cycle())
end
