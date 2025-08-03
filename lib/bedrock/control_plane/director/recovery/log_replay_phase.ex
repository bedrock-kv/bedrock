defmodule Bedrock.ControlPlane.Director.Recovery.LogReplayPhase do
  @moduledoc """
  Migrates the active transaction window from old logs to newly recruited log configuration.

  Solves the critical data migration challenge of transferring the current window of committed
  transactions to known-good storage. Since recovery must read old logs anyway to verify what's
  recoverable, it duplicates that data to new logs rather than trusting potentially corrupted
  or failing storage. This ensures storage servers can continue processing from reliable storage.

  **Migration Strategy**: Pairs each new log with old logs using round-robin assignment,
  cycling through old logs when scaling up or pairing with `:none` during initialization.
  All pairings operate efficiently in parallel to minimize recovery time.

  **Service Access**: All log services (old and new) are locked by this point in recovery,
  so the phase simply retrieves their PIDs from the `service_pids` map using `Map.fetch!`
  to enforce that all required services must be available.

  **Data Integrity**: Only copies committed transactions within the established version
  vector (the active window), discarding uncommitted transactions that lack durability
  guarantees. Maintains transaction ordering and ensures storage servers can continue
  processing seamlessly in the new configuration.

  Stalls with detailed failure information if log copying fails due to service issues.
  However, immediately halts with error if any log reports `:newer_epoch_exists` (this
  director has been superseded). Transitions to sequencer startup with complete migration.

  See the Log Replay section in `docs/knowlege_base/02-deep/recovery-narrative.md`
  for detailed explanation of the migration strategy and robustness approach.
  """

  alias Bedrock.DataPlane.Log

  use Bedrock.ControlPlane.Director.Recovery.RecoveryPhase

  import Bedrock.ControlPlane.Director.Recovery.Telemetry

  @impl true
  def execute(recovery_attempt, context) do
    replay_old_logs_into_new_logs(
      recovery_attempt.old_log_ids_to_copy,
      Map.keys(recovery_attempt.logs),
      recovery_attempt.version_vector,
      recovery_attempt,
      context
    )
    |> case do
      :ok ->
        trace_recovery_old_logs_replayed()
        {recovery_attempt, Bedrock.ControlPlane.Director.Recovery.SequencerStartupPhase}

      {:error, reason} ->
        {recovery_attempt, {:stalled, reason}}
    end
  end

  @spec replay_old_logs_into_new_logs(
          old_log_ids :: [Log.id()],
          new_log_ids :: [Log.id()],
          version_vector :: Bedrock.version_vector(),
          recovery_attempt :: map(),
          context :: map()
        ) ::
          :ok
          | {:error,
             {:failed_to_copy_some_logs,
              [{reason :: term(), new_log_id :: Log.id(), old_log_id :: Log.id()}]}}
  def replay_old_logs_into_new_logs(
        old_log_ids,
        new_log_ids,
        {first_version, last_version},
        recovery_attempt,
        context \\ %{}
      ) do
    copy_log_data_fn = Map.get(context, :copy_log_data_fn, &copy_log_data/5)
    service_pids = recovery_attempt.service_pids

    new_log_ids
    |> pair_with_old_log_ids(old_log_ids)
    |> Task.async_stream(
      fn {new_log_id, old_log_id} ->
        copy_log_data_fn.(new_log_id, old_log_id, first_version, last_version, service_pids)
        |> then(&{new_log_id, &1})
      end,
      ordered: false,
      zip_input_on_exit: true
    )
    |> Enum.reduce_while(%{}, fn
      {:ok, {_, {:error, :newer_epoch_exists} = error}}, _ ->
        {:halt, error}

      {:ok, {_log_id, {:ok, _pid}}}, failures ->
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

  @spec copy_log_data(
          new_log_id :: Log.id(),
          old_log_id :: Log.id() | :none,
          first_version :: Bedrock.version(),
          last_version :: Bedrock.version(),
          service_pids :: %{Log.id() => pid()}
        ) :: {:ok, pid() | :no_recovery_needed} | {:error, term()}
  def copy_log_data(_new_log_id, :none, _first_version, _last_version, _service_pids) do
    {:ok, :no_recovery_needed}
  end

  def copy_log_data(new_log_id, old_log_id, first_version, last_version, service_pids) do
    Log.recover_from(
      Map.fetch!(service_pids, new_log_id),
      Map.fetch!(service_pids, old_log_id),
      first_version,
      last_version
    )
  end

  @spec pair_with_old_log_ids([Log.id()], [Log.id()]) ::
          Enumerable.t({Log.id(), Log.id() | :none})
  def pair_with_old_log_ids(new_log_ids, []),
    do: new_log_ids |> Stream.zip([:none] |> Stream.cycle())

  def pair_with_old_log_ids(new_log_ids, old_log_ids),
    do: new_log_ids |> Stream.zip(old_log_ids |> Stream.cycle())
end
