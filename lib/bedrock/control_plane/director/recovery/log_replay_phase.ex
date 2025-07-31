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
  Transitions to data distribution once all required data is copied.
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
        {recovery_attempt, Bedrock.ControlPlane.Director.Recovery.DataDistributionPhase}

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
    log_recover_fn =
      Map.get(context, :log_recover_fn, fn new_log_id,
                                           old_log_id,
                                           first_version,
                                           last_version,
                                           recovery_attempt ->
        recover_log_with_pid_resolution(
          new_log_id,
          old_log_id,
          first_version,
          last_version,
          recovery_attempt,
          context
        )
      end)

    new_log_ids
    |> pair_with_old_log_ids(old_log_ids)
    |> Task.async_stream(
      fn {new_log_id, old_log_id} ->
        {new_log_id,
         log_recover_fn.(
           new_log_id,
           old_log_id,
           first_version,
           last_version,
           recovery_attempt
         )}
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

  @spec recover_log_with_pid_resolution(
          new_log_id :: Log.id(),
          old_log_id :: Log.id() | nil,
          first_version :: Bedrock.version(),
          last_version :: Bedrock.version(),
          recovery_attempt :: map(),
          context :: map()
        ) :: {:ok, pid()} | {:error, term()}
  defp recover_log_with_pid_resolution(
         new_log_id,
         old_log_id,
         first_version,
         last_version,
         recovery_attempt,
         context
       ) do
    # Get the target log name/node from available services
    case get_log_name_node(new_log_id, recovery_attempt, context) do
      {:ok, new_log_name_node} ->
        old_log_name_node =
          old_log_id &&
            case get_log_name_node(old_log_id, recovery_attempt, context) do
              {:ok, name_node} -> name_node
              _ -> nil
            end

        # Call log recovery directly with {name, node} - GenServer.call will resolve PIDs
        Log.recover_from(new_log_name_node, old_log_name_node, first_version, last_version)

      {:error, _} = error ->
        error
    end
  end

  @spec get_log_name_node(Log.id(), map(), map()) ::
          {:ok, {atom(), node()}} | {:error, :unavailable}
  defp get_log_name_node(log_id, recovery_attempt, context) do
    # First check transaction_services (has PIDs for locked services)
    case Map.get(recovery_attempt.transaction_services, log_id) do
      %{last_seen: {name, node}} ->
        {:ok, {name, node}}

      %{status: {:up, _pid}, last_seen: {name, node}} ->
        {:ok, {name, node}}

      _ ->
        # Check available_services from context for newly created services
        case get_in(context, [:available_services, log_id]) do
          {_kind, {name, node}} ->
            {:ok, {name, node}}

          _ ->
            {:error, :unavailable}
        end
    end
  end

  @spec pair_with_old_log_ids([Log.id()], [Log.id()]) ::
          Enumerable.t({Log.id(), Log.id() | :none})
  def pair_with_old_log_ids(new_log_ids, []),
    do: new_log_ids |> Stream.zip([:none] |> Stream.cycle())

  def pair_with_old_log_ids(new_log_ids, old_log_ids),
    do: new_log_ids |> Stream.zip(old_log_ids |> Stream.cycle())
end
