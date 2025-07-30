defmodule Bedrock.ControlPlane.Director.Recovery.LogDiscoveryPhase do
  @moduledoc """
  Identifies logs from the previous layout that contain data requiring preservation.

  Runs when recovering from an existing cluster. Examines the previous transaction
  system layout to determine which logs contain committed transactions that must
  be copied to the new layout.

  Establishes the version vector representing the cluster's committed state at
  recovery time. This version is used by storage teams to determine authoritative
  data and provides the baseline for new transaction processing.

  Identifies logs needing data migration based on content and layout changes.
  Logs with committed transactions must be preserved to maintain durability
  guarantees across recovery.

  Transitions to :determine_durable_version with a list of logs requiring data
  migration and the current cluster version vector.
  """

  alias Bedrock.DataPlane.Log
  alias Bedrock.ControlPlane.Config.LogDescriptor

  use Bedrock.ControlPlane.Director.Recovery.RecoveryPhase

  import Bedrock.ControlPlane.Director.Recovery.Telemetry

  @impl true
  def execute(%RecoveryAttempt{} = recovery_attempt, context) do
    determine_old_logs_to_copy(
      context.old_transaction_system_layout |> Map.get(:logs, %{}),
      recovery_attempt.log_recovery_info_by_id,
      context.cluster_config.parameters.desired_logs |> determine_quorum()
    )
    |> case do
      {:error, :unable_to_meet_log_quorum = reason} ->
        {recovery_attempt, {:stalled, reason}}

      {:ok, log_ids, version_vector} ->
        trace_recovery_suitable_logs_chosen(log_ids, version_vector)

        updated_recovery_attempt =
          recovery_attempt
          |> Map.put(:old_log_ids_to_copy, log_ids)
          |> Map.put(:version_vector, version_vector)

        {updated_recovery_attempt, Bedrock.ControlPlane.Director.Recovery.VacancyCreationPhase}
    end
  end

  @spec determine_old_logs_to_copy(
          old_logs :: %{Log.id() => LogDescriptor.t()},
          %{Log.id() => Log.recovery_info()},
          Bedrock.quorum()
        ) ::
          {:ok, [Log.id()], Bedrock.version_vector()} | {:error, :unable_to_meet_log_quorum}
  def determine_old_logs_to_copy([], _, _), do: {:error, :unable_to_meet_log_quorum}

  def determine_old_logs_to_copy(old_logs, recovery_info_by_id, quorum) do
    old_logs
    |> recovery_info_for_logs(recovery_info_by_id)
    |> version_vectors_by_id()
    |> combinations(quorum)
    |> build_log_groups_and_vectors_from_combinations()
    |> rank_log_groups()
    |> List.first()
    |> case do
      nil ->
        {:error, :unable_to_meet_log_quorum}

      {log_ids, version_vector} ->
        {:ok, log_ids, version_vector}
    end
  end

  @spec recovery_info_for_logs(
          %{Log.id() => LogDescriptor.t()},
          %{Log.id() => Log.recovery_info()}
        ) ::
          %{Log.id() => Log.recovery_info()}
  def recovery_info_for_logs(logs, recovery_info_by_id) do
    logs
    |> Map.keys()
    |> Enum.map(&{&1, Map.get(recovery_info_by_id, &1)})
    |> Enum.reject(&is_nil(elem(&1, 1)))
    |> Map.new()
  end

  @spec combinations([term()], non_neg_integer()) :: [[term()]]
  def combinations(_list, 0), do: [[]]
  def combinations([], _num), do: []

  def combinations([head | tail], num),
    do: Enum.map(combinations(tail, num - 1), &[head | &1]) ++ combinations(tail, num)

  @spec version_vectors_by_id(%{Log.id() => Log.recovery_info()}) ::
          [{Log.id(), Bedrock.version_vector()}]
  def version_vectors_by_id(log_info) do
    log_info
    |> Enum.map(fn
      {id, info} ->
        {id, {info[:oldest_version], info[:last_version]}}
    end)
  end

  @spec build_log_groups_and_vectors_from_combinations([[{Log.id(), Bedrock.version_vector()}]]) ::
          [{[Log.id()], Bedrock.version_vector()}]
  def build_log_groups_and_vectors_from_combinations(combinations) do
    combinations
    |> Enum.map(fn group ->
      oldest = group |> Enum.map(fn {_, {oldest, _}} -> oldest end) |> Enum.max()
      newest = group |> Enum.map(fn {_, {_, newest}} -> newest end) |> Enum.min()
      {group |> Enum.map(&elem(&1, 0)), {oldest, newest}}
    end)
    |> Enum.filter(&valid_range?(&1))
  end

  @spec valid_range?({[Log.id()], Bedrock.version_vector()}) :: boolean()
  def valid_range?({_, {0, _newest}}), do: true
  def valid_range?({_, {_oldest, 0}}), do: false
  def valid_range?({_, {oldest, newest}}), do: newest >= oldest

  @spec rank_log_groups([{[Log.id()], Bedrock.version_vector()}]) :: [
          {[Log.id()], Bedrock.version_vector()}
        ]
  def rank_log_groups(groups) do
    groups
    |> Enum.sort_by(
      fn {_, {oldest, newest}} -> newest - oldest end,
      :desc
    )
  end

  @spec determine_quorum(non_neg_integer()) :: pos_integer()
  defp determine_quorum(n) when is_integer(n), do: 1 + div(n, 2)
end
