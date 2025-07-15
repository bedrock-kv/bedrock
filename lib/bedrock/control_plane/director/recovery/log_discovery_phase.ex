defmodule Bedrock.ControlPlane.Director.Recovery.LogDiscoveryPhase do
  @moduledoc """
  Handles the :determine_old_logs_to_copy phase of recovery.

  This phase is responsible for determining which logs from the previous
  transaction system layout should be copied and what version vector
  should be used for recovery.

  See: [Recovery Guide](docs/knowledge_base/01-guides/recovery-guide.md#recovery-process)
  """

  alias Bedrock.DataPlane.Log
  alias Bedrock.ControlPlane.Config.LogDescriptor

  alias Bedrock.ControlPlane.Director.Recovery.RecoveryPhase
  alias Bedrock.ControlPlane.Config.RecoveryAttempt

  @behaviour RecoveryPhase

  import Bedrock.ControlPlane.Director.Recovery.Telemetry

  @doc """
  Execute the log discovery phase of recovery.

  Determines which old logs need to be copied based on the previous layout
  and available log recovery information.
  """
  @impl true
  def execute(%RecoveryAttempt{} = recovery_attempt, _context) do
    determine_old_logs_to_copy(
      recovery_attempt.last_transaction_system_layout.logs,
      recovery_attempt.log_recovery_info_by_id,
      recovery_attempt.parameters.desired_logs |> determine_quorum()
    )
    |> case do
      {:error, :unable_to_meet_log_quorum = reason} ->
        recovery_attempt |> Map.put(:state, {:stalled, reason})

      {:ok, log_ids, version_vector} ->
        trace_recovery_suitable_logs_chosen(log_ids, version_vector)

        recovery_attempt
        |> Map.put(:old_log_ids_to_copy, log_ids)
        |> Map.put(:version_vector, version_vector)
        |> Map.put(:state, :create_vacancies)
    end
  end

  @doc """
  Determines the old logs that need to be copied to recover a cluster to a
  consistent state.

  This function takes a list of logs described by `LogDescriptor`s, a map of
  recovery information indexed by log ID, and a quorum. We take a shortcut
  if the quorum is 1, as we can just copy the one existing log and use it's
  version vector.

  Otherwise, we generate all possible combinations of logs that can satisfy the
  quorum, and then rank them by the difference between the newest and oldest
  log's version vectors. We then return the log IDs of the combination with the
  smallest difference, as well as the version vector. We calculate the version
  vector by taking the oldest version from the oldest log and the newest version
  from the newest log in the set.
  """
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

  @spec combinations([any()], non_neg_integer()) :: [[any()]]
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
