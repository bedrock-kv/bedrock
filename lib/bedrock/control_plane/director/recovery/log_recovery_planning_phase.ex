defmodule Bedrock.ControlPlane.Director.Recovery.LogRecoveryPlanningPhase do
  alias Bedrock.DataPlane.Version

  @moduledoc """
  Determines which logs from the previous layout should be copied to preserve committed transactions.

  Runs when logs existed in the previous layout. Groups logs by shard (range tag) and evaluates 
  quorum combinations within each shard. For each shard combination, calculates version ranges 
  (max oldest, min newest) and ranks by range size. The final version vector takes the minimum 
  of the maximum edges across all shards—the highest transaction version guaranteed complete 
  across every shard.

  This shard-aware approach directly examines what transactions are actually persisted in logs 
  rather than relying on proxy-tracked commit confirmations, avoiding version lag issues where 
  successfully persisted transactions might be discarded.

  Stalls with `:unable_to_meet_log_quorum` if any shard cannot meet quorum requirements 
  or no valid cross-shard version range can be established.

  Transitions to vacancy creation with the selected logs and established version vector.

  See the Log Recovery Planning section in `docs/knowlege_base/02-deep/recovery-narrative.md` 
  for detailed explanation of the shard-aware planning algorithm and architectural rationale.
  """

  use Bedrock.ControlPlane.Director.Recovery.RecoveryPhase

  alias Bedrock.ControlPlane.Config.LogDescriptor
  alias Bedrock.DataPlane.Log

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
    recovery_info = recovery_info_for_logs(old_logs, recovery_info_by_id)

    # Use shard-aware algorithm universally for stronger safety guarantees
    # and better storage recovery lag tolerance
    determine_version_vector_shard_aware(recovery_info, old_logs, quorum)
  end

  @spec determine_version_vector_shard_aware(
          %{Log.id() => Log.recovery_info()},
          %{Log.id() => LogDescriptor.t()},
          Bedrock.quorum()
        ) ::
          {:ok, [Log.id()], Bedrock.version_vector()} | {:error, :unable_to_meet_log_quorum}
  def determine_version_vector_shard_aware(log_recovery_info, old_logs, _quorum) do
    logs_by_shard = group_logs_by_shard(log_recovery_info, old_logs)
    all_logs_by_shard = group_all_logs_by_shard(old_logs)

    logs_by_shard
    |> evaluate_shard_combinations(all_logs_by_shard)
    |> calculate_cross_shard_version_vector()
    |> case do
      nil -> {:error, :unable_to_meet_log_quorum}
      {log_ids, version_vector} -> {:ok, log_ids, version_vector}
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
  def valid_range?({_, {oldest, newest}}) do
    zero_version = Version.zero()

    cond do
      oldest == zero_version -> true
      newest == zero_version -> false
      true -> newest >= oldest
    end
  end

  @spec rank_log_groups([{[Log.id()], Bedrock.version_vector()}]) :: [
          {[Log.id()], Bedrock.version_vector()}
        ]
  def rank_log_groups(groups) do
    groups
    |> Enum.sort_by(
      fn {_, {oldest, newest}} -> Version.distance(newest, oldest) end,
      :desc
    )
  end

  @spec determine_quorum(non_neg_integer()) :: pos_integer()
  defp determine_quorum(n) when is_integer(n), do: 1 + div(n, 2)

  # Shard-aware algorithm implementation

  @spec group_logs_by_shard(
          %{Log.id() => Log.recovery_info()},
          %{Log.id() => LogDescriptor.t()}
        ) :: %{Bedrock.range_tag() => [{Log.id(), Log.recovery_info()}]}
  def group_logs_by_shard(log_recovery_info, old_logs) do
    log_recovery_info
    |> Enum.reduce(%{}, fn {log_id, recovery_info}, acc ->
      range_tags = Map.get(old_logs, log_id, [])

      # Add this log to each shard it participates in
      Enum.reduce(range_tags, acc, fn range_tag, shard_acc ->
        Map.update(shard_acc, range_tag, [{log_id, recovery_info}], fn existing ->
          [{log_id, recovery_info} | existing]
        end)
      end)
    end)
  end

  @spec group_all_logs_by_shard(%{Log.id() => LogDescriptor.t()}) :: %{
          Bedrock.range_tag() => [Log.id()]
        }
  def group_all_logs_by_shard(old_logs) do
    old_logs
    |> Enum.reduce(%{}, fn {log_id, range_tags}, acc ->
      Enum.reduce(range_tags, acc, fn range_tag, shard_acc ->
        Map.update(shard_acc, range_tag, [log_id], fn existing ->
          [log_id | existing]
        end)
      end)
    end)
  end

  @spec evaluate_shard_combinations(
          %{Bedrock.range_tag() => [{Log.id(), Log.recovery_info()}]},
          %{Bedrock.range_tag() => [Log.id()]}
        ) :: %{Bedrock.range_tag() => {[Log.id()], Bedrock.version_vector()}} | nil
  def evaluate_shard_combinations(logs_by_shard, all_logs_by_shard) do
    logs_by_shard
    |> Enum.map(fn {range_tag, shard_logs_with_recovery} ->
      evaluate_single_shard_combination(range_tag, shard_logs_with_recovery, all_logs_by_shard)
    end)
    |> Enum.reject(&is_nil/1)
    |> case do
      [] ->
        nil

      shard_results when length(shard_results) == map_size(all_logs_by_shard) ->
        Map.new(shard_results)

      # Not all shards met quorum
      _ ->
        nil
    end
  end

  @spec evaluate_single_shard_combination(
          Bedrock.range_tag(),
          [{Log.id(), Log.recovery_info()}],
          %{Bedrock.range_tag() => [Log.id()]}
        ) :: {Bedrock.range_tag(), {[Log.id()], Bedrock.version_vector()}} | nil
  defp evaluate_single_shard_combination(range_tag, shard_logs_with_recovery, all_logs_by_shard) do
    # Get total logs in this shard and calculate required quorum
    total_logs_in_shard = length(Map.get(all_logs_by_shard, range_tag, []))
    shard_quorum = determine_quorum(total_logs_in_shard)

    # Check if we have enough logs with recovery info to meet quorum
    if length(shard_logs_with_recovery) < shard_quorum do
      # Cannot meet quorum in this shard
      nil
    else
      # Apply existing algorithm within each shard using shard-specific quorum
      shard_logs_with_recovery
      |> Enum.map(fn {log_id, recovery_info} ->
        {log_id, {recovery_info[:oldest_version], recovery_info[:last_version]}}
      end)
      |> combinations(shard_quorum)
      |> build_log_groups_and_vectors_from_combinations()
      |> rank_log_groups()
      |> List.first()
      |> case do
        nil -> nil
        {log_ids, version_vector} -> {range_tag, {log_ids, version_vector}}
      end
    end
  end

  @spec calculate_cross_shard_version_vector(
          %{Bedrock.range_tag() => {[Log.id()], Bedrock.version_vector()}}
          | nil
        ) :: {[Log.id()], Bedrock.version_vector()} | nil
  def calculate_cross_shard_version_vector(nil), do: nil

  def calculate_cross_shard_version_vector(shard_results) do
    # Extract all participating logs and take minimum of maximum edges across shards
    all_logs =
      shard_results
      |> Map.values()
      |> Enum.flat_map(fn {log_ids, _} -> log_ids end)
      |> Enum.uniq()

    # Take minimum of the maximum edges (newest versions) across all shards
    min_newest =
      shard_results
      |> Map.values()
      |> Enum.map(fn {_, {_, newest}} -> newest end)
      |> Enum.min()

    # Take maximum of the minimum edges (oldest versions) across all shards
    max_oldest =
      shard_results
      |> Map.values()
      |> Enum.map(fn {_, {oldest, _}} -> oldest end)
      |> Enum.max()

    {all_logs, {max_oldest, min_newest}}
  end
end
