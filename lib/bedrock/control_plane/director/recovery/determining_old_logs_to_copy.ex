defmodule Bedrock.ControlPlane.Director.Recovery.DeterminingOldLogsToCopy do
  alias Bedrock.DataPlane.Log
  alias Bedrock.ControlPlane.Config.LogDescriptor

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
  defp combinations(_list, 0), do: [[]]
  defp combinations([], _num), do: []

  defp combinations([head | tail], num),
    do: Enum.map(combinations(tail, num - 1), &[head | &1]) ++ combinations(tail, num)

  @spec version_vectors_by_id(%{Log.id() => Log.recovery_info()}) ::
          [{Log.id(), Bedrock.version_vector()}]
  defp version_vectors_by_id(log_info) do
    log_info
    |> Enum.map(fn
      {id, info} ->
        {id, {info[:oldest_version], info[:last_version]}}
    end)
  end

  defp build_log_groups_and_vectors_from_combinations(combinations) do
    combinations
    |> Enum.map(fn group ->
      oldest = group |> Enum.map(fn {_, {oldest, _}} -> oldest end) |> Enum.max()
      newest = group |> Enum.map(fn {_, {_, newest}} -> newest end) |> Enum.min()
      {group |> Enum.map(&elem(&1, 0)), {oldest, newest}}
    end)
    |> Enum.filter(&valid_range?(&1))
  end

  defp valid_range?({_, {0, _newest}}), do: true
  defp valid_range?({_, {_oldest, 0}}), do: false
  defp valid_range?({_, {oldest, newest}}), do: newest >= oldest

  defp rank_log_groups(groups) do
    groups
    |> Enum.sort_by(
      fn {_, {oldest, newest}} -> newest - oldest end,
      :desc
    )
  end
end
