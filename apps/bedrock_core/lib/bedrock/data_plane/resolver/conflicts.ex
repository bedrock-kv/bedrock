defmodule Bedrock.DataPlane.Resolver.Conflicts do
  @moduledoc """
  Optimized conflict tracking that separates point writes from range operations.

  Uses MapSets for O(1) point conflict detection and interval trees only for
  true range operations. Point writes are detected by the pattern where
  end_key == start_key <> <<0>>.

  This provides significant performance improvements for import scenarios
  with many unique point writes.
  """

  alias Bedrock.DataPlane.Resolver.Tree

  @type t :: %__MODULE__{
          versions: [{Bedrock.version(), MapSet.t(binary()), Tree.t() | nil}]
        }

  defstruct versions: []

  @doc """
  Creates a new empty versioned conflicts structure.
  """
  @spec new() :: %__MODULE__{
          versions: []
        }
  def new do
    %__MODULE__{versions: []}
  end

  @doc """
  Separates conflicts into point writes and true ranges based on key patterns.
  Point writes have end_key == start_key <> <<0>>.
  """
  @spec separate_conflicts([Bedrock.key_range()]) :: {MapSet.t(binary()), [Bedrock.key_range()]}
  def separate_conflicts(conflicts) do
    Enum.reduce(conflicts, {MapSet.new(), []}, fn {start_key, end_key}, {points, ranges} ->
      case end_key do
        <<^start_key::binary, 0>> ->
          {MapSet.put(points, start_key), ranges}

        _ ->
          {points, [{start_key, end_key} | ranges]}
      end
    end)
  end

  @doc """
  Checks if any of the given conflicts overlap with existing conflicts
  at versions greater than the specified version.
  """
  @spec check_conflicts(t(), [Bedrock.key_range()], Bedrock.version()) :: :ok | :abort
  def check_conflicts(%__MODULE__{versions: versions}, conflicts, version) do
    {points, ranges} = separate_conflicts(conflicts)
    do_check_conflicts(versions, points, ranges, version, [])
  end

  defp do_check_conflicts([{v, existing_points, tree} | rest], points, ranges, version, acc) when v > version do
    # Check points first (fast path)
    if MapSet.disjoint?(points, existing_points) do
      # No point conflict - accumulate tree and continue
      new_acc = if tree, do: [tree | acc], else: acc
      do_check_conflicts(rest, points, ranges, version, new_acc)
    else
      :abort
    end
  end

  defp do_check_conflicts(_, points, ranges, _version, acc), do: check_accumulated_conflicts(acc, points, ranges)

  defp check_accumulated_conflicts([], _points, _ranges), do: :ok

  defp check_accumulated_conflicts([tree | trees], points, ranges) do
    if Enum.any?(points, &Tree.overlap?(tree, &1)) or Enum.any?(ranges, &Tree.overlap?(tree, &1)) do
      :abort
    else
      check_accumulated_conflicts(trees, points, ranges)
    end
  end

  @doc """
  Adds conflicts for a new version to the structure.
  If the version matches the most recent version, merges the conflicts instead of creating a new entry.
  """
  @spec add_conflicts(t(), [Bedrock.key_range()], Bedrock.version()) :: t()
  def add_conflicts(
        %__MODULE__{versions: [{version, existing_points, existing_tree} | rest]} = conflicts,
        new_conflicts,
        version
      ) do
    {new_points, new_ranges} = separate_conflicts(new_conflicts)

    # Merge with existing version entry
    merged_points = MapSet.union(existing_points, new_points)

    # Merge ranges by adding new ranges to existing tree
    new_range_value_pairs = Enum.map(new_ranges, &{&1, version})
    merged_tree = Tree.insert_bulk(existing_tree, new_range_value_pairs)

    new_versions = [{version, merged_points, merged_tree} | rest]

    %{conflicts | versions: new_versions}
  end

  def add_conflicts(%__MODULE__{versions: versions} = conflicts, new_conflicts, version) do
    {points, ranges} = separate_conflicts(new_conflicts)

    # Add new version entry
    range_value_pairs = Enum.map(ranges, &{&1, version})
    ranges_tree = Tree.insert_bulk(nil, range_value_pairs)

    new_versions = [{version, points, ranges_tree} | versions]

    %{conflicts | versions: new_versions}
  end

  @doc """
  Removes conflicts older than the specified version.
  """
  @spec remove_old_conflicts(t(), Bedrock.version()) :: t()
  def remove_old_conflicts(%__MODULE__{versions: versions} = conflicts, min_version) do
    new_versions = keep_recent_versions(versions, min_version, [])
    %{conflicts | versions: new_versions}
  end

  # Keep this version and continue
  defp keep_recent_versions([{version, _points, _tree} = entry | rest], min_version, acc) when version >= min_version,
    do: keep_recent_versions(rest, min_version, [entry | acc])

  # This version is too old, and all subsequent versions will be even older since versions are in reverse chronological
  # order - stop here
  defp keep_recent_versions([{version, _points, _tree} | _rest], min_version, acc) when version < min_version,
    do: Enum.reverse(acc)

  # All done, reverse accumulated entries to restore original order
  defp keep_recent_versions([], _min_version, acc), do: Enum.reverse(acc)

  @doc """
  Returns metrics about the conflict structure for debugging.
  """
  @spec metrics(t()) :: %{
          version_count: non_neg_integer(),
          total_points: non_neg_integer(),
          total_ranges: non_neg_integer()
        }
  def metrics(%__MODULE__{versions: versions}) do
    {total_points, total_ranges} =
      Enum.reduce(versions, {0, 0}, fn {_version, points, ranges_tree}, {acc_points, acc_ranges} ->
        point_count = MapSet.size(points)
        range_count = count_tree_nodes(ranges_tree)
        {acc_points + point_count, acc_ranges + range_count}
      end)

    %{
      version_count: length(versions),
      total_points: total_points,
      total_ranges: total_ranges
    }
  end

  defp count_tree_nodes(nil), do: 0
  defp count_tree_nodes(%Tree{left: nil, right: nil}), do: 1
  defp count_tree_nodes(%Tree{left: left, right: right}), do: 1 + count_tree_nodes(left) + count_tree_nodes(right)
end
