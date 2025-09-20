defmodule Bedrock.DataPlane.Resolver.VersionedConflicts do
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
          versions: [{Bedrock.version(), MapSet.t(binary()), Tree.t() | nil}],
          tree: Tree.t() | nil
        }

  defstruct versions: [], tree: nil

  @doc """
  Creates a new empty versioned conflicts structure.
  """
  @spec new() :: %__MODULE__{
          versions: [],
          tree: nil
        }
  def new do
    %__MODULE__{versions: [], tree: nil}
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
  @spec conflict?(t(), [Bedrock.key_range()], Bedrock.version()) :: boolean()
  def conflict?(%__MODULE__{versions: versions, tree: tree}, conflicts, version) do
    {points, ranges} = separate_conflicts(conflicts)

    point_conflict?(versions, points, version) or range_conflict?(tree, ranges, version)
  end

  @doc """
  Adds conflicts for a new version to the structure.
  """
  @spec add_conflicts(t(), [Bedrock.key_range()], Bedrock.version()) :: t()
  def add_conflicts(%__MODULE__{versions: versions, tree: tree} = conflicts, new_conflicts, version) do
    {points, ranges} = separate_conflicts(new_conflicts)

    # Add ranges to the global tree
    range_value_pairs = Enum.map(ranges, &{&1, version})
    new_tree = Tree.insert_bulk(tree, range_value_pairs)

    # Add version entry with points and ranges tree
    range_value_pairs = Enum.map(ranges, &{&1, version})
    ranges_tree = Tree.insert_bulk(nil, range_value_pairs)

    new_versions = [{version, points, ranges_tree} | versions]

    %{conflicts | versions: new_versions, tree: new_tree}
  end

  @doc """
  Removes conflicts older than the specified version.
  """
  @spec remove_old_conflicts(t(), Bedrock.version()) :: t()
  def remove_old_conflicts(%__MODULE__{versions: versions, tree: tree} = conflicts, min_version) do
    # Filter versions to keep only those >= min_version
    new_versions = Enum.filter(versions, fn {version, _points, _tree} -> version >= min_version end)

    # Rebuild global tree with only remaining versions
    new_tree = Tree.filter_by_value(tree, &(&1 >= min_version))

    %{conflicts | versions: new_versions, tree: new_tree}
  end

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

  # Check if any points conflict with existing points at later versions
  # Early exit when we reach a version <= our version since versions are in reverse chronological order
  defp point_conflict?(versions, points, version) do
    point_conflict_recursive(versions, points, version)
  end

  defp point_conflict_recursive([], _points, _version), do: false

  defp point_conflict_recursive([{v, _existing_points, _tree} | _rest], _points, version) when v <= version do
    # All subsequent versions will be <= our version, so we can stop
    false
  end

  defp point_conflict_recursive([{_v, existing_points, _tree} | rest], points, version) do
    if MapSet.disjoint?(points, existing_points) == false do
      # Found a conflict at a later version
      true
    else
      # No conflict at this version, continue checking
      point_conflict_recursive(rest, points, version)
    end
  end

  # Check if any ranges conflict with existing ranges at later versions
  defp range_conflict?(tree, ranges, version) do
    predicate = fn v -> v > version end
    Enum.any?(ranges, &Tree.overlap?(tree, &1, predicate))
  end

  defp count_tree_nodes(nil), do: 0
  defp count_tree_nodes(%Tree{left: nil, right: nil}), do: 1

  defp count_tree_nodes(%Tree{left: left, right: right}) do
    1 + count_tree_nodes(left) + count_tree_nodes(right)
  end
end
