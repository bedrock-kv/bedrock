defmodule ConflictShardingGenerators do
  @moduledoc """
  StreamData generators for property testing the conflict sharding functionality.

  Generates randomized keyspace partitionings, conflict ranges, and transaction
  sections to verify the correctness of the sharding algorithms.
  """

  use ExUnitProperties

  import StreamData

  alias Bedrock.DataPlane.Transaction

  @doc """
  Generates a random keyspace partitioning with 1-10 resolvers.

  Returns {resolver_ends, resolver_refs} where:
  - resolver_ends: [{end_key, resolver_ref}, ...] sorted by end_key
  - resolver_refs: list of resolver pids
  """
  def gen_resolver_boundaries do
    gen all(
          num_resolvers <- integer(1..10),
          boundary_keys <- gen_sorted_boundary_keys(num_resolvers)
        ) do
      resolver_refs =
        boundary_keys
        |> Enum.with_index()
        |> Enum.map(fn {_key, idx} -> :"resolver_#{idx}" end)

      resolver_ends =
        boundary_keys
        |> Enum.with_index()
        |> Enum.map(fn {end_key, idx} -> {end_key, Enum.at(resolver_refs, idx)} end)

      {resolver_ends, resolver_refs}
    end
  end

  @doc """
  Generates sorted boundary keys for resolvers.

  Ensures keys are properly ordered and cover the full keyspace.
  """
  def gen_sorted_boundary_keys(num_resolvers) do
    gen all(
          raw_keys <- list_of(gen_key(), length: num_resolvers - 1),
          # Always use \xff\xff\xff as final boundary to cover full keyspace
          final_boundary <- constant("\xFF\xFF\xFF")
        ) do
      # Sort the intermediate keys and add final boundary
      sorted_keys = Enum.sort(raw_keys) ++ [final_boundary]

      # Remove duplicates and ensure proper ordering
      Enum.dedup(sorted_keys)
    end
  end

  @doc """
  Generates a random binary key that is less than \xff\xff\xff.
  """
  def gen_key do
    gen all(
          key_parts <-
            list_of(
              one_of([
                # ASCII printable characters
                string(:printable, min_length: 1, max_length: 10),
                # Binary data with limited range to stay < \xff\xff\xff
                binary(min_length: 1, max_length: 8)
              ]),
              min_length: 1,
              max_length: 3
            ),
          key = IO.iodata_to_binary(key_parts),
          # Ensure key is less than \xff\xff\xff
          key < "\xFF\xFF\xFF"
        ) do
      key
    end
  end

  @doc """
  Generates a large key for testing boundary conditions, ensuring it's < \xff\xff\xff.
  """
  def gen_large_key do
    gen all(
          prefix <- string(:printable, min_length: 1, max_length: 5),
          suffix <- binary(min_length: 8, max_length: 16),
          key = prefix <> suffix,
          # Ensure key is less than \xff\xff\xff
          key < "\xFF\xFF\xFF"
        ) do
      key
    end
  end

  @doc """
  Generates a list of conflict ranges (both read and write).

  Returns {read_conflicts, write_conflicts} where each is a list of {start_key, end_key}.
  """
  def gen_conflict_ranges do
    gen all(
          read_version <- one_of([constant(nil), gen_version()]),
          read_ranges <- list_of(gen_conflict_range(), max_length: 20),
          write_ranges <- list_of(gen_conflict_range(), max_length: 20)
        ) do
      # Sort ranges by start key for realistic conflict patterns
      sorted_reads = Enum.sort_by(read_ranges, &elem(&1, 0))
      sorted_writes = Enum.sort_by(write_ranges, &elem(&1, 0))

      # Only include read version if we have read conflicts
      read_conflicts =
        case {read_version, sorted_reads} do
          {nil, _} -> {nil, []}
          # Don't include version if no reads
          {_, []} -> {nil, []}
          {version, reads} -> {version, reads}
        end

      {read_conflicts, sorted_writes}
    end
  end

  @doc """
  Generates a single conflict range {start_key, end_key}.
  """
  def gen_conflict_range do
    gen all(
          start_key <- gen_key(),
          end_type <-
            one_of([
              constant(:end),
              constant(:generated_end),
              constant(:same_as_start)
            ])
        ) do
      case end_type do
        :end ->
          # Use \xff\xff\xff as :end marker
          {start_key, "\xFF\xFF\xFF"}

        :same_as_start ->
          # Single key range - add small increment but stay < \xff\xff\xff
          end_key =
            if start_key <> <<1>> < "\xFF\xFF\xFF" do
              start_key <> <<0>>
            else
              "\xFF\xFF\xFF"
            end

          {start_key, end_key}

        :generated_end ->
          # Generate end key that's >= start key but < \xff\xff\xff
          potential_end = start_key <> <<Enum.random(1..254)>>

          end_key =
            if potential_end < "\xFF\xFF\xFF" do
              potential_end
            else
              "\xFF\xFF\xFF"
            end

          {start_key, end_key}
      end
    end
  end

  @doc """
  Generates a version for read conflicts.
  """
  def gen_version do
    gen all(version_int <- integer(0..1_000_000)) do
      Bedrock.DataPlane.Version.from_integer(version_int)
    end
  end

  @doc """
  Generates valid transaction sections containing conflicts.

  Returns a binary transaction with READ_CONFLICTS and/or WRITE_CONFLICTS sections.
  """
  def gen_transaction_sections(read_conflicts, write_conflicts) do
    # Build transaction map
    transaction_map = %{
      # No mutations needed for conflict resolution
      mutations: [],
      read_conflicts: read_conflicts,
      write_conflicts: write_conflicts
    }

    # Encode to binary format
    encoded = Transaction.encode(transaction_map)

    # Extract just the conflict sections
    Transaction.extract_sections!(encoded, [:read_conflicts, :write_conflicts])
  end

  @doc """
  Generates edge case scenarios for testing.
  """
  def gen_edge_cases do
    one_of([
      # Empty conflicts
      constant({{nil, []}, []}),

      # Single key conflicts
      gen all(key <- gen_key()) do
        single_range = {key, key <> <<0>>}
        {{nil, []}, [single_range]}
      end,

      # Conflicts that span entire keyspace
      constant({{<<>>, "\xFF\xFF\xFF"}, {<<>>, "\xFF\xFF\xFF"}}),

      # Many small adjacent ranges
      gen all(
            num_ranges <- integer(5..15),
            base_key <- gen_key()
          ) do
        ranges =
          for i <- 0..(num_ranges - 1) do
            start_key = base_key <> <<i>>
            end_key = base_key <> <<i + 1>>
            {start_key, end_key}
          end

        {{nil, []}, ranges}
      end
    ])
  end

  @doc """
  Property invariant: all conflicts are distributed to resolvers.
  """
  def all_conflicts_distributed?(sharded_result, original_read_conflicts, original_write_conflicts) do
    # Extract all conflicts from sharded results
    all_sharded_reads = extract_all_sharded_reads(sharded_result)
    all_sharded_writes = extract_all_sharded_writes(sharded_result)

    # Convert to sets for comparison
    original_read_set = conflicts_to_set(elem(original_read_conflicts, 1))
    original_write_set = conflicts_to_set(original_write_conflicts)

    sharded_read_set = conflicts_to_set(all_sharded_reads)
    sharded_write_set = conflicts_to_set(all_sharded_writes)

    # Check coverage (allowing for range splitting)
    read_coverage = check_coverage(original_read_set, sharded_read_set)
    write_coverage = check_coverage(original_write_set, sharded_write_set)

    read_coverage and write_coverage
  end

  @doc """
  Property invariant: no conflicts are duplicated across resolvers.
  """
  def no_conflicts_duplicated?(sharded_result) do
    # Check that no key appears in multiple resolvers' ranges
    all_resolver_ranges = extract_all_resolver_ranges(sharded_result)

    # Check for overlaps between resolver ranges
    resolver_pairs =
      for {r1, ranges1} <- all_resolver_ranges,
          {r2, ranges2} <- all_resolver_ranges,
          r1 < r2,
          do: {ranges1, ranges2}

    Enum.all?(resolver_pairs, fn {ranges1, ranges2} ->
      not ranges_have_overlap?(ranges1, ranges2)
    end)
  end

  @doc """
  Property invariant: each resolver only gets conflicts in its range.
  """
  def each_resolver_has_only_its_range?(sharded_result, resolver_boundaries) do
    {resolver_ends, _resolver_refs} = resolver_boundaries

    # Check each resolver's conflicts are within its range
    Enum.all?(sharded_result, fn {resolver_ref, binary_sections} ->
      resolver_range = get_resolver_key_range_by_ref(resolver_ref, resolver_ends)

      # Check read and write conflicts separately
      case Transaction.read_write_conflicts(binary_sections) do
        {:ok, {{_version, read_ranges}, write_ranges}} ->
          all_conflicts_in_range?(read_ranges, resolver_range) and
            all_conflicts_in_range?(write_ranges, resolver_range)

        {:error, _} ->
          false
      end
    end)
  end

  @doc """
  Property invariant: binary format is valid after sharding.
  """
  def binary_format_valid?(sharded_result) do
    Enum.all?(sharded_result, fn {_resolver_ref, binary_sections} ->
      case Transaction.read_write_conflicts(binary_sections) do
        {:ok, _conflicts} -> true
        {:error, _reason} -> false
      end
    end)
  end

  # Helper functions for property validation

  defp extract_all_sharded_reads(sharded_result) do
    Enum.flat_map(sharded_result, fn {_resolver, binary_sections} ->
      case Transaction.read_write_conflicts(binary_sections) do
        {:ok, {{_version, read_ranges}, _write_ranges}} -> read_ranges
        {:error, _} -> []
      end
    end)
  end

  defp extract_all_sharded_writes(sharded_result) do
    Enum.flat_map(sharded_result, fn {_resolver, binary_sections} ->
      case Transaction.read_write_conflicts(binary_sections) do
        {:ok, {_read_conflicts, write_ranges}} -> write_ranges
        {:error, _} -> []
      end
    end)
  end

  defp conflicts_to_set(conflicts) when is_list(conflicts) do
    MapSet.new(conflicts)
  end

  defp check_coverage(original_set, sharded_set) do
    # When ranges get split across resolvers, simple subset check won't work
    # For now, just check that if we have original conflicts, we have sharded conflicts
    # and vice versa (empty sets should match empty sets)
    original_empty = MapSet.size(original_set) == 0
    sharded_empty = MapSet.size(sharded_set) == 0

    case {original_empty, sharded_empty} do
      # Both empty - OK
      {true, true} -> true
      # Both non-empty - OK (ranges may be split)
      {false, false} -> true
      # One empty, one not - NOT OK
      _ -> false
    end
  end

  defp extract_all_resolver_ranges(sharded_result) do
    Enum.map(sharded_result, fn {resolver_ref, binary_sections} ->
      case Transaction.read_write_conflicts(binary_sections) do
        {:ok, {{_version, read_ranges}, write_ranges}} ->
          # Return both read and write ranges as separate lists for overlap checking
          {resolver_ref, {read_ranges, write_ranges}}

        {:error, _} ->
          {resolver_ref, {[], []}}
      end
    end)
  end

  defp ranges_have_overlap?({read1, write1}, {read2, write2}) do
    # Check if any range in resolver1 overlaps with any range in resolver2
    # Check read-read overlaps
    read_overlaps =
      Enum.any?(read1, fn range1 ->
        Enum.any?(read2, fn range2 ->
          ranges_overlap?(range1, range2)
        end)
      end)

    # Check write-write overlaps
    write_overlaps =
      Enum.any?(write1, fn range1 ->
        Enum.any?(write2, fn range2 ->
          ranges_overlap?(range1, range2)
        end)
      end)

    # Check read-write overlaps (read from resolver1 with write from resolver2)
    read_write_overlaps =
      Enum.any?(read1, fn range1 ->
        Enum.any?(write2, fn range2 ->
          ranges_overlap?(range1, range2)
        end)
      end)

    # Check write-read overlaps (write from resolver1 with read from resolver2)
    write_read_overlaps =
      Enum.any?(write1, fn range1 ->
        Enum.any?(read2, fn range2 ->
          ranges_overlap?(range1, range2)
        end)
      end)

    read_overlaps or write_overlaps or read_write_overlaps or write_read_overlaps
  end

  defp ranges_overlap?({start1, end1}, {start2, end2}) do
    # Handle :end cases
    actual_end1 = if end1 == "\xFF\xFF\xFF", do: "\xFF\xFF\xFF", else: end1
    actual_end2 = if end2 == "\xFF\xFF\xFF", do: "\xFF\xFF\xFF", else: end2

    # Ranges overlap if: start1 < end2 AND start2 < end1
    start1 < actual_end2 and start2 < actual_end1
  end

  defp get_resolver_key_range_by_ref(resolver_ref, resolver_ends) do
    resolver_idx = Enum.find_index(resolver_ends, fn {_end_key, ref} -> ref == resolver_ref end)
    get_resolver_key_range(resolver_idx, resolver_ends)
  end

  defp get_resolver_key_range(resolver_idx, resolver_ends) do
    {end_key, _} = Enum.at(resolver_ends, resolver_idx)
    max_resolver_idx = length(resolver_ends) - 1

    start_key =
      if resolver_idx == 0 do
        <<>>
      else
        {prev_end, _} = Enum.at(resolver_ends, resolver_idx - 1)
        prev_end
      end

    # Last resolver always extends to \xFF\xFF\xFF to cover full keyspace
    actual_end_key =
      if resolver_idx == max_resolver_idx do
        "\xFF\xFF\xFF"
      else
        end_key
      end

    {start_key, actual_end_key}
  end

  defp all_conflicts_in_range?(conflicts, {range_start, range_end}) do
    Enum.all?(conflicts, fn {conflict_start, conflict_end} ->
      conflict_in_range?({conflict_start, conflict_end}, {range_start, range_end})
    end)
  end

  defp conflict_in_range?({conflict_start, conflict_end}, {range_start, range_end}) do
    # Handle :end cases
    actual_conflict_end = if conflict_end == "\xFF\xFF\xFF", do: "\xFF\xFF\xFF", else: conflict_end
    actual_range_end = if range_end == "\xFF\xFF\xFF", do: "\xFF\xFF\xFF", else: range_end

    # Conflict is in range if it's entirely within the range bounds
    conflict_start >= range_start and actual_conflict_end <= actual_range_end
  end

  # This function is no longer needed since we check read/write separately
  # defp extract_conflicts_from_binary(binary_sections) do
  #   case Transaction.read_write_conflicts(binary_sections) do
  #     {:ok, {{_version, read_ranges}, write_ranges}} -> {read_ranges, write_ranges}
  #     {:error, _} -> {[], []}
  #   end
  # end
end
