defmodule Bedrock.DataPlane.CommitProxy.ConflictShardingTest do
  use ExUnitProperties
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.CommitProxy.ConflictSharding
  alias Bedrock.DataPlane.Transaction
  alias ConflictShardingGenerators, as: Gen

  describe "shard_conflicts_across_resolvers/3 property tests" do
    property "sharded conflicts cover exact key space with no duplication" do
      check all(
              {resolver_ends, resolver_refs} <- Gen.gen_resolver_boundaries(),
              {read_conflicts, write_conflicts} <- Gen.gen_conflict_ranges(),
              max_runs: 50
            ) do
        # Create binary sections from conflicts
        sections = Gen.gen_transaction_sections(read_conflicts, write_conflicts)

        # Shard across resolvers
        sharded = ConflictSharding.shard_conflicts_across_resolvers(sections, resolver_ends, resolver_refs)

        # Verify properties
        assert Gen.all_conflicts_distributed?(sharded, read_conflicts, write_conflicts),
               "Not all conflicts were distributed to resolvers"

        assert Gen.no_conflicts_duplicated?(sharded),
               "Some conflicts were duplicated across resolvers"

        assert Gen.each_resolver_has_only_its_range?(sharded, {resolver_ends, resolver_refs}),
               "Some resolvers received conflicts outside their range"

        assert Gen.binary_format_valid?(sharded),
               "Generated binary sections are not valid transaction format"

        # Verify read version is preserved across all resolvers that have read conflicts
        assert read_version_preserved?(sharded, read_conflicts),
               "Read version not preserved across resolver shards"

        # Verify all resolvers are present in result
        assert map_size(sharded) == length(resolver_refs),
               "Result doesn't contain all resolvers"
      end
    end

    property "single resolver passthrough works correctly" do
      check all(
              {read_conflicts, write_conflicts} <- Gen.gen_conflict_ranges(),
              max_runs: 20
            ) do
        # Single resolver setup
        resolver_ends = [{"\xFF\xFF\xFF", :single_resolver}]
        resolver_refs = [:single_resolver]

        sections = Gen.gen_transaction_sections(read_conflicts, write_conflicts)
        sharded = ConflictSharding.shard_conflicts_across_resolvers(sections, resolver_ends, resolver_refs)

        # Should have exactly one entry
        assert map_size(sharded) == 1
        assert Map.has_key?(sharded, :single_resolver)

        # The single resolver should get all conflicts
        single_sections = Map.get(sharded, :single_resolver)
        {:ok, {result_read, result_write}} = Transaction.read_write_conflicts(single_sections)

        # Verify conflicts match (allowing for format differences)
        assert conflicts_equivalent?(result_read, read_conflicts), "Read conflicts don't match"
        assert conflicts_equivalent?(result_write, write_conflicts), "Write conflicts don't match"
      end
    end

    property "empty conflicts handled correctly" do
      check all(
              {resolver_ends, resolver_refs} <- Gen.gen_resolver_boundaries(),
              max_runs: 10
            ) do
        # Empty conflicts
        read_conflicts = {nil, []}
        write_conflicts = []

        sections = Gen.gen_transaction_sections(read_conflicts, write_conflicts)
        sharded = ConflictSharding.shard_conflicts_across_resolvers(sections, resolver_ends, resolver_refs)

        # All resolvers should get empty transactions
        assert map_size(sharded) == length(resolver_refs)

        Enum.each(sharded, fn {_resolver_ref, binary_sections} ->
          {:ok, {result_read, result_write}} = Transaction.read_write_conflicts(binary_sections)
          assert result_read == {nil, []} or elem(result_read, 1) == []
          assert result_write == []
        end)
      end
    end

    property "boundary spanning conflicts are split correctly" do
      # Test specific case where conflicts span resolver boundaries
      check all(
              num_resolvers <- integer(2..5),
              max_runs: 20
            ) do
        # Create resolvers with predictable boundaries, ending with @end_marker
        intermediate_boundaries = for i <- 1..(num_resolvers - 1), do: "key_#{String.pad_leading("#{i}", 3, "0")}"
        boundaries = intermediate_boundaries ++ ["\xFF\xFF\xFF"]

        resolver_refs = for i <- 0..(num_resolvers - 1), do: :"resolver_#{i}"

        resolver_ends =
          boundaries
          |> Enum.with_index()
          |> Enum.map(fn {boundary, idx} -> {boundary, Enum.at(resolver_refs, idx)} end)

        # Create a conflict that spans from first to last resolver
        spanning_conflict = {"key_000", "key_999"}
        read_conflicts = {nil, []}
        write_conflicts = [spanning_conflict]

        sections = Gen.gen_transaction_sections(read_conflicts, write_conflicts)
        sharded = ConflictSharding.shard_conflicts_across_resolvers(sections, resolver_ends, resolver_refs)

        # Verify that multiple resolvers received parts of the conflict
        non_empty_resolvers =
          Enum.count(sharded, fn {_resolver_ref, binary_sections} ->
            {:ok, {_read, write}} = Transaction.read_write_conflicts(binary_sections)
            write != []
          end)

        # Should have at least 2 resolvers with conflicts (since it spans boundaries)
        assert non_empty_resolvers >= 2, "Spanning conflict not properly split across resolvers"
      end
    end

    property "key ordering is preserved within resolver shards" do
      check all(
              {resolver_ends, resolver_refs} <- Gen.gen_resolver_boundaries(),
              {read_conflicts, write_conflicts} <- Gen.gen_conflict_ranges(),
              max_runs: 20
            ) do
        sections = Gen.gen_transaction_sections(read_conflicts, write_conflicts)
        sharded = ConflictSharding.shard_conflicts_across_resolvers(sections, resolver_ends, resolver_refs)

        # Verify that within each resolver's conflicts, ordering is preserved
        Enum.each(sharded, fn {_resolver_ref, binary_sections} ->
          {:ok, {result_read, result_write}} = Transaction.read_write_conflicts(binary_sections)

          # Check read conflicts ordering
          read_ranges =
            case result_read do
              {_version, ranges} -> ranges
              _ -> []
            end

          assert sorted?(read_ranges), "Read conflicts not properly ordered"
          assert sorted?(result_write), "Write conflicts not properly ordered"
        end)
      end
    end
  end

  describe "edge cases" do
    test "handles :end boundaries correctly" do
      # Resolver that covers from some key to :end
      resolver_ends = [{"middle_key", :resolver_0}, {"\xFF\xFF\xFF", :resolver_1}]
      resolver_refs = [:resolver_0, :resolver_1]

      # Conflict that goes beyond middle_key should go to resolver_1
      read_conflicts = {nil, []}
      write_conflicts = [{"zzzz_key", "\xFF\xFF\xFF"}]

      sections = Gen.gen_transaction_sections(read_conflicts, write_conflicts)
      sharded = ConflictSharding.shard_conflicts_across_resolvers(sections, resolver_ends, resolver_refs)

      # The conflict should go to resolver_1 (covers middle_key to :end)
      {:ok, {_read, write_0}} = Transaction.read_write_conflicts(Map.get(sharded, :resolver_0))
      {:ok, {_read, write_1}} = Transaction.read_write_conflicts(Map.get(sharded, :resolver_1))

      # Only resolver_1 should have the conflict
      assert write_0 == []
      assert write_1 != []
    end

    test "handles single key conflicts" do
      resolver_ends = [{"key_b", :resolver_0}, {"\xFF\xFF\xFF", :resolver_1}]
      resolver_refs = [:resolver_0, :resolver_1]

      # Single key conflicts on boundary - put in write conflicts since read needs version
      single_key_conflict = {"key_a", "key_a" <> <<0>>}
      read_conflicts = {nil, []}
      write_conflicts = [single_key_conflict]

      sections = Gen.gen_transaction_sections(read_conflicts, write_conflicts)
      sharded = ConflictSharding.shard_conflicts_across_resolvers(sections, resolver_ends, resolver_refs)

      # Should go to resolver_0 (write conflicts)
      {:ok, {_read_0, write_0}} = Transaction.read_write_conflicts(Map.get(sharded, :resolver_0))
      {:ok, {_read_1, write_1}} = Transaction.read_write_conflicts(Map.get(sharded, :resolver_1))

      assert write_0 != []
      assert write_1 == []
    end
  end

  # Helper functions

  defp conflicts_equivalent?({version1, ranges1}, {version2, ranges2}) do
    version1 == version2 and ranges_equivalent?(ranges1, ranges2)
  end

  defp conflicts_equivalent?(ranges1, ranges2) when is_list(ranges1) and is_list(ranges2) do
    ranges_equivalent?(ranges1, ranges2)
  end

  defp conflicts_equivalent?({nil, []}, {nil, []}) do
    true
  end

  defp conflicts_equivalent?({nil, []}, []) do
    true
  end

  defp conflicts_equivalent?([], {nil, []}) do
    true
  end

  defp conflicts_equivalent?(_, _) do
    false
  end

  defp ranges_equivalent?(ranges1, ranges2) do
    # Convert to sets and compare (order might change due to sharding)
    set1 = MapSet.new(ranges1)
    set2 = MapSet.new(ranges2)
    MapSet.equal?(set1, set2)
  end

  defp sorted?([]) do
    true
  end

  defp sorted?([_single]) do
    true
  end

  defp sorted?([{start1, _} | [{start2, _} | _] = rest]) do
    start1 <= start2 and sorted?(rest)
  end

  defp read_version_preserved?(sharded_results, original_read_conflicts) do
    # Extract the original read version
    original_version =
      case original_read_conflicts do
        {version, _ranges} -> version
        _ -> nil
      end

    # Check that all resolvers with read conflicts have the same version
    Enum.all?(sharded_results, fn {_resolver_ref, binary_sections} ->
      {:ok, {result_read, _result_write}} = Transaction.read_write_conflicts(binary_sections)

      case result_read do
        {version, [_ | _]} ->
          # If this resolver has read conflicts, version must match original
          version == original_version

        {_version, []} ->
          # Empty read conflicts are fine regardless of version
          true

        {nil, []} ->
          # No read conflicts is fine
          true
      end
    end)
  end
end
