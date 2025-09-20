defmodule ResolverImplementationComparison do
  @moduledoc """
  Benchmarks for the optimized versioned conflicts resolver implementation.

  Tests performance on realistic scenarios:
  - Many unique point writes (import scenarios)
  - Mixed point/range operations
  - Pure conflict detection
  - Read-write conflicts

  Usage:
    mix run benchmarks/resolver_implementation_comparison.exs              # Point writes benchmark
    mix run benchmarks/resolver_implementation_comparison.exs mixed        # Mixed point/range benchmark
    mix run benchmarks/resolver_implementation_comparison.exs conflict_only    # Conflict detection only
    mix run benchmarks/resolver_implementation_comparison.exs worst_vs_best    # Worst case vs best case comparison
  """

  alias Bedrock.DataPlane.Resolver.ConflictResolution
  alias Bedrock.DataPlane.Resolver.Conflicts
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version
  alias Benchee.Formatters.Console

  @keys_per_transaction 200
  # Match original benchmark
  @setup_transaction_count 500

  def generate_unique_keys(base_prefix, count) do
    for i <- 1..count do
      suffix = "_#{i}_#{System.unique_integer()}"
      key = base_prefix <> suffix

      if byte_size(key) > 15 do
        binary_part(key, 0, 15)
      else
        key
      end
    end
  end

  def create_transaction_with_keys(keys, version) do
    mutations =
      Enum.map(keys, fn key ->
        {:set, key, "value_#{Base.encode64(key)}"}
      end)

    # Point writes create {key, key <> <<0>>} ranges
    write_conflicts = Enum.map(keys, fn key -> {key, key <> <<0>>} end)
    read_conflicts = Enum.map(keys, fn key -> {key, key <> <<0>>} end)

    Transaction.encode(%{
      mutations: mutations,
      read_conflicts: {Version.subtract(version, 1), read_conflicts},
      write_conflicts: write_conflicts,
      commit_version: version
    })
  end

  def create_range_transaction(start_key, end_key, version) do
    mutations = [{:clear_range, start_key, end_key}]
    write_conflicts = [{start_key, end_key}]

    Transaction.encode(%{
      mutations: mutations,
      write_conflicts: write_conflicts,
      read_version: Version.subtract(version, 1),
      commit_version: version
    })
  end

  def build_versioned_conflicts(transaction_count \\ @setup_transaction_count) do
    IO.puts("Building versioned conflicts with #{transaction_count} transactions...")

    {conflicts, _} =
      for i <- 1..transaction_count, reduce: {Conflicts.new(), 1} do
        {acc_conflicts, version_counter} ->
          version = Version.from_integer(version_counter)
          keys = generate_unique_keys("import_#{i}_", @keys_per_transaction)
          transaction = create_transaction_with_keys(keys, version)

          {new_conflicts, _aborted} = ConflictResolution.resolve(acc_conflicts, [transaction], version)
          {new_conflicts, version_counter + 1}
      end

    conflicts
  end

  def build_mixed_conflicts(transaction_count \\ @setup_transaction_count) do
    IO.puts("Building mixed conflicts with #{transaction_count} transactions (90% points, 10% ranges)...")

    {conflicts, _} =
      for i <- 1..transaction_count, reduce: {Conflicts.new(), 1} do
        {acc_conflicts, version_counter} ->
          version = Version.from_integer(version_counter)

          transaction =
            if rem(i, 10) == 0 do
              # Every 10th transaction is a range operation
              create_range_transaction("range_#{i}_start", "range_#{i}_end", version)
            else
              # Other transactions are point writes
              keys = generate_unique_keys("import_#{i}_", @keys_per_transaction)
              create_transaction_with_keys(keys, version)
            end

          {new_conflicts, _aborted} = ConflictResolution.resolve(acc_conflicts, [transaction], version)
          {new_conflicts, version_counter + 1}
      end

    conflicts
  end

  def create_benchmark_transaction do
    version = Version.from_integer(@setup_transaction_count + 1)
    keys = generate_unique_keys("benchmark_", @keys_per_transaction)
    create_transaction_with_keys(keys, version)
  end

  def create_range_benchmark_transaction do
    version = Version.from_integer(@setup_transaction_count + 1)
    create_range_transaction("range_start", "range_end", version)
  end

  def create_conflicting_transaction do
    version = Version.from_integer(@setup_transaction_count + 1)
    # Use keys that we know already exist from the setup
    # These should conflict with setup
    conflicting_keys = generate_unique_keys("import_1_", 10)
    create_transaction_with_keys(conflicting_keys, version)
  end

  def run_benchmark do
    conflicts = build_versioned_conflicts()

    point_transaction = create_benchmark_transaction()
    benchmark_version = Version.from_integer(@setup_transaction_count + 1)

    metrics = Conflicts.metrics(conflicts)

    IO.puts(
      "\nVersioned conflicts structure: #{metrics.version_count} versions, #{metrics.total_points} points, #{metrics.total_ranges} ranges"
    )

    IO.puts("\nBenchmarking point write resolution...")

    Benchee.run(
      %{
        "versioned_resolve" => fn ->
          ConflictResolution.resolve(conflicts, [point_transaction], benchmark_version)
        end
      },
      time: 5,
      memory_time: 2,
      formatters: [Console],
      print: [fast_warning: false]
    )
  end

  def run_mixed_benchmark do
    conflicts = build_mixed_conflicts()

    point_transaction = create_benchmark_transaction()
    range_transaction = create_range_benchmark_transaction()
    benchmark_version = Version.from_integer(@setup_transaction_count + 1)

    metrics = Conflicts.metrics(conflicts)

    IO.puts(
      "\nMixed conflicts structure: #{metrics.version_count} versions, #{metrics.total_points} points, #{metrics.total_ranges} ranges"
    )

    IO.puts("\nBenchmarking mixed point/range resolution...")

    Benchee.run(
      %{
        "point_write_resolve" => fn ->
          ConflictResolution.resolve(conflicts, [point_transaction], benchmark_version)
        end,
        "range_resolve" => fn ->
          ConflictResolution.resolve(conflicts, [range_transaction], benchmark_version)
        end
      },
      time: 5,
      memory_time: 2,
      formatters: [Console],
      print: [fast_warning: false]
    )
  end

  def run_conflict_detection_benchmark do
    # Test pure conflict detection without resolution
    conflicts = build_versioned_conflicts()

    point_transaction = create_benchmark_transaction()
    benchmark_version = Version.from_integer(@setup_transaction_count + 1)

    IO.puts("\nBenchmarking conflict detection only...")

    Benchee.run(
      %{
        "conflict_check" => fn ->
          ConflictResolution.try_to_resolve_transaction(conflicts, point_transaction, benchmark_version) == :abort
        end
      },
      time: 5,
      memory_time: 2,
      formatters: [Console],
      print: [fast_warning: false]
    )
  end

  def run_conflict_vs_no_conflict_benchmark do
    conflicts = build_versioned_conflicts()

    non_conflicting_transaction = create_benchmark_transaction()
    conflicting_transaction = create_conflicting_transaction()
    benchmark_version = Version.from_integer(@setup_transaction_count + 1)

    metrics = Conflicts.metrics(conflicts)

    IO.puts(
      "\nConflict structure: #{metrics.version_count} versions, #{metrics.total_points} points, #{metrics.total_ranges} ranges"
    )

    IO.puts("\nBenchmarking worst case (no conflict) vs best case (conflict) transaction resolution...")

    Benchee.run(
      %{
        "worst_case_no_conflict" => fn ->
          ConflictResolution.resolve(conflicts, [non_conflicting_transaction], benchmark_version)
        end,
        "best_case_conflict" => fn ->
          ConflictResolution.resolve(conflicts, [conflicting_transaction], benchmark_version)
        end
      },
      time: 5,
      memory_time: 2,
      formatters: [Console],
      print: [fast_warning: false]
    )

    IO.puts("\nBenchmarking worst case (no conflict) vs best case (conflict) detection...")

    Benchee.run(
      %{
        "worst_case_no_conflict_check" => fn ->
          ConflictResolution.try_to_resolve_transaction(conflicts, non_conflicting_transaction, benchmark_version) ==
            :abort
        end,
        "best_case_conflict_check" => fn ->
          ConflictResolution.try_to_resolve_transaction(conflicts, conflicting_transaction, benchmark_version) == :abort
        end
      },
      time: 5,
      memory_time: 2,
      formatters: [Console],
      print: [fast_warning: false]
    )
  end
end

# Run the benchmarks
case System.argv() do
  ["conflict_only"] ->
    ResolverImplementationComparison.run_conflict_detection_benchmark()

  ["mixed"] ->
    ResolverImplementationComparison.run_mixed_benchmark()

  ["worst_vs_best"] ->
    ResolverImplementationComparison.run_conflict_vs_no_conflict_benchmark()

  _ ->
    ResolverImplementationComparison.run_benchmark()
end
