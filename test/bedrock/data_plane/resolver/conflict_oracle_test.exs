defmodule Bedrock.DataPlane.Resolver.ConflictOracleTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Bedrock.DataPlane.Resolver.ConflictResolution
  alias Bedrock.DataPlane.Resolver.Conflicts
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version

  test "exact verdicts include conflicts, endpoint nonconflicts and discarded aborted writes" do
    batches = [
      [
        tx(nil, [], [point("b")]),
        tx(0, [point("b")], [point("x")]),
        tx(0, [point("x")], [point("d")]),
        tx(0, [{"a", "c"}], []),
        tx(0, [{"a", "b"}], []),
        tx(nil, [], [{"m", "p"}]),
        tx(0, [point("n")], []),
        tx(0, [point("p")], [])
      ],
      [
        tx(10, [point("b"), point("n")], []),
        tx(0, [point("b")], []),
        tx(nil, [], [point("b")]),
        tx(10, [point("b")], [])
      ]
    ]

    assert check_history(batches) == [[1, 3, 6], [1, 3]]
  end

  test "same-start write ranges preserve all stale-read conflicts in either order" do
    for writes <- [[{"a", "c"}, {"a", "b"}], [{"a", "b"}, {"a", "c"}]] do
      assert check_history([[tx(nil, [], writes)], [tx(0, [point("b")], [])]]) == [[], [0]]
    end
  end

  test "a spanning write interval in the left subtree still aborts a stale read" do
    writes = [{"x", "y"}, {"a", "z"}, {"m", "n"}]
    assert check_history([[tx(nil, [], writes)], [tx(0, [point("p")], [])]]) == [[], [0]]
  end

  property "complete multi-batch verdicts match independent versioned interval history" do
    check all(
            raw_batches <-
              list_of(list_of(transaction_spec(), min_length: 1, max_length: 10), min_length: 2, max_length: 8),
            max_runs: 100
          ) do
      batches =
        raw_batches
        |> Enum.with_index()
        |> Enum.map(fn {specs, batch_index} ->
          Enum.map(specs, fn {reads, writes, snapshot} ->
            read_version = if reads == [], do: nil, else: 10 * rem(snapshot, batch_index + 1)
            tx(read_version, reads, writes)
          end)
        end)

      check_history(batches)
    end
  end

  defp transaction_spec do
    ranges = Enum.map(~w(a b c d), &point/1) ++ [{"a", "b"}, {"a", "c"}, {"b", "d"}, {"c", "d"}]
    tuple({list_of(member_of(ranges), max_length: 3), list_of(member_of(ranges), max_length: 3), integer(0..20)})
  end

  defp tx(read_version, reads, writes), do: %{read_version: read_version, reads: reads, writes: writes}
  defp point(key), do: {key, key <> <<0>>}

  defp check_history(batches) do
    {_, _, verdicts} =
      batches
      |> Enum.with_index(1)
      |> Enum.reduce({Conflicts.new(), [], []}, fn {batch, n}, {actual, history, verdicts} ->
        commit_version = n * 10
        encoded = Enum.map(batch, &encode_checked/1)
        {history, expected_aborts} = interpret_batch(history, batch, commit_version)
        {actual, aborts} = ConflictResolution.resolve(actual, encoded, Version.from_integer(commit_version))
        assert Enum.sort(aborts) == expected_aborts
        {actual, history, verdicts ++ [expected_aborts]}
      end)

    verdicts
  end

  defp encode_checked(%{reads: reads, writes: writes, read_version: version}) do
    read_version = if is_nil(version), do: nil, else: Version.from_integer(version)
    encoded = Transaction.encode(%{read_conflicts: {read_version, reads}, write_conflicts: writes})
    assert {:ok, {^read_version, ^reads}} = Transaction.read_conflicts(encoded)
    assert {:ok, {{^read_version, ^reads}, ^writes}} = Transaction.read_write_conflicts(encoded)
    encoded
  end

  # Plain successful-write history and mathematical half-open intersection.
  # No production conflict, tree, encoding or resolution helpers calculate verdicts.
  defp interpret_batch(history, transactions, commit_version) do
    transactions
    |> Enum.with_index()
    |> Enum.reduce({history, []}, fn {transaction, index}, {history, aborted} ->
      conflict? =
        Enum.any?(transaction.reads, fn {read_start, read_end} ->
          Enum.any?(history, fn {write_version, writes} ->
            write_version > transaction.read_version and
              Enum.any?(writes, fn {write_start, write_end} ->
                read_start < write_end and write_start < read_end
              end)
          end)
        end)

      if conflict?,
        do: {history, aborted ++ [index]},
        else: {history ++ [{commit_version, transaction.writes}], aborted}
    end)
  end
end
