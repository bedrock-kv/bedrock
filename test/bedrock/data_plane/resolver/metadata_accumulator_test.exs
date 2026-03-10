defmodule Bedrock.DataPlane.Resolver.MetadataAccumulatorTest do
  use ExUnit.Case, async: true

  import Bedrock.DataPlane.Resolver.MetadataAccumulator, only: [entries: 1]

  alias Bedrock.DataPlane.Resolver.MetadataAccumulator
  alias Bedrock.DataPlane.Version

  # Helper to create version binaries
  defp v(n), do: Version.from_integer(n)

  describe "new/0" do
    test "creates empty accumulator" do
      acc = MetadataAccumulator.new()
      assert entries(acc) == []
    end
  end

  describe "append/3" do
    test "appends mutations to empty accumulator" do
      acc = MetadataAccumulator.new()
      mutations = [{:set, <<0xFF, "key">>, "value"}]
      version = v(1)

      acc = MetadataAccumulator.append(acc, version, mutations)

      assert length(entries(acc)) == 1
      assert [{^version, ^mutations}] = entries(acc)
    end

    test "appends multiple versions in order" do
      acc =
        MetadataAccumulator.new()
        |> MetadataAccumulator.append(v(1), [{:set, <<0xFF, "a">>, "1"}])
        |> MetadataAccumulator.append(v(2), [{:set, <<0xFF, "b">>, "2"}])
        |> MetadataAccumulator.append(v(3), [{:set, <<0xFF, "c">>, "3"}])

      assert length(entries(acc)) == 3
      versions = Enum.map(entries(acc), fn {ver, _} -> ver end)
      assert versions == [v(1), v(2), v(3)]
    end

    test "ignores empty mutation lists" do
      acc = MetadataAccumulator.new()
      acc = MetadataAccumulator.append(acc, v(1), [])

      assert entries(acc) == []
    end
  end

  describe "mutations_since/2" do
    setup do
      acc =
        MetadataAccumulator.new()
        |> MetadataAccumulator.append(v(1), [{:set, <<0xFF, "a">>, "1"}])
        |> MetadataAccumulator.append(v(2), [{:set, <<0xFF, "b">>, "2"}])
        |> MetadataAccumulator.append(v(3), [{:set, <<0xFF, "c">>, "3"}])
        |> MetadataAccumulator.append(v(4), [{:clear, <<0xFF, "d">>}])

      {:ok, acc: acc}
    end

    test "returns all mutations when since_version is nil", %{acc: acc} do
      result = MetadataAccumulator.mutations_since(acc, nil)

      assert length(result) == 4
    end

    test "returns mutations after specified version", %{acc: acc} do
      result = MetadataAccumulator.mutations_since(acc, v(2))

      assert length(result) == 2
      versions = Enum.map(result, fn {ver, _} -> ver end)
      assert versions == [v(3), v(4)]
    end

    test "returns empty list when since_version is latest", %{acc: acc} do
      result = MetadataAccumulator.mutations_since(acc, v(4))

      assert result == []
    end

    test "returns empty list when since_version is beyond latest", %{acc: acc} do
      result = MetadataAccumulator.mutations_since(acc, v(100))

      assert result == []
    end

    test "returns all mutations when since_version is before first", %{acc: acc} do
      result = MetadataAccumulator.mutations_since(acc, v(0))

      assert length(result) == 4
    end
  end

  describe "prune_before/2" do
    setup do
      acc =
        MetadataAccumulator.new()
        |> MetadataAccumulator.append(v(1), [{:set, <<0xFF, "a">>, "1"}])
        |> MetadataAccumulator.append(v(2), [{:set, <<0xFF, "b">>, "2"}])
        |> MetadataAccumulator.append(v(3), [{:set, <<0xFF, "c">>, "3"}])
        |> MetadataAccumulator.append(v(4), [{:clear, <<0xFF, "d">>}])

      {:ok, acc: acc}
    end

    test "removes entries before specified version", %{acc: acc} do
      acc = MetadataAccumulator.prune_before(acc, v(3))

      assert length(entries(acc)) == 2
      versions = Enum.map(entries(acc), fn {ver, _} -> ver end)
      assert versions == [v(3), v(4)]
    end

    test "keeps all entries when pruning before first version", %{acc: acc} do
      acc = MetadataAccumulator.prune_before(acc, v(0))

      assert length(entries(acc)) == 4
    end

    test "removes all entries when pruning at or after last version", %{acc: acc} do
      acc = MetadataAccumulator.prune_before(acc, v(100))

      assert entries(acc) == []
    end

    test "prune_before is idempotent", %{acc: acc} do
      acc1 = MetadataAccumulator.prune_before(acc, v(2))
      acc2 = MetadataAccumulator.prune_before(acc1, v(2))

      assert acc1 == acc2
    end
  end

  describe "integration scenarios" do
    test "sliding window pattern" do
      # Simulate a sliding window where old entries are pruned
      acc =
        MetadataAccumulator.new()
        |> MetadataAccumulator.append(v(1), [{:set, <<0xFF, "a">>, "1"}])
        |> MetadataAccumulator.append(v(2), [{:set, <<0xFF, "b">>, "2"}])
        |> MetadataAccumulator.append(v(3), [{:set, <<0xFF, "c">>, "3"}])

      # Get mutations since v(1) for a client
      updates = MetadataAccumulator.mutations_since(acc, v(1))
      assert length(updates) == 2

      # Prune old entries now that client is at v(3)
      acc = MetadataAccumulator.prune_before(acc, v(2))

      # Add new mutations
      acc = MetadataAccumulator.append(acc, v(4), [{:set, <<0xFF, "d">>, "4"}])
      acc = MetadataAccumulator.append(acc, v(5), [{:set, <<0xFF, "e">>, "5"}])

      # Client at v(3) can still get updates
      updates = MetadataAccumulator.mutations_since(acc, v(3))
      assert length(updates) == 2
    end

    test "multiple mutation types per version" do
      mutations = [
        {:set, <<0xFF, "key1">>, "val1"},
        {:set, <<0xFF, "key2">>, "val2"},
        {:clear, <<0xFF, "key3">>},
        {:clear_range, <<0xFF, "start">>, <<0xFF, "end">>}
      ]

      acc = MetadataAccumulator.append(MetadataAccumulator.new(), v(1), mutations)

      [{_version, returned_mutations}] = MetadataAccumulator.mutations_since(acc, nil)
      assert returned_mutations == mutations
    end
  end
end
