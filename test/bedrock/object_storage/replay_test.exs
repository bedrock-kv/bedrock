defmodule Bedrock.ObjectStorage.ReplayTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Transaction
  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.Chunk
  alias Bedrock.ObjectStorage.Keys
  alias Bedrock.ObjectStorage.LocalFilesystem
  alias Bedrock.ObjectStorage.Replay
  alias Bedrock.ObjectStorage.Replay.LiveCapture
  alias Bedrock.SystemKeys
  alias Bedrock.SystemKeys.Values

  @end_of_keyspace <<0xFF, 0xFF>>

  # The layout a fresh cluster bootstraps with: shard 1 covers the user
  # keyspace, shard 0 everything from \xFF up.
  @default_layout %{<<0xFF>> => {1, <<>>}, @end_of_keyspace => {0, <<0xFF>>}}

  # ---------------------------------------------------------------- fixtures

  setup do
    root = Path.join(System.tmp_dir!(), "replay_test_#{:erlang.unique_integer([:positive])}")
    File.mkdir_p!(root)
    on_exit(fn -> File.rm_rf!(root) end)

    {:ok, backend: ObjectStorage.backend(LocalFilesystem, root: root), root: root}
  end

  defp slice(version, mutations) do
    Transaction.encode(%{mutations: mutations, commit_version: <<version::unsigned-big-64>>})
  end

  # Writes one chunk holding `versioned_mutations`, named for its last version.
  defp put_slices(backend, shard_tag, versioned_mutations) do
    entries = Enum.map(versioned_mutations, fn {version, muts} -> {version, slice(version, muts)} end)
    {:ok, binary} = Chunk.encode(entries)
    {version, _} = List.last(entries)
    key = Keys.chunk_path(shard_tag, version)
    :ok = ObjectStorage.put(backend, key, binary)
    key
  end

  defp shard_key_sets(layout) do
    Enum.map(layout, fn {end_key, {tag, start_key}} ->
      {:set, SystemKeys.shard_key(end_key), Values.encode_shard_key_entry(tag, start_key)}
    end)
  end

  defp put_layout(backend, layout \\ @default_layout, version \\ 10) do
    put_slices(backend, "0", [{version, shard_key_sets(layout)}])
  end

  defp value(image, key) do
    case Replay.fetch(image, key) do
      {:ok, value} -> value
      :error -> nil
    end
  end

  # --------------------------------------------------------------- reconstruct

  describe "reconstruct/2 folding" do
    test "rebuilds the keyspace from a shard's chunks", %{backend: backend} do
      put_layout(backend)

      put_slices(backend, "1", [
        {20, [{:set, "a", "1"}, {:set, "b", "1"}]},
        {30, [{:set, "a", "2"}]}
      ])

      {:ok, image} = Replay.reconstruct(backend)

      assert value(image, "a") == "2"
      assert value(image, "b") == "1"
    end

    test "folds a shard's chunks oldest-first, not listing-first", %{backend: backend} do
      put_layout(backend)
      put_slices(backend, "1", [{40, [{:set, "a", "newest"}]}])
      put_slices(backend, "1", [{20, [{:set, "a", "oldest"}]}])
      put_slices(backend, "1", [{30, [{:set, "a", "middle"}]}])

      {:ok, image} = Replay.reconstruct(backend)

      assert value(image, "a") == "newest"
      assert [_system, %{shard_tag: "1", chunk_count: 3, transaction_count: 3}] = image.shards
    end

    test "refuses to reconstruct from a chunk it cannot read", %{backend: backend} do
      put_layout(backend)
      :ok = ObjectStorage.put(backend, Keys.chunk_path("1", 20), "not a chunk")

      assert {:error, {:unreadable_chunk, _key, _reason}} = Replay.reconstruct(backend)
    end

    test "applies clears as removals, not as empty values", %{backend: backend} do
      put_layout(backend)
      put_slices(backend, "1", [{20, [{:set, "a", "1"}, {:set, "b", "1"}]}, {30, [{:clear, "a"}]}])

      {:ok, image} = Replay.reconstruct(backend)

      assert Replay.fetch(image, "a") == :error
      assert value(image, "b") == "1"
    end

    test "applies clear_range over the keys the shard holds", %{backend: backend} do
      put_layout(backend)

      put_slices(backend, "1", [
        {20, [{:set, "a", "1"}, {:set, "b", "1"}, {:set, "c", "1"}]},
        {30, [{:clear_range, "a", "c"}]}
      ])

      {:ok, image} = Replay.reconstruct(backend)

      assert Replay.fetch(image, "a") == :error
      assert Replay.fetch(image, "b") == :error
      assert value(image, "c") == "1"
    end

    test "ignores a degenerate clear_range", %{backend: backend} do
      put_layout(backend)
      put_slices(backend, "1", [{20, [{:set, "a", "1"}]}, {30, [{:clear_range, "b", "a"}]}])

      {:ok, image} = Replay.reconstruct(backend)

      assert value(image, "a") == "1"
    end

    test "folds atomic operations the way the materializer does", %{backend: backend} do
      put_layout(backend)

      put_slices(backend, "1", [
        {20, [{:set, "n", <<1, 0>>}]},
        {30, [{:atomic, :add, "n", <<2, 0>>}]},
        {40, [{:atomic, :add, "fresh", <<7>>}]}
      ])

      {:ok, image} = Replay.reconstruct(backend)

      assert value(image, "n") == <<3, 0>>
      assert value(image, "fresh") == <<7>>
    end

    test "a compare_and_clear that matches removes the key", %{backend: backend} do
      put_layout(backend)

      put_slices(backend, "1", [
        {20, [{:set, "n", <<5>>}]},
        {30, [{:atomic, :compare_and_clear, "n", <<5>>}]}
      ])

      {:ok, image} = Replay.reconstruct(backend)

      assert Replay.fetch(image, "n") == :error
    end

    test "drops privatized mutations addressed past the end of the keyspace", %{backend: backend} do
      put_layout(backend)

      put_slices(backend, "1", [
        {20,
         [
           {:set, "a", "1"},
           {:set, @end_of_keyspace <> "notice", "ignored"},
           {:clear, @end_of_keyspace <> "worker"}
         ]}
      ])

      {:ok, image} = Replay.reconstruct(backend)

      assert image.keys |> Map.keys() |> Enum.filter(&(&1 < <<0xFF>>)) == ["a"]
      refute Enum.any?(Map.keys(image.keys), &(&1 >= @end_of_keyspace))
    end

    test "replays every shard the layout names, including the system shard", %{backend: backend} do
      put_layout(backend)
      put_slices(backend, "1", [{20, [{:set, "a", "1"}]}])

      {:ok, image} = Replay.reconstruct(backend)

      assert value(image, "a") == "1"
      assert value(image, SystemKeys.shard_key(<<0xFF>>)) == Values.encode_shard_key_entry(1, <<>>)
    end

    test "carries the per-shard frontier and the anchor", %{backend: backend} do
      put_layout(backend, @default_layout, 10)
      put_slices(backend, "1", [{20, [{:set, "a", "1"}]}, {40, [{:set, "b", "1"}]}])

      {:ok, image} = Replay.reconstruct(backend)

      assert image.frontier == 40
      assert image.anchor == 10
      assert [%{shard_tag: "0", max_chunk_version: 10}, %{shard_tag: "1", max_chunk_version: 40}] = image.shards
    end

    test "truncates the replay at :through_version", %{backend: backend} do
      put_layout(backend)
      put_slices(backend, "1", [{20, [{:set, "a", "1"}]}, {40, [{:set, "a", "2"}]}])

      {:ok, image} = Replay.reconstruct(backend, through_version: 30)

      assert value(image, "a") == "1"
      assert image.through_version == 30
    end

    test "reports a layout shard that has flushed nothing", %{backend: backend} do
      put_layout(backend)

      {:ok, image} = Replay.reconstruct(backend)

      assert image.shards_without_chunks == ["1"]
    end

    test "reports chunks filed under a tag the layout does not name", %{backend: backend} do
      put_layout(backend)
      put_slices(backend, "1", [{20, [{:set, "a", "1"}]}])
      put_slices(backend, "7", [{20, [{:set, "zzz", "1"}]}])

      {:ok, image} = Replay.reconstruct(backend)

      assert image.orphan_shards == ["7"]
      assert Replay.fetch(image, "zzz") == :error
    end

    test "resolves a key two shards claim by the newer write, and says so", %{backend: backend} do
      put_layout(backend, %{<<0xFF>> => {1, <<>>}, @end_of_keyspace => {0, <<0xFF>>}, "m" => {2, <<>>}})
      put_slices(backend, "1", [{20, [{:set, "a", "from-1"}]}])
      put_slices(backend, "2", [{30, [{:set, "a", "from-2"}]}])

      {:ok, image} = Replay.reconstruct(backend)

      assert value(image, "a") == "from-2"
      assert image.multiply_claimed_keys == ["a"]
    end

    test "refuses to guess when the layout cannot be recovered", %{backend: backend} do
      {:error, reason} = Replay.reconstruct(backend)

      assert reason == {:layout_unavailable, :no_system_shard_chunks}
    end

    test "restricts the replay to :shards when asked", %{backend: backend} do
      put_layout(backend)
      put_slices(backend, "1", [{20, [{:set, "a", "1"}]}])

      {:ok, image} = Replay.reconstruct(backend, shards: ["1"])

      assert Map.keys(image.keys) == ["a"]
      assert image.orphan_shards == []
    end
  end

  # ---------------------------------------------------------------------- diff

  defp image_with(backend, mutations, version \\ 20) do
    put_layout(backend)
    put_slices(backend, "1", [{version, mutations}])
    {:ok, image} = Replay.reconstruct(backend)
    image
  end

  defp user_capture(version, pairs), do: LiveCapture.new(version, pairs, range: {<<>>, <<0xFF>>})

  describe "diff/3 disagreement categories" do
    test "agrees when both sides hold the same pairs", %{backend: backend} do
      image = image_with(backend, [{:set, "a", "1"}, {:set, "b", "2"}])

      diff = Replay.diff(image, user_capture(100, [{"a", "1"}, {"b", "2"}]), quiesced: true)

      assert diff.agreed?
      assert diff.counts == %{missing_from_live: 0, missing_from_storage: 0, value_mismatches: 0, explained_by_skew: 0}
    end

    test "reports a key object storage holds that the cluster does not serve", %{backend: backend} do
      image = image_with(backend, [{:set, "a", "1"}, {:set, "b", "2"}])

      diff = Replay.diff(image, user_capture(100, [{"a", "1"}]), quiesced: true)

      refute diff.agreed?
      assert [%{key: "b", storage_value: "2", storage_version: 20}] = diff.missing_from_live
      assert diff.counts.missing_from_live == 1
    end

    test "reports a key the cluster serves that object storage does not hold", %{backend: backend} do
      image = image_with(backend, [{:set, "a", "1"}])

      diff = Replay.diff(image, user_capture(100, [{"a", "1"}, {"b", "2"}]), quiesced: true)

      refute diff.agreed?
      assert [%{key: "b", live_value: "2"}] = diff.missing_from_storage
      assert diff.counts.missing_from_storage == 1
    end

    test "reports a key both sides hold with different values", %{backend: backend} do
      image = image_with(backend, [{:set, "a", "1"}])

      diff = Replay.diff(image, user_capture(100, [{"a", "other"}]), quiesced: true)

      refute diff.agreed?
      assert [%{key: "a", storage_value: "1", live_value: "other"}] = diff.value_mismatches
      assert diff.counts.value_mismatches == 1
    end

    test "caps the reported entries while keeping the counts honest", %{backend: backend} do
      image = image_with(backend, Enum.map(1..10, &{:set, "k#{&1}", "v"}))

      diff = Replay.diff(image, user_capture(100, []), limit: 3)

      assert length(diff.missing_from_live) == 3
      assert diff.counts.missing_from_live == 10
    end
  end

  describe "diff/3 anchoring" do
    test "exonerates a storage write that postdates the live read version", %{backend: backend} do
      image = image_with(backend, [{:set, "a", "1"}], 200)

      diff = Replay.diff(image, user_capture(100, []), quiesced: true)

      assert diff.agreed?
      assert diff.counts.explained_by_skew == 1
    end

    test "does not exonerate a storage write at or below the live read version", %{backend: backend} do
      image = image_with(backend, [{:set, "a", "1"}], 100)

      diff = Replay.diff(image, user_capture(100, []), quiesced: true)

      refute diff.agreed?
      assert diff.counts.missing_from_live == 1
    end

    test "is advisory unless the caller asserts quiescence", %{backend: backend} do
      image = image_with(backend, [{:set, "a", "1"}])

      assert Replay.diff(image, user_capture(100, [])).soundness == :advisory
      assert Replay.diff(image, user_capture(100, []), quiesced: true).soundness == :quiesced
    end

    test "flags a live read version ahead of everything storage has seen", %{backend: backend} do
      image = image_with(backend, [{:set, "a", "1"}])

      diff = Replay.diff(image, user_capture(100, [{"a", "1"}]), quiesced: true)

      assert :live_ahead_of_storage in diff.caveats
    end

    test "flags storage that has run ahead of the live read version", %{backend: backend} do
      image = image_with(backend, [{:set, "a", "1"}], 200)

      diff = Replay.diff(image, user_capture(100, [{"a", "1"}]), quiesced: true)

      assert :storage_ahead_of_live in diff.caveats
    end

    test "carries forward the reconstruction's own caveats", %{backend: backend} do
      put_layout(backend)
      put_slices(backend, "7", [{20, [{:set, "zzz", "1"}]}])
      {:ok, image} = Replay.reconstruct(backend)

      diff = Replay.diff(image, user_capture(100, []), quiesced: true)

      assert :shards_without_chunks in diff.caveats
      assert :orphan_shards in diff.caveats
    end

    test "compares only inside the range the capture covers", %{backend: backend} do
      image = image_with(backend, [{:set, "a", "1"}])

      diff = Replay.diff(image, user_capture(100, [{"a", "1"}]), quiesced: true)

      assert diff.agreed?
      assert diff.storage_key_count == 1
    end
  end

  # ------------------------------------------------------------------- compare

  describe "compare/3" do
    test "captures the live side first, then reconstructs, and diffs", %{backend: backend} do
      put_layout(backend)
      put_slices(backend, "1", [{20, [{:set, "a", "1"}]}])

      live_fun = fn -> {:ok, user_capture(100, [{"a", "1"}])} end

      {:ok, diff} = Replay.compare(backend, live_fun, quiesced: true)

      assert diff.agreed?
    end

    test "propagates a failure from the live side", %{backend: backend} do
      put_layout(backend)

      assert {:error, :no_gateway} = Replay.compare(backend, fn -> {:error, :no_gateway} end)
    end

    test "propagates a failure from the storage side without calling live", %{backend: backend} do
      live_fun = fn -> raise "must not be called" end

      assert {:error, {:layout_unavailable, :no_system_shard_chunks}} = Replay.compare(backend, live_fun)
    end
  end

  describe "LiveCapture.new/3" do
    test "accepts a version as an encoded 8-byte version or an integer" do
      assert LiveCapture.new(<<100::unsigned-big-64>>, []).version == 100
      assert LiveCapture.new(100, []).version == 100
    end

    test "covers the whole keyspace by default" do
      assert LiveCapture.new(1, []).range == {<<>>, @end_of_keyspace}
    end
  end
end
