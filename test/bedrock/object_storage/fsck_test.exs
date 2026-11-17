defmodule Bedrock.ObjectStorage.FsckTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Materializer.Olivine.IndexDatabase
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version
  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.Chunk
  alias Bedrock.ObjectStorage.Fsck
  alias Bedrock.ObjectStorage.Keys
  alias Bedrock.ObjectStorage.LocalFilesystem
  alias Bedrock.SystemKeys
  alias Bedrock.SystemKeys.Values

  @header_size 32
  @end_of_keyspace <<0xFF, 0xFF>>

  # The layout a fresh cluster bootstraps with: shard 1 covers the user
  # keyspace, shard 0 everything from \xFF up.
  @default_layout %{<<0xFF>> => {1, <<>>}, @end_of_keyspace => {0, <<0xFF>>}}

  # ---------------------------------------------------------------- fixtures

  setup do
    root = Path.join(System.tmp_dir!(), "fsck_test_#{:erlang.unique_integer([:positive])}")
    File.mkdir_p!(root)
    on_exit(fn -> File.rm_rf!(root) end)

    {:ok, backend: ObjectStorage.backend(LocalFilesystem, root: root), root: root}
  end

  defp txn(version) do
    Transaction.encode(%{
      mutations: [{:set, "k#{version}", "v#{version}"}],
      commit_version: <<version::unsigned-big-64>>
    })
  end

  defp chunk_for(versions) do
    {:ok, binary} = Chunk.encode(Enum.map(versions, &{&1, txn(&1)}))
    binary
  end

  defp key_for(shard_tag, version), do: Keys.chunk_path(shard_tag, version)

  defp put_chunk(backend, shard_tag, versions) do
    key = key_for(shard_tag, List.last(versions))
    :ok = ObjectStorage.put(backend, key, chunk_for(versions))
    key
  end

  # ----------------------------------------------------------- layout fixtures

  defp slice(version, mutations) do
    Transaction.encode(%{mutations: mutations, commit_version: <<version::unsigned-big-64>>})
  end

  defp put_slices(backend, shard_tag, versioned_mutations) do
    entries = Enum.map(versioned_mutations, fn {version, muts} -> {version, slice(version, muts)} end)
    {:ok, binary} = Chunk.encode(entries)
    {version, _} = List.last(entries)
    key = key_for(shard_tag, version)
    :ok = ObjectStorage.put(backend, key, binary)
    key
  end

  defp shard_key_sets(layout) do
    Enum.map(layout, fn {end_key, {tag, start_key}} ->
      {:set, SystemKeys.shard_key(end_key), Values.encode_shard_key_entry(tag, start_key)}
    end)
  end

  # Writes `layout` into the system shard's chunks the way recovery's
  # bootstrap transaction does, so fsck has something to replay.
  defp put_layout(backend, layout, version \\ 10) do
    put_slices(backend, "0", [{version, shard_key_sets(layout)}])
  end

  # Replaces `size` bytes at `offset` with `replacement`.
  defp splice(binary, offset, size, replacement) do
    <<head::binary-size(^offset), _::binary-size(^size), tail::binary>> = binary
    head <> replacement <> tail
  end

  defp patch_header(binary, field, value) do
    case field do
      :magic -> splice(binary, 0, 4, <<value::unsigned-big-32>>)
      :format_version -> splice(binary, 4, 1, <<value::unsigned-8>>)
      :min_version -> splice(binary, 8, 8, <<value::unsigned-big-64>>)
      :max_version -> splice(binary, 16, 8, <<value::unsigned-big-64>>)
      :txn_count -> splice(binary, 24, 4, <<value::unsigned-big-32>>)
      :directory_size -> splice(binary, 28, 4, <<value::unsigned-big-32>>)
    end
  end

  # ------------------------------------------------------------- check_chunk

  defp patch_directory_entry(binary, index, field, value) do
    base = @header_size + index * 16

    case field do
      :version -> splice(binary, base, 8, <<value::unsigned-big-64>>)
      :offset -> splice(binary, base + 8, 4, <<value::unsigned-big-32>>)
      :length -> splice(binary, base + 12, 4, <<value::unsigned-big-32>>)
    end
  end

  defp kinds(faults), do: Enum.map(faults, & &1.kind)

  describe "check_chunk/2 on a well-formed chunk" do
    test "reports no faults and recovers the range" do
      result = Fsck.check_chunk(key_for("a", 30), chunk_for([10, 20, 30]))

      assert result.faults == []
      assert result.range == {10, 30}
      assert result.txn_count == 3
      assert result.shard_tag == "a"
    end
  end

  describe "check_chunk/2 key checks" do
    test "faults when the basename is not a canonical inverted version" do
      result = Fsck.check_chunk("c/a/not-a-version", chunk_for([10]))

      assert :unparsable_key in kinds(result.faults)
    end

    test "faults when the basename parses but is out of the uint64 range" do
      # 13 base36 characters can exceed 2^64; Keys.key_to_version/1 raises
      # rather than returning an error on those, so fsck must not call it
      # blind.
      result = Fsck.check_chunk("c/a/zzzzzzzzzzzzz", chunk_for([10]))

      assert :unparsable_key in kinds(result.faults)
    end

    test "faults when the basename is not the canonical encoding of its version" do
      canonical = Path.basename(key_for("a", 10))
      result = Fsck.check_chunk("c/a/" <> String.upcase(canonical), chunk_for([10]))

      assert :unparsable_key in kinds(result.faults)
    end

    test "faults when the filename disagrees with the header's max version" do
      # The chunk holds 10..30 but is filed under 20. ChunkReader decides
      # from the name alone, so a read for 30 would never open this chunk.
      result = Fsck.check_chunk(key_for("a", 20), chunk_for([10, 20, 30]))

      assert [fault] = Enum.filter(result.faults, &(&1.kind == :key_version_mismatch))
      assert fault.detail == %{key_version: 20, header_max_version: 30}
    end

    test "faults when the key is not shaped c/{shard}/{version}" do
      result = Fsck.check_chunk("c/a/b/3w5e11264sg0n", chunk_for([10]))

      assert :malformed_chunk_key in kinds(result.faults)
    end
  end

  describe "check_chunk/2 header checks" do
    test "faults on a short object that cannot hold a header" do
      result = Fsck.check_chunk(key_for("a", 10), binary_part(chunk_for([10]), 0, @header_size - 1))

      assert kinds(result.faults) == [:truncated_header]
    end

    test "faults on a bad magic number" do
      result = Fsck.check_chunk(key_for("a", 10), patch_header(chunk_for([10]), :magic, 0xDEADBEEF))

      assert [fault] = result.faults
      assert fault.kind == :bad_magic
      assert fault.detail == %{magic: 0xDEADBEEF, expected: 0x42444348}
    end

    test "faults on an unsupported format version" do
      result = Fsck.check_chunk(key_for("a", 10), patch_header(chunk_for([10]), :format_version, 0x02))

      assert [fault] = result.faults
      assert fault.kind == :unsupported_format_version
    end

    test "faults when min_version exceeds max_version" do
      binary = chunk_for([10, 20])
      result = Fsck.check_chunk(key_for("a", 20), patch_header(binary, :min_version, 99))

      assert :version_range_inverted in kinds(result.faults)
    end

    test "faults on a zero transaction count" do
      binary = [10] |> chunk_for() |> patch_header(:txn_count, 0) |> patch_header(:directory_size, 0)
      result = Fsck.check_chunk(key_for("a", 10), binary)

      assert :empty_directory in kinds(result.faults)
    end

    test "faults when directory_size disagrees with txn_count" do
      result = Fsck.check_chunk(key_for("a", 20), patch_header(chunk_for([10, 20]), :directory_size, 48))

      assert [fault] = result.faults
      assert fault.kind == :directory_size_mismatch
      assert fault.detail == %{directory_size: 48, txn_count: 2, expected: 32}
    end

    test "faults when the object is too short to hold the directory it claims" do
      binary = chunk_for([10, 20])
      result = Fsck.check_chunk(key_for("a", 20), binary_part(binary, 0, @header_size + 16))

      assert [fault] = result.faults
      assert fault.kind == :truncated_directory
    end

    test "faults when header min does not match the first directory entry" do
      binary = chunk_for([10, 20, 30])

      result =
        Fsck.check_chunk(
          key_for("a", 30),
          binary |> patch_header(:min_version, 5) |> patch_header(:max_version, 30)
        )

      assert [fault] = Enum.filter(result.faults, &(&1.kind == :header_min_mismatch))
      assert fault.detail == %{header_min_version: 5, first_entry_version: 10}
    end

    test "faults when header max does not match the last directory entry" do
      binary = patch_header(chunk_for([10, 20, 30]), :max_version, 40)
      result = Fsck.check_chunk(key_for("a", 40), binary)

      assert [fault] = Enum.filter(result.faults, &(&1.kind == :header_max_mismatch))
      assert fault.detail == %{header_max_version: 40, last_entry_version: 30}
    end
  end

  describe "check_chunk/2 directory checks" do
    test "faults when directory entries do not ascend by version" do
      # Swap the directory versions so entry 0 is newer than entry 1, while
      # keeping header min/max consistent with the (now reordered) ends.
      binary =
        [10, 20, 30]
        |> chunk_for()
        |> patch_directory_entry(0, :version, 25)
        |> patch_header(:min_version, 25)

      result = Fsck.check_chunk(key_for("a", 30), binary)

      assert [fault] = Enum.filter(result.faults, &(&1.kind == :directory_not_ascending))
      assert fault.detail == %{index: 1, version: 20, previous_version: 25}
    end

    test "faults when the data section stops short of the directory's extents" do
      binary = chunk_for([10, 20, 30])
      result = Fsck.check_chunk(key_for("a", 30), binary_part(binary, 0, byte_size(binary) - 3))

      assert [fault] = Enum.filter(result.faults, &(&1.kind == :data_section_truncated))
      assert fault.detail.actual == fault.detail.required - 3
    end
  end

  describe "check_chunk/2 transaction checks" do
    test "faults when a slice does not decode as a transaction" do
      binary = chunk_for([10, 20])
      # Corrupt the first byte of the data section (the BRDT magic).
      data_start = @header_size + 32
      result = Fsck.check_chunk(key_for("a", 20), splice(binary, data_start, 1, <<0x00>>))

      assert [fault] = Enum.filter(result.faults, &(&1.kind == :transaction_undecodable))
      assert fault.detail.version == 10
    end

    test "faults when a slice's commit version disagrees with its directory entry" do
      # The directory says 15; the transaction inside still says 10.
      binary = [10, 20] |> chunk_for() |> patch_directory_entry(0, :version, 15) |> patch_header(:min_version, 15)
      result = Fsck.check_chunk(key_for("a", 20), binary)

      assert [fault] = Enum.filter(result.faults, &(&1.kind == :transaction_version_mismatch))
      assert fault.detail == %{entry_version: 15, commit_version: 10}
    end

    test "faults when a slice carries no commit version at all" do
      naked = Transaction.encode(%{mutations: [{:set, "k", "v"}]})
      {:ok, binary} = Chunk.encode([{10, naked}])

      result = Fsck.check_chunk(key_for("a", 10), binary)

      assert :transaction_missing_commit_version in kinds(result.faults)
    end

    test "check_transactions: false skips the slice decode" do
      binary = chunk_for([10, 20])
      data_start = @header_size + 32
      corrupt = splice(binary, data_start, 1, <<0x00>>)

      # -------------------------------------------------------------- check/2
      assert Fsck.check_chunk(key_for("a", 20), corrupt, check_transactions: false).faults == []
    end
  end

  describe "check/2 over a store" do
    test "a healthy store is clean", %{backend: backend} do
      put_chunk(backend, "a", [10, 20])
      put_chunk(backend, "a", [30, 40])
      put_chunk(backend, "a", [50, 60])

      report = Fsck.check(backend)

      assert report.clean?
      assert report.faults == []
      assert report.chunk_count == 3
      assert [shard] = report.shards
      assert shard.shard_tag == "a"
      assert shard.range == {10, 60}
    end

    test "an empty store is clean", %{backend: backend} do
      report = Fsck.check(backend)

      assert report.clean?
      assert report.shards == []
      assert report.chunk_count == 0
    end

    test "chunks from several shards are grouped and checked per shard", %{backend: backend} do
      put_chunk(backend, "a", [10, 20])
      put_chunk(backend, "b", [10, 20])

      report = Fsck.check(backend)

      assert report.clean?
      assert Enum.map(report.shards, & &1.shard_tag) == ["a", "b"]
    end

    test "per-chunk faults surface in the flattened report", %{backend: backend} do
      put_chunk(backend, "a", [10, 20])
      :ok = ObjectStorage.put(backend, key_for("a", 40), patch_header(chunk_for([30, 40]), :magic, 0))

      report = Fsck.check(backend)

      refute report.clean?
      assert [fault] = report.faults
      assert fault.kind == :bad_magic
      assert fault.shard_tag == "a"
      assert fault.key == key_for("a", 40)
    end

    test "restricting to a shard skips the others", %{backend: backend} do
      put_chunk(backend, "a", [10, 20])
      :ok = ObjectStorage.put(backend, key_for("b", 20), patch_header(chunk_for([10, 20]), :magic, 0))

      report = Fsck.check(backend, shards: ["a"])

      assert report.clean?
      assert Enum.map(report.shards, & &1.shard_tag) == ["a"]
    end
  end

  describe "check/2 per-shard range analysis" do
    test "faults when two chunks in a shard cover the same version", %{backend: backend} do
      put_chunk(backend, "a", [10, 20, 30])
      put_chunk(backend, "a", [25, 40])

      report = Fsck.check(backend)

      refute report.clean?
      assert [fault] = Enum.filter(report.faults, &(&1.kind == :chunk_range_overlap))
      assert fault.shard_tag == "a"

      assert fault.detail == %{
               earlier_key: key_for("a", 30),
               earlier_range: {10, 30},
               later_key: key_for("a", 40),
               later_range: {25, 40}
             }
    end

    test "faults when one chunk's range wholly contains another's", %{backend: backend} do
      put_chunk(backend, "a", [10, 50])
      put_chunk(backend, "a", [20, 30])

      report = Fsck.check(backend)

      assert [fault] = Enum.filter(report.faults, &(&1.kind == :chunk_range_overlap))
      assert fault.detail.earlier_range == {10, 50}
      assert fault.detail.later_range == {20, 30}
    end

    test "a version gap between chunks is a note, never a fault", %{backend: backend} do
      put_chunk(backend, "a", [10, 20])
      put_chunk(backend, "a", [500, 600])

      report = Fsck.check(backend)

      assert report.clean?
      assert report.faults == []
      assert [note] = Enum.filter(report.notes, &(&1.kind == :version_gap))
      assert note.shard_tag == "a"

      assert note.detail == %{
               after_version: 20,
               before_version: 500,
               after_key: key_for("a", 20),
               before_key: key_for("a", 600)
             }

      assert [shard] = report.shards
      assert length(shard.gaps) == 1
    end

    test "old, superseded chunks are not faults", %{backend: backend} do
      # Chunks are never deleted; a shard that has snapshotted past version
      # 1000 still has all its old chunks sitting there, and that is legal.
      put_chunk(backend, "a", [10, 20])
      put_chunk(backend, "a", [30, 40])
      put_chunk(backend, "a", [1000, 1010])

      report = Fsck.check(backend)

      assert report.clean?
      assert report.faults == []
    end

    test "range analysis is marked partial when a chunk's range is untrustworthy", %{backend: backend} do
      put_chunk(backend, "a", [10, 20])
      :ok = ObjectStorage.put(backend, key_for("a", 40), <<0::64>>)

      report = Fsck.check(backend)

      refute report.clean?
      assert [shard] = report.shards
      assert shard.range_analysis == :partial
    end

    test "range analysis is complete on a healthy shard", %{backend: backend} do
      put_chunk(backend, "a", [10, 20])

      report = Fsck.check(backend)

      assert [shard] = report.shards
      assert shard.range_analysis == :complete
    end
  end

  defmodule VanishingBackend do
    @moduledoc false
    @behaviour ObjectStorage

    # A chunk that the listing turned up but that is gone by the time it is
    # read — the ordinary shape of a store being changed underneath a
    # reader, and the one a chaos harness sees most.
    @impl true
    def get(_config, _key), do: {:error, :not_found}

    @impl true
    def list(config, prefix, opts \\ []), do: LocalFilesystem.list(config, prefix, opts)
    @impl true
    def put(config, key, data, opts \\ []), do: LocalFilesystem.put(config, key, data, opts)
    @impl true
    def delete(config, key), do: LocalFilesystem.delete(config, key)
    @impl true
    def put_if_not_exists(config, key, data, opts \\ []), do: LocalFilesystem.put_if_not_exists(config, key, data, opts)
    @impl true
    def get_with_version(config, key), do: LocalFilesystem.get_with_version(config, key)
    @impl true
    def put_if_version_matches(config, key, token, data, opts \\ []),
      do: LocalFilesystem.put_if_version_matches(config, key, token, data, opts)
  end

  describe "check/2 unreadable chunks" do
    test "faults when a listed chunk cannot be read", %{backend: backend, root: root} do
      put_chunk(backend, "a", [10, 20])

      report = Fsck.check(ObjectStorage.backend(VanishingBackend, root: root))

      refute report.clean?
      assert [fault] = report.faults
      assert fault.kind == :unreadable
      assert fault.key == key_for("a", 20)
      assert fault.detail == %{reason: :not_found}
      assert [shard] = report.shards
      assert shard.range_analysis == :partial
    end
  end

  describe "check/2 scratch debris" do
    test "reports .bedrock-tmp.* debris as a note, not a fault", %{backend: backend, root: root} do
      put_chunk(backend, "a", [10, 20])
      debris = Path.join([root, "c", "a", ".bedrock-tmp.abc.nonode@nohost.1.2"])
      File.write!(debris, "half a chunk")

      report = Fsck.check(backend)

      assert report.clean?
      assert report.faults == []
      assert [note] = Enum.filter(report.notes, &(&1.kind == :scratch_debris))
      assert note.key == "c/a/.bedrock-tmp.abc.nonode@nohost.1.2"
      assert note.detail == %{bytes: 12}
    end

    test "a shard filter scopes the debris sweep too", %{backend: backend, root: root} do
      put_chunk(backend, "a", [10, 20])
      put_chunk(backend, "b", [10, 20])
      File.write!(Path.join([root, "c", "b", ".bedrock-tmp.abc"]), "x")

      assert Enum.filter(Fsck.check(backend, shards: ["a"]).notes, &(&1.kind == :scratch_debris)) == []
      assert [_] = Enum.filter(Fsck.check(backend, shards: ["b"]).notes, &(&1.kind == :scratch_debris))
    end

    test "a store with no debris reports none", %{backend: backend} do
      put_chunk(backend, "a", [10, 20])

      assert Enum.filter(Fsck.check(backend).notes, &(&1.kind == :scratch_debris)) == []
    end
  end

  # ------------------------------------------------------------------ layout

  describe "check/2 layout recovery" do
    test "replays the system shard's chunks into the shard layout", %{backend: backend} do
      put_layout(backend, @default_layout)

      report = Fsck.check(backend)

      assert report.clean?
      assert report.layout.status == :recovered
      assert report.layout.containment == :decidable

      assert report.layout.shards == [
               %{tag: 1, start_key: <<>>, end_key: <<0xFF>>},
               %{tag: 0, start_key: <<0xFF>>, end_key: @end_of_keyspace}
             ]

      assert report.layout.ranges_by_tag == %{
               "0" => [{<<0xFF>>, @end_of_keyspace}],
               "1" => [{<<>>, <<0xFF>>}]
             }
    end

    test "a later transaction's clear removes the entry it names", %{backend: backend} do
      # Shard 1 is split at "m" into 1 and 2, and the old boundary cleared.
      split = %{"m" => {1, <<>>}, <<0xFF>> => {2, "m"}, @end_of_keyspace => {0, <<0xFF>>}}

      put_slices(backend, "0", [
        {10, shard_key_sets(@default_layout)},
        {20, shard_key_sets(split)}
      ])

      report = Fsck.check(backend)

      assert report.clean?
      assert Enum.map(report.layout.shards, & &1.tag) == [1, 2, 0]

      # Shard 1's earlier, wider range survives in the containment domain:
      # the chunks it wrote under it are not retroactively wrong.
      assert Enum.sort(report.layout.ranges_by_tag["1"]) == [{<<>>, "m"}, {<<>>, <<0xFF>>}]
    end

    test "a clear retires the boundary it names", %{backend: backend} do
      # A merge: shards 1 and 2 become one, so the "m" boundary is cleared
      # and 1 widens to take it back. Ignoring the clear would leave both
      # the old and the new entry live, and the layout would overlap.
      split = %{"m" => {1, <<>>}, <<0xFF>> => {2, "m"}, @end_of_keyspace => {0, <<0xFF>>}}

      put_slices(backend, "0", [
        {10, shard_key_sets(split)},
        {20, [{:clear, SystemKeys.shard_key("m")} | shard_key_sets(%{<<0xFF>> => {1, <<>>}})]}
      ])

      report = Fsck.check(backend)

      assert report.clean?
      assert Enum.map(report.layout.shards, & &1.tag) == [1, 0]
    end

    test "a clear_range over the family drops every entry it covers", %{backend: backend} do
      prefix = SystemKeys.shard_keys_prefix()

      put_slices(backend, "0", [
        {10, shard_key_sets(@default_layout)},
        {20, [{:clear_range, prefix, prefix <> <<0xFF, 0xFF, 0xFF>>}]}
      ])

      report = Fsck.check(backend)

      assert report.layout.status == :unavailable
      assert report.layout.reason == :no_shard_keys_entries
    end

    test "no system shard chunks leaves the layout unavailable, not faulted", %{backend: backend} do
      put_chunk(backend, "1", [10, 20])

      report = Fsck.check(backend)

      assert report.clean?
      assert report.layout.status == :unavailable
      assert report.layout.reason == :no_system_shard_chunks
      assert [note] = Enum.filter(report.notes, &(&1.kind == :layout_unavailable))
      assert note.detail == %{reason: :no_system_shard_chunks}
    end

    test "check_layout: false skips recovery entirely", %{backend: backend} do
      put_layout(backend, @default_layout)

      assert Fsck.check(backend, check_layout: false).layout == nil
    end
  end

  describe "check/2 layout coverage" do
    test "faults when the layout leaves a hole between two shards", %{backend: backend} do
      gapped = %{"d" => {1, <<>>}, <<0xFF>> => {2, "h"}, @end_of_keyspace => {0, <<0xFF>>}}
      put_layout(backend, gapped)

      report = Fsck.check(backend)

      refute report.clean?
      assert [fault] = Enum.filter(report.faults, &(&1.kind == :layout_gap))
      assert fault.detail == %{after_key: "d", before_key: "h"}
    end

    test "faults when the layout does not start at the beginning of the keyspace", %{backend: backend} do
      late = %{<<0xFF>> => {1, "a"}, @end_of_keyspace => {0, <<0xFF>>}}
      put_layout(backend, late)

      report = Fsck.check(backend)

      assert [fault] = Enum.filter(report.faults, &(&1.kind == :layout_gap))
      assert fault.detail == %{after_key: <<>>, before_key: "a"}
    end

    test "faults when the layout stops short of the end of the keyspace", %{backend: backend} do
      short = %{<<0xFF>> => {1, <<>>}}
      put_layout(backend, short)

      report = Fsck.check(backend)

      assert [fault] = Enum.filter(report.faults, &(&1.kind == :layout_gap))
      assert fault.detail == %{after_key: <<0xFF>>, before_key: @end_of_keyspace}
    end

    test "faults when two shards claim the same keys", %{backend: backend} do
      overlapping = %{"m" => {1, <<>>}, <<0xFF>> => {2, "d"}, @end_of_keyspace => {0, <<0xFF>>}}
      put_layout(backend, overlapping)

      report = Fsck.check(backend)

      refute report.clean?
      assert [fault] = Enum.filter(report.faults, &(&1.kind == :layout_overlap))
      assert fault.detail == %{covered_through: "m", start_key: "d", end_key: <<0xFF>>}
    end

    test "a tag that names no object-storage prefix is reported, not raised on", %{backend: backend} do
      # `decode_shard_key_entry/1` promises an integer and nothing more; a
      # negative one round-trips through it, and `Keys.shard_tag/1` has no
      # clause for it. An operator's fsck must report that, not crash in it.
      odd = %{<<0xFF>> => {-3, <<>>}, @end_of_keyspace => {0, <<0xFF>>}}
      put_layout(backend, odd)

      report = Fsck.check(backend)

      assert report.clean?
      assert Enum.map(report.layout.shards, & &1.tag) == [-3, 0]
      assert Map.keys(report.layout.ranges_by_tag) == ["0"]
    end

    test "faults on a shard_keys value that will not decode", %{backend: backend} do
      key = SystemKeys.shard_key(<<0xFF>>)
      put_slices(backend, "0", [{10, [{:set, key, "not a packed tuple"}]}])

      report = Fsck.check(backend)

      refute report.clean?
      assert [fault] = Enum.filter(report.faults, &(&1.kind == :layout_undecodable_entry))
      assert fault.key == key
      assert report.layout.status == :unavailable
    end
  end

  describe "check/2 layout recovery degrades when the system shard is broken" do
    test "a faulted system shard chunk abandons recovery rather than replaying it", %{backend: backend} do
      put_layout(backend, @default_layout)
      # A second system chunk with bad magic: the replay would silently
      # skip it and report a layout derived from whatever survived.
      :ok = ObjectStorage.put(backend, key_for("0", 40), patch_header(chunk_for([30, 40]), :magic, 0))

      report = Fsck.check(backend)

      refute report.clean?
      assert report.layout.status == :unavailable
      assert report.layout.reason == :system_shard_unhealthy
      # The structural fault is still reported once, by the main pass.
      assert [_] = Enum.filter(report.faults, &(&1.kind == :bad_magic))
    end

    test "overlapping system chunks abandon recovery: the replay would double-apply", %{backend: backend} do
      put_slices(backend, "0", [{10, shard_key_sets(@default_layout)}, {30, []}])
      put_slices(backend, "0", [{25, []}, {40, []}])

      report = Fsck.check(backend)

      assert report.layout.status == :unavailable
      assert report.layout.reason == :system_shard_unhealthy
    end

    test "a system chunk filed under the wrong version abandons recovery", %{backend: backend} do
      # ChunkReader selects from the name alone, so this chunk is skipped
      # by a read that wants version 30 — a silent hole in the replay.
      {:ok, binary} = Chunk.encode([{10, slice(10, shard_key_sets(@default_layout))}, {30, slice(30, [])}])
      :ok = ObjectStorage.put(backend, key_for("0", 20), binary)

      report = Fsck.check(backend)

      assert report.layout.status == :unavailable
      assert report.layout.reason == :system_shard_unhealthy
    end

    test "no layout-derived fault is reported when the layout is unavailable", %{backend: backend} do
      # Shard 1 holds a key that belongs to shard 0, but with no trustworthy
      # map there is nothing to say so.
      put_slices(backend, "1", [{10, [{:set, <<0xFF, "x">>, "v"}]}])

      report = Fsck.check(backend)

      assert report.clean?
      assert Enum.filter(report.faults, &(&1.kind == :mutation_out_of_shard_range)) == []
    end
  end

  describe "check/2 key containment" do
    setup %{backend: backend} do
      put_layout(backend, @default_layout)
      :ok
    end

    test "a shard holding only its own keys is clean", %{backend: backend} do
      put_slices(backend, "1", [{20, [{:set, "apple", "v"}, {:clear, "banana"}]}])

      report = Fsck.check(backend)

      assert report.clean?
    end

    test "faults when a set lands outside the shard's range", %{backend: backend} do
      key = put_slices(backend, "1", [{20, [{:set, "apple", "v"}, {:set, <<0xFF, "sys">>, "v"}]}])

      report = Fsck.check(backend)

      refute report.clean?
      assert [fault] = Enum.filter(report.faults, &(&1.kind == :mutation_out_of_shard_range))
      assert fault.shard_tag == "1"
      assert fault.key == key
      assert fault.detail.version == 20
      assert fault.detail.key == <<0xFF, "sys">>
      assert fault.detail.count == 1
      assert fault.detail.shard_ranges == [{<<>>, <<0xFF>>}]
    end

    test "faults when a clear lands outside the shard's range", %{backend: backend} do
      put_slices(backend, "0", [{20, [{:clear, "user-key"}]}])

      report = Fsck.check(backend)

      assert [fault] = Enum.filter(report.faults, &(&1.kind == :mutation_out_of_shard_range))
      assert fault.shard_tag == "0"
      assert fault.detail.key == "user-key"
    end

    test "faults when an atomic op lands outside the shard's range", %{backend: backend} do
      put_slices(backend, "1", [{20, [{:atomic, :add, <<0xFF, "counter">>, <<1::64>>}]}])

      report = Fsck.check(backend)

      assert [_] = Enum.filter(report.faults, &(&1.kind == :mutation_out_of_shard_range))
    end

    test "faults when a clear_range straddles the shard's boundary", %{backend: backend} do
      # The proxy clamps each routed copy to the owning shard's bounds, so
      # an unclamped range in a persisted slice was never routed.
      put_slices(backend, "1", [{20, [{:clear_range, "m", <<0xFF, "z">>}]}])

      report = Fsck.check(backend)

      assert [fault] = Enum.filter(report.faults, &(&1.kind == :mutation_out_of_shard_range))
      assert fault.detail.key == {"m", <<0xFF, "z">>}
    end

    test "a clear_range inside the shard's range is clean", %{backend: backend} do
      put_slices(backend, "1", [{20, [{:clear_range, "m", "q"}]}])

      assert Fsck.check(backend).clean?
    end

    test "one fault per chunk, carrying the first offender and a count", %{backend: backend} do
      put_slices(backend, "1", [
        {20, [{:set, <<0xFF, "a">>, "v"}, {:set, <<0xFF, "b">>, "v"}]},
        {30, [{:set, <<0xFF, "c">>, "v"}]}
      ])

      report = Fsck.check(backend)

      assert [fault] = Enum.filter(report.faults, &(&1.kind == :mutation_out_of_shard_range))
      assert fault.detail.version == 20
      assert fault.detail.key == <<0xFF, "a">>
      assert fault.detail.count == 3
    end

    test "a privatized notice past the end of the keyspace is not a fault", %{backend: backend} do
      # The proxy prefixes a membership clear past every boundary and
      # addresses it to a tag, precisely so no shard can store it. It
      # still rides that shard's stream into that shard's chunks.
      notice = @end_of_keyspace <> SystemKeys.materializer_key(1, "worker-7")
      put_slices(backend, "1", [{20, [{:clear, notice}]}])

      report = Fsck.check(backend)

      assert report.clean?
    end

    test "a degenerate empty clear_range names no keys and is not a fault", %{backend: backend} do
      put_slices(backend, "1", [{20, [{:clear_range, <<0xFF, "a">>, <<0xFF, "a">>}]}])

      assert Fsck.check(backend).clean?
    end

    test "containment is checked against every range the tag has held", %{backend: backend} do
      # Shard 1 gave up ["m", 0xFF) in a later transaction. Its older
      # chunks still hold keys from that range, and they are correct.
      split = %{"m" => {1, <<>>}, <<0xFF>> => {2, "m"}, @end_of_keyspace => {0, <<0xFF>>}}
      put_slices(backend, "0", [{40, shard_key_sets(split)}])
      put_slices(backend, "1", [{20, [{:set, "zebra", "v"}]}])

      report = Fsck.check(backend)

      assert report.clean?
    end

    test "chunks under a tag the layout does not name are not checked", %{backend: backend} do
      put_slices(backend, "7", [{20, [{:set, "apple", "v"}]}])

      report = Fsck.check(backend)

      assert report.clean?
      assert [note] = Enum.filter(report.notes, &(&1.kind == :shard_not_in_layout))
      assert note.shard_tag == "7"
    end

    test "containment survives a shard filter", %{backend: backend} do
      put_slices(backend, "1", [{20, [{:set, <<0xFF, "sys">>, "v"}]}])

      report = Fsck.check(backend, shards: ["1"])

      assert [_] = Enum.filter(report.faults, &(&1.kind == :mutation_out_of_shard_range))
    end

    test "skip_transactions takes containment with it", %{backend: backend} do
      put_slices(backend, "1", [{20, [{:set, <<0xFF, "sys">>, "v"}]}])

      report = Fsck.check(backend, check_transactions: false)

      assert report.clean?
    end

    test "a legacy shard_keys family suspends containment", %{backend: backend} do
      # Pre-Values entries carry a bare tag; start keys are reconstructed
      # by adjacency, which is a current map with no history behind it.
      legacy =
        Enum.map(@default_layout, fn {end_key, {tag, _start}} ->
          {:set, SystemKeys.shard_key(end_key), :erlang.term_to_binary(tag)}
        end)

      :ok = ObjectStorage.delete(backend, key_for("0", 10))
      put_slices(backend, "0", [{10, legacy}])
      put_slices(backend, "1", [{20, [{:set, <<0xFF, "sys">>, "v"}]}])

      report = Fsck.check(backend)

      assert report.clean?
      assert report.layout.status == :recovered
      assert report.layout.containment == :undecidable
      assert [_] = Enum.filter(report.notes, &(&1.kind == :containment_undecidable))
    end
  end

  describe "check/2 tag and prefix correspondence" do
    test "a layout shard with no chunks is a note, not a fault", %{backend: backend} do
      # Shard 1 is named by the layout but has never flushed. Chunks
      # appear only after a flush, so this is an ordinary young cluster.
      put_layout(backend, @default_layout)

      report = Fsck.check(backend)

      assert report.clean?
      assert [note] = Enum.filter(report.notes, &(&1.kind == :layout_shard_without_chunks))
      assert note.shard_tag == "1"
    end

    test "chunks under a tag the layout forgot are a note, not a fault", %{backend: backend} do
      put_layout(backend, @default_layout)
      put_chunk(backend, "7", [20, 30])

      report = Fsck.check(backend)

      assert report.clean?
      assert [note] = Enum.filter(report.notes, &(&1.kind == :shard_not_in_layout))
      assert note.shard_tag == "7"
    end

    test "correspondence is not reported under a shard filter", %{backend: backend} do
      put_layout(backend, @default_layout)
      put_chunk(backend, "7", [20, 30])

      report = Fsck.check(backend, shards: ["7"])

      kinds = Enum.map(report.notes, & &1.kind)
      refute :shard_not_in_layout in kinds
      refute :layout_shard_without_chunks in kinds
    end
  end

  describe "check_chunk/3 containment" do
    test "checks a chunk against ranges handed to it directly" do
      {:ok, binary} = Chunk.encode([{20, slice(20, [{:set, "zebra", "v"}])}])

      result = Fsck.check_chunk(key_for("a", 20), binary, layout_ranges: %{"a" => [{<<>>, "m"}]})

      assert [fault] = result.faults
      assert fault.kind == :mutation_out_of_shard_range
    end

    test "a tag absent from the ranges map is not checked" do
      {:ok, binary} = Chunk.encode([{20, slice(20, [{:set, "zebra", "v"}])}])

      assert Fsck.check_chunk(key_for("a", 20), binary, layout_ranges: %{"b" => [{<<>>, "m"}]}).faults == []
    end
  end

  # ------------------------------------------------------- snapshot fixtures

  # A well-formed bundle is built by the WRITER — `Database.compact/4`
  # ends in exactly this call — so a change to the record format fails
  # these tests instead of quietly agreeing with a stale hand-rolled copy.
  defp index_record(version, pages_map \\ %{}) do
    version |> Version.from_integer() |> IndexDatabase.build_snapshot_record(pages_map) |> IO.iodata_to_binary()
  end

  # `[data][index record]`, exactly the iodata `maybe_upload_snapshot/4`
  # hands to `Snapshot.write/3`.
  defp bundle(version, opts \\ []) do
    data = Keyword.get(opts, :data, "compacted-page-data")
    record = Keyword.get(opts, :record, index_record(version))
    data <> record
  end

  # A record from a LIVE append chain rather than a compaction: its
  # previous_version points at an older record, not at itself.
  defp delta_record(version, previous_version, pages_map) do
    payload = :erlang.term_to_binary({Version.from_integer(previous_version), pages_map})
    size = byte_size(payload)
    <<0x4F4C5644::unsigned-big-32, version::unsigned-big-64, size::unsigned-big-32>> <> payload <> <<size::32>>
  end

  defp put_snapshot(backend, shard_tag, version, opts \\ []) do
    key = Keys.snapshot_path(shard_tag, version)
    :ok = ObjectStorage.put(backend, key, bundle(version, opts))
    key
  end

  # ---------------------------------------------------------- check_snapshot

  describe "check_snapshot/2 on a well-formed bundle" do
    test "reports no faults and recovers the embedded durable version" do
      result = Fsck.check_snapshot(Keys.snapshot_path("a", 20), bundle(20))

      assert result.faults == []
      assert result.shard_tag == "a"
      assert result.version == 20
      assert result.durable_version == 20
    end

    test "an empty page map is a legitimately compacted empty shard" do
      assert Fsck.check_snapshot(Keys.snapshot_path("a", 0), bundle(0, data: "")).faults == []
    end
  end

  describe "check_snapshot/2 key checks" do
    test "faults when the key is not s/{shard}/{version}" do
      result = Fsck.check_snapshot("s/a", bundle(20))

      assert :malformed_snapshot_key in kinds(result.faults)
    end

    test "faults when the basename is not the canonical encoding of its version" do
      canonical = Path.basename(Keys.snapshot_path("a", 20))
      result = Fsck.check_snapshot("s/a/" <> String.upcase(canonical), bundle(20))

      assert :unparsable_snapshot_key in kinds(result.faults)
    end

    test "faults when the basename parses but is out of the uint64 range" do
      assert :unparsable_snapshot_key in kinds(Fsck.check_snapshot("s/a/zzzzzzzzzzzzz", bundle(20)).faults)
    end
  end

  describe "check_snapshot/2 index record checks" do
    test "faults when the bundle is too short to hold an index record" do
      result = Fsck.check_snapshot(Keys.snapshot_path("a", 20), "tiny")

      assert [fault] = result.faults
      assert fault.kind == :snapshot_index_record_invalid
      assert fault.detail.reason == :invalid_bundle
    end

    test "faults when the index record's magic is wrong" do
      record = index_record(20)
      corrupt = splice(record, 0, 4, <<0::unsigned-big-32>>)

      result = Fsck.check_snapshot(Keys.snapshot_path("a", 20), bundle(20, record: corrupt))

      assert [fault] = result.faults
      assert fault.kind == :snapshot_index_record_invalid
      assert fault.detail.reason == :no_index_record
    end

    test "faults when the footer's payload size overruns the bundle" do
      record = index_record(20)
      overrun = splice(record, byte_size(record) - 4, 4, <<0xFFFFFF::unsigned-big-32>>)

      result = Fsck.check_snapshot(Keys.snapshot_path("a", 20), bundle(20, record: overrun))

      assert [fault] = result.faults
      assert fault.kind == :snapshot_index_record_invalid
      assert fault.detail.reason == :invalid_bundle
    end

    test "faults when the header and footer disagree about the payload size" do
      # `find_index_boundary/1` trusts the footer and never looks at the
      # header's copy, so a restore SUCCEEDS here — and then
      # `IndexDatabase.read_durable_version/2` fails its own match and
      # silently answers version zero, which loads no pages at all.
      record = index_record(20)
      header_lie = splice(record, 12, 4, <<7::unsigned-big-32>>)

      result = Fsck.check_snapshot(Keys.snapshot_path("a", 20), bundle(20, record: header_lie))

      assert [fault] = result.faults
      assert fault.kind == :snapshot_index_size_disagreement
      assert fault.detail.header_payload_size == 7
    end

    test "faults when the record's version is not the one the key names" do
      result = Fsck.check_snapshot(Keys.snapshot_path("a", 20), bundle(20, record: index_record(30)))

      assert [fault] = result.faults
      assert fault.kind == :snapshot_version_mismatch
      assert fault.detail == %{key_version: 20, record_version: 30}
    end

    test "faults when the index record is a delta rather than a compaction base" do
      # The live idx file is a chain of per-window deltas. Uploading one
      # raw gives the restored shard that single delta as its whole
      # index: the chain walk stops at a record the bundle does not
      # contain, and the shard comes up nearly empty.
      record = delta_record(20, 10, %{1 => {<<>>, 0}})

      result = Fsck.check_snapshot(Keys.snapshot_path("a", 20), bundle(20, record: record))

      assert [fault] = result.faults
      assert fault.kind == :snapshot_index_not_a_base
      assert fault.detail.version == 20
      assert fault.detail.previous_version == 10
    end

    test "faults when the payload will not decode as a page block" do
      payload = "not a term"
      size = byte_size(payload)
      record = <<0x4F4C5644::unsigned-big-32, 20::unsigned-big-64, size::32>> <> payload <> <<size::32>>

      result = Fsck.check_snapshot(Keys.snapshot_path("a", 20), bundle(20, record: record))

      assert [fault] = result.faults
      assert fault.kind == :snapshot_index_undecodable
    end

    test "faults when the payload decodes to something that is not a page block" do
      payload = :erlang.term_to_binary({Version.from_integer(20), "not a page map"})
      size = byte_size(payload)
      record = <<0x4F4C5644::unsigned-big-32, 20::unsigned-big-64, size::32>> <> payload <> <<size::32>>

      result = Fsck.check_snapshot(Keys.snapshot_path("a", 20), bundle(20, record: record))

      assert kinds(result.faults) == [:snapshot_index_undecodable]
    end
  end

  # ------------------------------------------------------------- check/2

  describe "check/2 snapshots" do
    test "a shard with a well-formed newest bundle is clean", %{backend: backend} do
      put_chunk(backend, "a", [10, 20])
      put_snapshot(backend, "a", 20)

      report = Fsck.check(backend)

      assert report.clean?
      assert [snapshot] = report.snapshots
      assert snapshot.shard_tag == "a"
      assert snapshot.count == 1
      assert snapshot.durable_version == 20
      assert snapshot.chunk_max_version == 20
    end

    test "faults on a corrupt newest bundle", %{backend: backend} do
      put_chunk(backend, "a", [10, 20])
      :ok = ObjectStorage.put(backend, Keys.snapshot_path("a", 20), "not a bundle")

      report = Fsck.check(backend)

      refute report.clean?
      assert [fault] = Enum.filter(report.faults, &(&1.kind == :snapshot_index_record_invalid))
      assert fault.shard_tag == "a"
      assert fault.key == Keys.snapshot_path("a", 20)
    end

    test "only the newest bundle is checked: an older corrupt one is invisible", %{backend: backend} do
      # `Snapshot.read_latest/1` takes the first key the listing yields
      # and has no fallback to the next, so the newest bundle is the only
      # one a cold start will ever open.
      put_chunk(backend, "a", [10, 30])
      :ok = ObjectStorage.put(backend, Keys.snapshot_path("a", 10), "not a bundle")
      put_snapshot(backend, "a", 30)

      report = Fsck.check(backend)

      assert report.clean?
      assert [snapshot] = report.snapshots
      assert snapshot.count == 2
      assert snapshot.key == Keys.snapshot_path("a", 30)
    end

    test "faults on an unparsable name without fetching the object", %{backend: backend, root: root} do
      # A name that sorts ahead of every canonical one is what
      # `Snapshot.read_latest/1` picks up, and `Keys.extract_version/1`
      # then fails it — bricking every cold start of this shard.
      put_chunk(backend, "a", [10, 20])
      put_snapshot(backend, "a", 20)
      File.write!(Path.join([root, "s", "a", "!junk"]), "irrelevant")

      report = Fsck.check(backend)

      refute report.clean?
      assert [fault] = Enum.filter(report.faults, &(&1.kind == :unparsable_snapshot_key))
      assert fault.key == "s/a/!junk"
    end

    test "faults when the newest bundle cannot be read", %{backend: backend, root: root} do
      # A bundle the listing turned up that is gone by the time it is
      # opened is exactly what a cold start hits mid-incident.
      key = put_snapshot(backend, "a", 20)

      report = Fsck.check(ObjectStorage.backend(VanishingBackend, root: root))

      refute report.clean?
      assert [fault] = Enum.filter(report.faults, &(&1.kind == :snapshot_unreadable))
      assert fault.key == key
      assert fault.detail == %{reason: :not_found}
    end

    test "check_snapshots: false declines the whole pass", %{backend: backend} do
      put_chunk(backend, "a", [10, 20])
      :ok = ObjectStorage.put(backend, Keys.snapshot_path("a", 20), "not a bundle")

      report = Fsck.check(backend, check_snapshots: false)

      assert report.clean?
      assert report.snapshots == []
      assert Enum.filter(report.notes, &(&1.kind == :shard_without_snapshot)) == []
    end

    test "a shard filter scopes the snapshot pass", %{backend: backend} do
      put_chunk(backend, "a", [10, 20])
      :ok = ObjectStorage.put(backend, Keys.snapshot_path("b", 20), "not a bundle")

      report = Fsck.check(backend, shards: ["a"])

      assert report.clean?
    end
  end

  describe "check/2 snapshot and chunk continuity" do
    test "a snapshot ahead of the shard's chunks is a note, not a fault", %{backend: backend} do
      # Olivine clamps its durable version to the KNOWN-COMMITTED
      # version, not to the demux's last confirmed cut, so a materializer
      # routinely persists a snapshot for versions the ShardServer still
      # holds in its buffer. Ordinary operation, not a defect.
      put_chunk(backend, "a", [10, 20])
      put_snapshot(backend, "a", 40)

      report = Fsck.check(backend)

      assert report.clean?
      assert [note] = Enum.filter(report.notes, &(&1.kind == :snapshot_ahead_of_chunks))
      assert note.shard_tag == "a"
      assert note.detail == %{durable_version: 40, chunk_max_version: 20}
    end

    test "a snapshot at or below the chunk maximum raises no note", %{backend: backend} do
      put_chunk(backend, "a", [10, 20, 30])
      put_snapshot(backend, "a", 20)

      report = Fsck.check(backend)

      assert report.clean?
      assert Enum.filter(report.notes, &(&1.kind == :snapshot_ahead_of_chunks)) == []
    end

    test "a snapshot under a tag with no chunks at all is a note", %{backend: backend} do
      put_snapshot(backend, "a", 20)

      report = Fsck.check(backend)

      assert report.clean?
      assert [note] = Enum.filter(report.notes, &(&1.kind == :snapshot_ahead_of_chunks))
      assert note.detail == %{durable_version: 20, chunk_max_version: nil}
    end

    test "a shard with chunks and no snapshot is a note, not a fault", %{backend: backend} do
      put_chunk(backend, "a", [10, 20])

      report = Fsck.check(backend)

      assert report.clean?
      assert [note] = Enum.filter(report.notes, &(&1.kind == :shard_without_snapshot))
      assert note.shard_tag == "a"
    end

    test "a shard whose newest bundle will not decode raises no continuity note", %{backend: backend} do
      # The durable version could not be recovered, so there is nothing
      # to compare against the chunks; the framing fault says it all.
      put_chunk(backend, "a", [10, 20])
      :ok = ObjectStorage.put(backend, Keys.snapshot_path("a", 20), "not a bundle")

      report = Fsck.check(backend)

      assert Enum.filter(report.notes, &(&1.kind == :snapshot_ahead_of_chunks)) == []
    end
  end
end
