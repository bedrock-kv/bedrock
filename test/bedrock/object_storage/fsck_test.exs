defmodule Bedrock.ObjectStorage.FsckTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Transaction
  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.Chunk
  alias Bedrock.ObjectStorage.Fsck
  alias Bedrock.ObjectStorage.Keys
  alias Bedrock.ObjectStorage.LocalFilesystem

  @header_size 32

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

  # Replaces `size` bytes at `offset` with `replacement`.
  defp splice(binary, offset, size, replacement) do
    <<head::binary-size(offset), _::binary-size(size), tail::binary>> = binary
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

      assert Fsck.check(backend, shards: ["a"]).notes == []
      assert [_] = Enum.filter(Fsck.check(backend, shards: ["b"]).notes, &(&1.kind == :scratch_debris))
    end

    test "a store with no debris reports none", %{backend: backend} do
      put_chunk(backend, "a", [10, 20])

      assert Enum.filter(Fsck.check(backend).notes, &(&1.kind == :scratch_debris)) == []
    end
  end

  describe "check/2 snapshots" do
    test "snapshot bundles are out of scope and are not read", %{backend: backend, root: root} do
      put_chunk(backend, "a", [10, 20])
      File.mkdir_p!(Path.join([root, "s", "a"]))
      File.write!(Path.join(root, Keys.snapshot_path("a", 20)), "not a chunk")

      report = Fsck.check(backend)

      assert report.clean?
      assert report.chunk_count == 1
    end
  end
end
