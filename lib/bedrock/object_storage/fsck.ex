defmodule Bedrock.ObjectStorage.Fsck do
  @moduledoc """
  Offline structural validation of the transaction chunks in object storage.

  Reads a store directly — no running cluster, no cluster-reported state —
  and decides for itself whether what is persisted is coherent. `mix
  bedrock.fsck` is a thin shell over this module; callers that want the
  answer rather than the printout (a chaos harness, a test) should call
  `check/2` and read the returned `Report`.

  ## What it can prove

  Object storage has no manifest and no index. The namespace is flat and
  self-describing by listing alone, so the only referential integrity
  available is the one the store can recover for itself: the shard layout,
  replayed out of the system shard's own chunks (see "Recovering the
  layout" below). Everything else is structural, per chunk, and then per
  shard across the chunks a listing turns up.

  Per chunk (`check_chunk/3`):

  - the key is `c/{shard}/{inverted_version_base36}`, canonically encoded,
    and the version it names equals the header's `max_version` — the writer
    names a chunk for its last commit, and `ChunkReader` decides which
    chunks a read needs from names alone;
  - the header is internally consistent: magic, format version,
    `min_version <= max_version`, `directory_size == txn_count * 16`, and
    min/max agreeing with the first and last directory entries;
  - the directory ascends by version and its extents fit inside the data
    section that follows;
  - every slice decodes as a transaction whose commit version is the one
    its directory entry claims.

  Per shard:

  - no two chunks may cover the same version. This is the highest-value
    check here. `ChunkReader` selects the chunks a read needs by taking the
    leading run of the (newest-first) listing whose named max is at or above
    the target, then replays them oldest-first. Overlapping ranges make that
    replay emit the same version twice, out of order, with no error
    anywhere.

  Per snapshot bundle (`check_snapshot/2`), for the NEWEST bundle under
  each `s/{shard}/` prefix — the only one a cold start will ever open,
  since `Snapshot.read_latest/1` takes the first key its listing yields
  and has no fallback to the next:

  - the key is `s/{shard}/{inverted_version_base36}`, canonically
    encoded. One object under the prefix whose name will not parse and
    that sorts ahead of the real ones is enough to fail every cold start
    of that shard: `read_latest/1` hands it to `Keys.extract_version/1`
    and returns the error, and `maybe_load_snapshot/2` turns anything
    other than `:not_found` into a hard failure rather than degrading to
    a replay from the chunks;
  - the bundle ends in an index record `find_index_boundary/1` accepts —
    the same call `SnapshotBundle.split_in_place/3` makes on the restore
    path, so a bundle that fails it is a bundle no shard can be restored
    from;
  - the record's header and footer agree about `payload_size`.
    `find_index_boundary/1` sizes the record from the footer alone, so a
    disagreement restores without complaint and then loses the shard
    silently: `IndexDatabase` matches the two against each other, falls
    through to `Version.zero()`, finds no page block for version zero,
    and the shard comes up EMPTY;
  - the version the record carries is the version the key names. They are
    written from one value, and the NAME is what decides which bundle is
    newest and what `SnapshotRetention` deletes;
  - the record is a compaction base, not a delta. A compacted record
    points at itself (`IndexDatabase.build_snapshot_record/2`), which
    terminates the page chain. A record from a live append chain points
    at an older record that is not in the bundle, and restoring from it
    yields whatever pages that one delta held. `Snapshot.write/3` is
    put-if-not-exists, so such a bundle is permanent.

  ## Recovering the layout

  A slice records no shard tag of its own (`Demux.MutationSlicer` encodes
  mutations and a commit version, nothing else), so which keys a chunk is
  *entitled* to hold is decided entirely by the tag in its path. The
  authority for that is the `\\xFF/system/shard_keys/` family — and that
  family is not in object storage as a map, it is materialized from the
  system shard's own chunks. So the check bootstraps: replay `c/0/`,
  fold the `shard_keys/` set/clear mutations, hand the surviving entries
  to `SystemKeys.Reader.shard_layout_from_entries/1`, and validate every
  shard against the map that replay just recovered.

  Two things follow from where the map comes from.

  **The replay is only trusted when the system shard is structurally
  clean.** Every fault above is a reason a replay silently returns the
  wrong mutations — a chunk filed under the wrong version is skipped, an
  unordered directory replays out of order, overlapping chunks replay
  twice. So `check/2` checks `c/0/` first and, on any fault there,
  abandons layout recovery entirely rather than deriving confident
  nonsense from a broken map. The result is `status: :unavailable` with a
  reason, a note, and no layout-derived faults at all.

  **The replayed history, not just its last state, is the containment
  domain.** See below.

  ## What it cannot prove

  **A gap between chunks is not evidence of anything.** Versions are commit
  versions from the sequencer, not a dense sequence, and a shard only
  receives the transactions that touch it — so consecutive chunks routinely
  leave a hole in version space. Nothing in the store records what the
  version stream *should* have been, and chunks carry no link to their
  predecessor, so a lost chunk is indistinguishable from a quiet shard.
  Gaps are therefore reported (in `ShardResult.gaps` and as `:version_gap`
  notes) and never faulted. A caller that *does* know the expected stream —
  a chaos harness driving a known workload — can make the call this module
  cannot.

  **A tag with no chunks, or chunks under a tag the layout does not name,
  are not faults.** Both have ordinary explanations and object storage
  cannot tell them from the alarming ones. Chunks appear only after a
  flush, so a shard that was just recruited, or that has taken no writes,
  legitimately has no `c/<tag>/` prefix yet — and the same lag runs the
  other way, since the `shard_keys/` mutation naming a new shard reaches
  `c/0/` on the system shard's flush schedule, not the new shard's. In the
  other direction, chunks are never deleted, so a tag the layout has
  stopped naming keeps its chunks forever. Both are `:shard_not_in_layout`
  and `:layout_shard_without_chunks` notes.

  **Containment is checked against every range the tag has ever held, not
  against its current one.** A chunk written before a boundary moved holds
  the keys the *then*-current layout routed to it, and it is still a
  correct chunk; faulting it against today's map would report the normal
  consequence of resharding as corruption. So the replay accumulates the
  ranges from every `shard_keys/` entry it sees written, and a mutation is
  contained if it falls in any of them. Today that set has one member per
  tag — boundaries are written once at bootstrap and nothing moves them —
  so the check is exactly as tight as a current-layout check would be,
  and it stays sound when split and merge land. What it gives up is
  catching a misroute *into a range the tag used to own*, which needs the
  version-indexed layout history and is not worth its complexity while
  there is no resharding to produce one.

  Two kinds of key are exempt, because they are outside every shard's
  range by design rather than by accident:

  - anything at or above `Bedrock.end_of_keyspace/0`. The commit proxy
    privatizes a membership clear by prefixing it past every boundary and
    addressing it to an explicit tag
    (`CommitProxy.Finalization.privatized_mutations/1`, FDB's
    `ApplyMetadataMutation.cpp` `withPrefix(systemKeys.begin)`), precisely
    so no materializer can store it. It rides the shard's stream and lands
    in the shard's chunks all the same;
  - a degenerate `clear_range` whose start is not below its end, which
    names no keys.

  A `clear_range` is checked whole: the proxy clamps each routed copy to
  the owning shard's bounds, so a range that straddles a boundary in the
  persisted slice was never clamped.

  **A legacy `shard_keys/` family suspends containment.** Values written
  before `SystemKeys.Values` carry a tag and no start key, and
  `shard_layout_from_entries/1` reconstructs the missing starts by
  adjacency over the final state alone. That gives a usable current map —
  enough to check coverage — but no history, so containment would be
  measuring against a fabrication. It is skipped, with a
  `:containment_undecidable` note.

  **A snapshot ahead of its shard's chunks is not evidence of anything.**
  Olivine clamps its durable version to the KNOWN-COMMITTED version
  (`Logic.advance_window/1`), which the commit proxies report — not to the
  demux's last confirmed cut, which is what decides when a chunk is
  written. The two lag independently, so a materializer routinely persists
  a snapshot covering versions the ShardServer still holds in its buffer,
  and a shard with a snapshot and no chunks at all is an ordinary young
  one. It is a `:snapshot_ahead_of_chunks` note, never a fault.

  **Nor is a snapshot whose forward chunks appear to be missing.** The
  question this pass was meant to answer — "could a cold start actually
  catch up from here?" — turns out not to be decidable against today's
  store, in either of its two halves. Chunks are never deleted, so
  coverage above a snapshot cannot be lost to pruning; a chunk that is
  absent was either never written (the lag above) or lost, and a lost
  chunk is indistinguishable from a quiet shard for exactly the reason
  version gaps are. And when chunks above the snapshot DO exist, forward
  coverage follows from the checks above rather than needing one of its
  own: `ChunkReader.read_from_version/3` takes the whole leading run of
  the listing whose named max is at or above the target, so every
  transaction newer than the snapshot is selected as long as the chunk
  names are canonical and the ranges do not overlap. The relationship is
  reported in `SnapshotResult.chunk_max_version` so a caller that knows
  the expected stream can judge it; this becomes faultable here the day
  chunk reclamation lands (bedrock-wxf.6.11) and gives the store a replay
  floor to measure against.

  **The cluster's own durability claim cannot be checked at all.** A
  shard's durable version lives in RAM in `ShardServer` — rebuilt from
  confirmed cuts, aggregated by `Demux.Durability.min_durable_version/1`,
  and never written to object storage. So the highest version fsck can
  derive for a shard is the highest its chunks HOLD, which is a lower
  bound on nothing the cluster promised: it says what was persisted, not
  what was acknowledged. A green run means the bytes in the store are
  coherent with each other. It does not mean the cluster kept a
  commit it confirmed, and nobody should read it that way.

  **There are no checksums.** The chunk format carries none, so integrity
  here rests on magic bytes plus structural completeness: bit rot inside a
  transaction payload is caught only to the extent the transaction's own
  section CRCs catch it. Snapshot bundles carry none either, and the index
  record's payload is `term_to_binary` output, so a flipped bit inside it
  is caught only when it makes the term undecodable.

  ## What is legal

  Superseded and unreferenced chunks are not faults. Chunks are never
  deleted by design, so a shard that has snapshotted far past its oldest
  chunks still has all of them sitting there.

  A shard with chunks and no snapshot at all is legal, and today it is the
  ordinary case: nothing drives Olivine compaction (bedrock-947), so a
  running cluster writes a bundle only on an idle spin-down. Such a shard
  cold starts by replaying its whole history, which is slow and correct.
  It is a `:shard_without_snapshot` note.

  Superseded bundles are legal too, and so is a corrupt one that is not
  the newest — nothing will ever open it. Only the newest is checked.

  `.bedrock-tmp.*` scratch files left behind by a killed writer are
  reported as `:scratch_debris` notes and never faulted; reclaiming them is
  separate work. They are hidden from `ObjectStorage.list/3`, so they can
  only be seen by walking a `LocalFilesystem` root directly — against any
  other backend the scan is empty rather than wrong.

  ## Example

      backend = ObjectStorage.backend(LocalFilesystem, root: "/var/lib/bedrock/objects")
      report = Fsck.check(backend)

      report.clean?
      #=> false

      Enum.map(report.faults, & &1.kind)
      #=> [:chunk_range_overlap]
  """

  alias Bedrock.ControlPlane.Config.RecoveryAttempt
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.Chunk
  alias Bedrock.ObjectStorage.ChunkReader
  alias Bedrock.ObjectStorage.Keys
  alias Bedrock.ObjectStorage.LocalFilesystem
  alias Bedrock.ObjectStorage.SnapshotBundle
  alias Bedrock.SystemKeys
  alias Bedrock.SystemKeys.Reader, as: SystemKeysReader
  alias Bedrock.SystemKeys.Values

  @max_uint64 0xFFFFFFFFFFFFFFFF
  @scratch_prefix ".bedrock-tmp."
  # `IndexDatabase`'s record framing, inlined the way `SnapshotBundle`
  # inlines it: a 16-byte header (magic, an 8-byte version, payload_size)
  # and a 4-byte payload_size footer around the payload.
  @index_header_size 16
  @index_footer_size 4
  # `Bedrock.end_of_keyspace/0`, inlined the way the materializer inlines
  # it: the exclusive top of every shard's range, and the floor of the
  # privatized-notice space above it.
  @end_of_keyspace <<0xFF, 0xFF>>

  defmodule Fault do
    @moduledoc """
    A structural violation: something that is, or will become, a read error
    or a silent replay defect.
    """

    @type t :: %__MODULE__{
            kind: atom(),
            shard_tag: String.t() | nil,
            key: String.t() | nil,
            detail: map()
          }

    defstruct [:kind, :shard_tag, :key, detail: %{}]
  end

  defmodule Note do
    @moduledoc """
    An observation worth surfacing that is not a violation — scratch debris,
    or a version gap this module cannot classify.
    """

    @type t :: %__MODULE__{
            kind: atom(),
            shard_tag: String.t() | nil,
            key: String.t() | nil,
            detail: map()
          }

    defstruct [:kind, :shard_tag, :key, detail: %{}]
  end

  defmodule ChunkResult do
    @moduledoc """
    The verdict on one chunk.

    `range` is the `{min, max}` the chunk's DIRECTORY describes, and is
    `nil` when the directory could not be trusted to describe anything — a
    chunk with a `nil` range is excluded from its shard's range analysis,
    which is then reported as `:partial`.
    """

    @type t :: %__MODULE__{
            key: String.t(),
            shard_tag: String.t() | nil,
            range: {non_neg_integer(), non_neg_integer()} | nil,
            txn_count: non_neg_integer() | nil,
            bytes: non_neg_integer(),
            faults: [Fault.t()]
          }

    defstruct [:key, :shard_tag, :range, :txn_count, :bytes, faults: []]
  end

  defmodule ShardResult do
    @moduledoc "The verdict on one shard's chunks, taken together."

    @type gap :: %{
            after_key: String.t(),
            after_version: non_neg_integer(),
            before_key: String.t(),
            before_version: non_neg_integer()
          }

    @type t :: %__MODULE__{
            shard_tag: String.t(),
            range: {non_neg_integer(), non_neg_integer()} | nil,
            range_analysis: :complete | :partial,
            chunks: [ChunkResult.t()],
            faults: [Fault.t()],
            gaps: [gap()]
          }

    defstruct [:shard_tag, :range, :range_analysis, chunks: [], faults: [], gaps: []]
  end

  defmodule LayoutResult do
    @moduledoc """
    The shard layout recovered by replaying the system shard's chunks, and
    the verdict on it.

    `status` is `:recovered` only when the system shard was structurally
    clean, its `shard_keys/` family decoded, and it named at least one
    shard. Otherwise it is `:unavailable` and `reason` says which of those
    failed — every layout-derived check is then skipped rather than run
    against a map nobody should trust.

    `shards` is the recovered layout in ascending `start_key` order.
    `ranges_by_tag` is the containment domain: for each shard tag, as its
    object-storage path spells it, every `{start_key, end_key}` that tag
    has held across the replayed history — which is what a chunk's
    contents are actually judged against, not `shards`.

    `containment` is `:undecidable` when the family carries legacy
    values, whose start keys are reconstructed rather than recorded.
    """

    @type status :: :recovered | :unavailable
    @type shard :: %{tag: term(), start_key: Bedrock.key(), end_key: Bedrock.key()}

    @type t :: %__MODULE__{
            status: status(),
            reason: atom() | nil,
            shards: [shard()],
            ranges_by_tag: %{String.t() => [{Bedrock.key(), Bedrock.key()}]},
            containment: :decidable | :undecidable,
            faults: [Fault.t()]
          }

    defstruct [:status, :reason, shards: [], ranges_by_tag: %{}, containment: :decidable, faults: []]
  end

  defmodule SnapshotResult do
    @moduledoc """
    The verdict on one shard's snapshot prefix.

    Only the NEWEST bundle is opened, because it is the only one a cold
    start will ever open: `Snapshot.read_latest/1` takes the first key the
    listing yields and has no fallback to the next. `key`, `version`,
    `durable_version` and `bytes` all describe that one bundle; `count` is
    how many objects the prefix holds.

    `version` is the version the KEY names and `durable_version` the one
    the index record carries. They are written from the same value and
    should agree; `durable_version` is `nil` when the record could not be
    trusted to carry one.

    `chunk_max_version` is the highest version this shard's chunks hold,
    carried here so the two can be compared — see "What it cannot prove" in
    the module doc for why that comparison is a note and not a fault.
    """

    @type t :: %__MODULE__{
            shard_tag: String.t() | nil,
            key: String.t(),
            count: non_neg_integer(),
            version: non_neg_integer() | nil,
            durable_version: non_neg_integer() | nil,
            chunk_max_version: non_neg_integer() | nil,
            bytes: non_neg_integer(),
            faults: [Fault.t()]
          }

    defstruct [:shard_tag, :key, :version, :durable_version, :chunk_max_version, count: 0, bytes: 0, faults: []]
  end

  defmodule Report do
    @moduledoc """
    The verdict on a store. `clean?` is true exactly when `faults` is empty;
    notes never make a store unclean.

    `layout` is `nil` when layout recovery was not attempted (`check_layout:
    false`); otherwise it is a `LayoutResult`, which may still be
    `:unavailable`.

    `snapshots` holds one `SnapshotResult` per shard tag that has a `s/`
    prefix, and is empty when the snapshot pass was declined
    (`check_snapshots: false`).
    """

    @type t :: %__MODULE__{
            clean?: boolean(),
            chunk_count: non_neg_integer(),
            shards: [ShardResult.t()],
            snapshots: [SnapshotResult.t()],
            layout: LayoutResult.t() | nil,
            faults: [Fault.t()],
            notes: [Note.t()]
          }

    defstruct [:clean?, :chunk_count, :layout, shards: [], snapshots: [], faults: [], notes: []]
  end

  @doc """
  Checks every transaction chunk in a store.

  ## Options

  - `:shards` - only check these shard tags (default: every shard the
    listing turns up). Layout recovery always reads the system shard, so
    containment survives a filter; the tag/prefix correspondence notes do
    not, and are skipped, because a filtered listing cannot tell a shard
    that has no chunks from one that was not looked at.
  - `:check_transactions` - decode each transaction slice and compare its
    commit version against its directory entry (default: `true`). Key
    containment needs the same decode and is skipped with it.
  - `:check_layout` - replay the system shard to recover the shard layout,
    and check coverage, containment and tag correspondence against it
    (default: `true`)
  - `:check_snapshots` - open the newest snapshot bundle under each `s/`
    prefix and validate its index record (default: `true`). This is the
    one pass that fetches an object whose size is not bounded by a chunk:
    `ObjectStorage.get/2` has no ranged read, so reading the record at the
    tail means downloading the whole bundle, which is the shard's entire
    materialized state. Decline it on a large store where the chunk
    structure is the question.

  Raises `ObjectStorage.ListError` if the store cannot be listed: a short
  listing would report chunks as absent without having looked for them,
  which is the exact failure this tool exists to find.
  """
  @spec check(ObjectStorage.backend(), keyword()) :: Report.t()
  def check(backend, opts \\ []) do
    shards = Keyword.get(opts, :shards)
    layout = recover_layout(backend, opts)

    shard_results =
      backend
      |> chunk_keys(shards)
      |> Enum.map(&check_stored_chunk(backend, &1, chunk_opts(opts, layout)))
      |> Enum.group_by(& &1.shard_tag)
      |> Enum.sort_by(fn {shard_tag, _} -> shard_tag end)
      |> Enum.map(fn {shard_tag, chunks} -> check_shard(shard_tag, chunks) end)

    snapshot_results = check_snapshots(backend, shard_results, opts)

    faults =
      layout_faults(layout) ++
        Enum.flat_map(shard_results, &shard_faults/1) ++ Enum.flat_map(snapshot_results, & &1.faults)

    notes =
      debris_notes(backend, shards) ++
        Enum.flat_map(shard_results, &gap_notes/1) ++
        layout_notes(layout) ++
        correspondence_notes(layout, shard_results, shards) ++
        snapshot_notes(snapshot_results, shard_results, opts)

    %Report{
      clean?: faults == [],
      chunk_count: shard_results |> Enum.map(&length(&1.chunks)) |> Enum.sum(),
      shards: shard_results,
      snapshots: snapshot_results,
      layout: layout,
      faults: faults,
      notes: notes
    }
  end

  @doc """
  Checks one chunk: its key against its contents, and its contents against
  themselves.

  Takes the key and the raw object rather than a backend, so a caller can
  check a chunk it already holds — and so every fault below is reachable
  from a handwritten fixture.

  ## Options

  - `:check_transactions` - decode each transaction slice (default: `true`)
  - `:layout_ranges` - `%{shard_tag => [{start_key, end_key}]}`, the ranges
    each tag is entitled to hold. A tag absent from the map is not checked;
    so is a `nil` map, which is what `check/2` passes when no layout could
    be recovered.
  """
  @spec check_chunk(String.t(), binary(), keyword()) :: ChunkResult.t()
  def check_chunk(key, binary, opts \\ []) do
    shard_tag = shard_tag_from_key(key)
    {key_faults, key_version} = check_key(key)
    {body_faults, range, txn_count} = check_body(binary, key_version, containment_opts(opts, shard_tag))

    %ChunkResult{
      key: key,
      shard_tag: shard_tag,
      range: range,
      txn_count: txn_count,
      bytes: byte_size(binary),
      faults: Enum.map(key_faults ++ body_faults, &%{&1 | key: key, shard_tag: shard_tag})
    }
  end

  @doc """
  Checks one snapshot bundle: its key against its contents, and the index
  record that terminates it against itself.

  Takes the key and the raw object rather than a backend, for the same
  reason `check_chunk/3` does — so every fault below is reachable from a
  handwritten fixture. `count` is 1: the caller holds one bundle, not a
  prefix.
  """
  @spec check_snapshot(String.t(), binary()) :: SnapshotResult.t()
  def check_snapshot(key, binary) do
    shard_tag = snapshot_tag_from_key(key)
    {key_faults, key_version} = check_snapshot_key(key)
    {record_faults, durable_version} = check_index_record(binary, key_version)

    %SnapshotResult{
      key: key,
      shard_tag: shard_tag,
      count: 1,
      version: key_version,
      durable_version: durable_version,
      bytes: byte_size(binary),
      faults: Enum.map(key_faults ++ record_faults, &%{&1 | key: key, shard_tag: shard_tag})
    }
  end

  # ------------------------------------------------------------------ store

  defp chunk_keys(backend, nil), do: ObjectStorage.list(backend, "c/")

  defp chunk_keys(backend, shards) do
    Enum.flat_map(shards, &Enum.to_list(ObjectStorage.list(backend, Keys.chunks_prefix(&1))))
  end

  defp check_stored_chunk(backend, key, opts) do
    case ObjectStorage.get(backend, key) do
      {:ok, binary} ->
        check_chunk(key, binary, opts)

      {:error, reason} ->
        shard_tag = shard_tag_from_key(key)

        %ChunkResult{
          key: key,
          shard_tag: shard_tag,
          bytes: 0,
          faults: [%Fault{kind: :unreadable, key: key, shard_tag: shard_tag, detail: %{reason: reason}}]
        }
    end
  end

  # -------------------------------------------------------------------- key

  # A chunk key is `c/{shard}/{inverted_version_base36}`. The basename must
  # be the CANONICAL encoding of its version, not merely something base36
  # can parse: `Keys.parse_inverted_version/1` accepts uppercase and any
  # width, and `Keys.key_to_version/1` raises rather than erroring on the
  # 13-character strings that overflow a uint64 — so neither is safe to
  # point at a name that arrived from a filesystem.
  defp check_key(key) do
    case Path.split(key) do
      ["c", _shard_tag, basename] -> check_basename(basename, :unparsable_key)
      _ -> {[fault(:malformed_chunk_key, %{expected: "c/{shard}/{version}"})], nil}
    end
  end

  # Shared with the snapshot namespace, which names its objects the same
  # way and reads them back through the same `Keys.extract_version/1`.
  defp check_basename(basename, kind) do
    with {:ok, inverted} <- Keys.parse_inverted_version(basename),
         true <- inverted <= @max_uint64,
         ^basename <- Keys.format_inverted_version(inverted) do
      {[], Keys.restore_version(inverted)}
    else
      _ -> {[fault(kind, %{basename: basename})], nil}
    end
  end

  defp shard_tag_from_key(key) do
    case Path.split(key) do
      ["c", shard_tag | _] -> shard_tag
      _ -> nil
    end
  end

  # ------------------------------------------------------------------ chunk

  # Returns {faults, range, txn_count}. Stops descending whenever the next
  # layer would have to be interpreted through a field already known to be
  # wrong: a truncated chunk should report that it is truncated, not a
  # cascade of derived nonsense.
  defp check_body(binary, key_version, opts) do
    header_size = Chunk.header_size()

    case binary do
      <<header_binary::binary-size(^header_size), rest::binary>> ->
        decode_header(header_binary, rest, key_version, opts)

      _ ->
        {[fault(:truncated_header, %{bytes: byte_size(binary), required: header_size})], nil, nil}
    end
  end

  defp decode_header(header_binary, rest, key_version, opts) do
    case Chunk.decode_header(header_binary) do
      {:ok, header} ->
        check_header(header, rest, key_version, opts)

      {:error, {:invalid_magic, magic}} ->
        {[fault(:bad_magic, %{magic: magic, expected: Chunk.magic_number()})], nil, nil}

      # Precluded today: exactly 32 bytes always match one of
      # `decode_header/1`'s first two clauses, so there is no third answer
      # to receive. Kept so a widened header format surfaces here as a
      # fault rather than as a CaseClauseError in an operator's fsck.
      {:error, reason} ->
        {[fault(:malformed_header, %{reason: reason})], nil, nil}
    end
  end

  defp check_header(header, rest, key_version, opts) do
    if header.format_version == Chunk.format_version() do
      check_supported_header(header, rest, key_version, opts)
    else
      detail = %{format_version: header.format_version, expected: Chunk.format_version()}
      {[fault(:unsupported_format_version, detail)], nil, nil}
    end
  end

  defp check_supported_header(header, rest, key_version, opts) do
    faults =
      key_version_faults(header, key_version) ++
        range_faults(header) ++ shape_faults(header)

    if Enum.any?(faults, &(&1.kind in [:empty_directory, :directory_size_mismatch])) do
      {faults, nil, header.txn_count}
    else
      {directory_faults, range} = check_directory(header, rest, opts)
      {faults ++ directory_faults, range, header.txn_count}
    end
  end

  defp key_version_faults(_header, nil), do: []
  defp key_version_faults(%{max_version: max}, max), do: []

  # The name is the whole selection predicate for a read that has not
  # fetched the chunk: `ChunkReader` stops taking chunks at the first name
  # whose version is below its target. A chunk filed under a version lower
  # than it holds is skipped even though it holds what the reader wants —
  # a silent replay gap with nothing to raise on.
  defp key_version_faults(%{max_version: max}, key_version) do
    [fault(:key_version_mismatch, %{key_version: key_version, header_max_version: max})]
  end

  defp range_faults(%{min_version: min, max_version: max}) when min > max do
    [fault(:version_range_inverted, %{min_version: min, max_version: max})]
  end

  defp range_faults(_header), do: []

  defp shape_faults(%{txn_count: 0}), do: [fault(:empty_directory, %{})]

  defp shape_faults(%{txn_count: count, directory_size: size}) do
    expected = count * Chunk.directory_entry_size()

    if size == expected do
      []
    else
      [fault(:directory_size_mismatch, %{directory_size: size, txn_count: count, expected: expected})]
    end
  end

  # -------------------------------------------------------------- directory

  defp check_directory(header, rest, opts) do
    directory_size = header.directory_size

    if byte_size(rest) < directory_size do
      {[fault(:truncated_directory, %{bytes: byte_size(rest), required: directory_size})], nil}
    else
      <<directory_binary::binary-size(^directory_size), data::binary>> = rest

      case Chunk.decode_directory(directory_binary, header.txn_count) do
        {:ok, directory} ->
          check_entries(header, directory, data, opts)

        # Precluded today: `directory_size == txn_count * 16` was already
        # checked, and the slice above is exactly that long, so there are
        # always enough bytes for the entries claimed. Kept for the same
        # reason as `:malformed_header`.
        {:error, reason} ->
          {[fault(:malformed_directory, %{reason: reason})], nil}
      end
    end
  end

  defp check_entries(header, directory, data, opts) do
    ordering = ordering_faults(directory)
    faults = ordering ++ bounds_faults(header, directory)

    # The directory only describes a range once it is known to ascend; an
    # unordered one has no first and last to speak of.
    range =
      if ordering == [] do
        {List.first(directory).version, List.last(directory).version}
      end

    case data_faults(directory, data) do
      [] -> {faults ++ transaction_faults(directory, data, opts), range}
      data_faults -> {faults ++ data_faults, range}
    end
  end

  defp ordering_faults(directory) do
    directory
    |> Enum.chunk_every(2, 1, :discard)
    |> Enum.with_index(1)
    |> Enum.flat_map(fn {[previous, entry], index} ->
      if entry.version > previous.version do
        []
      else
        [fault(:directory_not_ascending, %{index: index, version: entry.version, previous_version: previous.version})]
      end
    end)
  end

  defp bounds_faults(header, directory) do
    first = List.first(directory).version
    last = List.last(directory).version

    min_fault =
      if first == header.min_version,
        do: [],
        else: [fault(:header_min_mismatch, %{header_min_version: header.min_version, first_entry_version: first})]

    max_fault =
      if last == header.max_version,
        do: [],
        else: [fault(:header_max_mismatch, %{header_max_version: header.max_version, last_entry_version: last})]

    min_fault ++ max_fault
  end

  # The directory is a map of extents into the data section; a chunk whose
  # data stops short of them is torn. Nothing below may slice the data
  # until this holds — `binary_part/3` would raise instead of reporting.
  defp data_faults(directory, data) do
    required = Enum.reduce(directory, 0, fn entry, acc -> max(acc, entry.offset + entry.length) end)

    if byte_size(data) >= required do
      []
    else
      [fault(:data_section_truncated, %{required: required, actual: byte_size(data)})]
    end
  end

  # ----------------------------------------------------------- transactions

  defp transaction_faults(directory, data, opts) do
    if Keyword.get(opts, :check_transactions, true) do
      slices = Enum.map(directory, &{&1, binary_part(data, &1.offset, &1.length)})

      Enum.flat_map(slices, fn {entry, slice} -> transaction_fault(entry, slice) end) ++
        containment_faults(slices, Keyword.get(opts, :shard_ranges))
    else
      []
    end
  end

  defp transaction_fault(entry, slice) do
    with {:ok, _transaction} <- Transaction.decode(slice),
         {:ok, commit_version} <- Transaction.commit_version(slice) do
      commit_version_fault(entry, commit_version)
    else
      {:error, reason} -> [fault(:transaction_undecodable, %{version: entry.version, reason: reason})]
    end
  end

  defp commit_version_fault(entry, nil) do
    [fault(:transaction_missing_commit_version, %{entry_version: entry.version})]
  end

  defp commit_version_fault(entry, <<commit_version::unsigned-big-64>>) do
    if commit_version == entry.version do
      []
    else
      [fault(:transaction_version_mismatch, %{entry_version: entry.version, commit_version: commit_version})]
    end
  end

  # ------------------------------------------------------------ containment

  # Resolves the per-store `:layout_ranges` map down to the one tag's
  # ranges before descending, so nothing below this point has to know
  # which shard it is looking at.
  defp containment_opts(opts, shard_tag) do
    case Keyword.get(opts, :layout_ranges) do
      nil -> opts
      by_tag -> Keyword.put(opts, :shard_ranges, Map.get(by_tag, shard_tag))
    end
  end

  defp containment_faults(_slices, nil), do: []

  # One fault per chunk, not per mutation: a misrouted shard produces
  # offenders by the thousand, and the operator's question is answered by
  # the first one plus a count.
  defp containment_faults(slices, ranges) do
    slices
    |> Enum.flat_map(fn {entry, slice} -> escapees(entry.version, slice, ranges) end)
    |> case do
      [] ->
        []

      [{version, key_or_range} | _] = all ->
        detail = %{version: version, key: key_or_range, count: length(all), shard_ranges: ranges}
        [fault(:mutation_out_of_shard_range, detail)]
    end
  end

  # A slice whose mutations section will not stream has nothing to place;
  # `transaction_fault/2` has already faulted it if it is torn, and an
  # absent section is an empty transaction.
  defp escapees(version, slice, ranges) do
    case Transaction.mutations(slice) do
      {:ok, mutations} ->
        mutations
        |> Enum.reject(&contained?(&1, ranges))
        |> Enum.map(&{version, mutation_key(&1)})

      {:error, _reason} ->
        []
    end
  end

  # A range mutation is checked whole and must fit inside ONE range: the
  # proxy clamps each routed copy to the owning shard's bounds, so a
  # persisted range that straddles a boundary was never clamped. An empty
  # range names no keys.
  defp contained?({:clear_range, start_key, end_key}, ranges) do
    start_key >= end_key or
      Enum.any?(ranges, fn {range_start, range_end} -> range_start <= start_key and end_key <= range_end end)
  end

  # Anything past the end of the keyspace is a privatized notice:
  # deliberately outside every shard's range so no materializer can store
  # it, and addressed to a tag rather than routed by a boundary walk.
  defp contained?({:set, key, _value}, ranges), do: holds?(ranges, key)
  defp contained?({:clear, key}, ranges), do: holds?(ranges, key)
  defp contained?({:atomic, _op, key, _value}, ranges), do: holds?(ranges, key)

  # A mutation shape this module does not know is not evidence of a
  # misroute; whatever introduced it teaches this function about it.
  defp contained?(_mutation, _ranges), do: true

  defp holds?(_ranges, key) when key >= @end_of_keyspace, do: true
  defp holds?(ranges, key), do: Enum.any?(ranges, fn {start_key, end_key} -> start_key <= key and key < end_key end)

  defp mutation_key({:clear_range, start_key, end_key}), do: {start_key, end_key}
  defp mutation_key({:atomic, _op, key, _value}), do: key
  defp mutation_key(mutation), do: elem(mutation, 1)

  # ----------------------------------------------------------------- layout

  defp chunk_opts(opts, %LayoutResult{status: :recovered, containment: :decidable, ranges_by_tag: by_tag}),
    do: Keyword.put(opts, :layout_ranges, by_tag)

  defp chunk_opts(opts, _layout), do: opts

  @doc """
  Recovers the shard layout by replaying the system shard, without checking
  anything else.

  Exposed because the layout is the prerequisite for any offline work over
  the store's contents — `Bedrock.ObjectStorage.Replay` needs to know which
  prefixes to replay before it can replay them — and deriving it a second
  way would be deriving it a second, less trustworthy way.

  Returns `nil` when `check_layout: false`.
  """
  @spec recover_layout(ObjectStorage.backend(), keyword()) :: LayoutResult.t() | nil
  def recover_layout(backend, opts \\ []) do
    if Keyword.get(opts, :check_layout, true) do
      tag = system_shard_tag()

      case system_shard_health(backend, tag) do
        :ok -> replay_layout(backend, tag)
        {:unavailable, reason} -> %LayoutResult{status: :unavailable, reason: reason}
      end
    end
  end

  defp system_shard_tag, do: Keys.shard_tag(RecoveryAttempt.system_shard_id())

  # The system shard is read twice — once here for its verdict, once by
  # the replay — and a third time by the main pass. It is the metadata
  # shard, small by construction, and the alternative is trusting a
  # replay whose ordering nothing has checked. The slice decode is forced
  # on regardless of `:check_transactions`, for the same reason: this is
  # the one shard whose contents are about to be believed.
  defp system_shard_health(backend, tag) do
    case Enum.to_list(ObjectStorage.list(backend, Keys.chunks_prefix(tag))) do
      [] ->
        {:unavailable, :no_system_shard_chunks}

      keys ->
        chunks = Enum.map(keys, &check_stored_chunk(backend, &1, check_transactions: true))

        if tag |> check_shard(chunks) |> shard_faults() == [],
          do: :ok,
          else: {:unavailable, :system_shard_unhealthy}
    end
  end

  defp replay_layout(backend, tag) do
    {entries, history, legacy?} = replay_shard_keys(backend, tag)

    if entries == %{} do
      %LayoutResult{status: :unavailable, reason: :no_shard_keys_entries}
    else
      decode_layout(entries, history, legacy?, tag)
    end
  end

  defp decode_layout(entries, history, legacy?, tag) do
    case SystemKeysReader.shard_layout_from_entries(Map.to_list(entries)) do
      {:ok, layout} ->
        recovered_layout(layout, history, legacy?)

      {:error, {:invalid_shard_value, key}} ->
        fault = %Fault{kind: :layout_undecodable_entry, shard_tag: tag, key: key, detail: %{}}
        %LayoutResult{status: :unavailable, reason: :undecodable_entry, faults: [fault]}
    end
  end

  # Folds the `shard_keys/` family out of the system shard's own stream,
  # oldest first, exactly as `RoutingData.apply_mutations/2` would. What
  # survives is the family's final state; what accumulates alongside it is
  # every range any entry ever named, which is the containment domain.
  defp replay_shard_keys(backend, tag) do
    backend
    |> ChunkReader.new(tag)
    |> ChunkReader.read_all_transactions()
    |> Enum.reduce({%{}, MapSet.new(), false}, fn {_version, slice}, acc ->
      case Transaction.mutations(slice) do
        {:ok, mutations} -> Enum.reduce(mutations, acc, &apply_shard_key_mutation/2)
        {:error, _reason} -> acc
      end
    end)
  end

  defp apply_shard_key_mutation({:set, key, value}, {entries, history, legacy?} = acc) do
    case SystemKeys.parse_key(key) do
      {:shard_key, end_key} ->
        case Values.decode_shard_key_entry(value) do
          {:ok, {tag, start_key}} ->
            {Map.put(entries, key, value), MapSet.put(history, {tag, start_key, end_key}), legacy?}

          {:error, _reason} ->
            {Map.put(entries, key, value), history, true}
        end

      _not_a_shard_key ->
        acc
    end
  end

  # `entries` holds nothing but shard keys, so a clear that names anything
  # else is already a no-op and needs no family test of its own.
  defp apply_shard_key_mutation({:clear, key}, {entries, history, legacy?}),
    do: {Map.delete(entries, key), history, legacy?}

  defp apply_shard_key_mutation({:clear_range, start_key, end_key}, {entries, history, legacy?}) do
    kept = Map.reject(entries, fn {key, _value} -> key >= start_key and key < end_key end)
    {kept, history, legacy?}
  end

  defp apply_shard_key_mutation(_mutation, acc), do: acc

  defp recovered_layout(layout, history, legacy?) do
    shards =
      layout
      |> Enum.map(fn {end_key, {tag, start_key}} -> %{tag: tag, start_key: start_key, end_key: end_key} end)
      |> Enum.sort_by(& &1.start_key)

    %LayoutResult{
      status: :recovered,
      shards: shards,
      ranges_by_tag: ranges_by_tag(history, shards),
      containment: if(legacy?, do: :undecidable, else: :decidable),
      faults: coverage_faults(shards)
    }
  end

  # The current layout is folded in as well as the history: a store whose
  # oldest system chunk postdates the bootstrap write has entries nobody
  # saw written, and they are no less legitimate for it.
  defp ranges_by_tag(history, shards) do
    shards
    |> Enum.reduce(history, fn shard, acc -> MapSet.put(acc, {shard.tag, shard.start_key, shard.end_key}) end)
    |> Enum.flat_map(fn {tag, start_key, end_key} ->
      case shard_prefix(tag) do
        nil -> []
        prefix -> [{prefix, {start_key, end_key}}]
      end
    end)
    |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))
  end

  # A tag that is not a shard id names no object-storage prefix, so there
  # is no chunk it could be compared against. `decode_shard_key_entry/1`
  # only guarantees an integer.
  defp shard_prefix(tag) when is_integer(tag) and tag >= 0, do: Keys.shard_tag(tag)
  defp shard_prefix(_tag), do: nil

  # Walks the layout in ascending start order against the highest end seen
  # so far, from the start of the keyspace through its end. Both a hole
  # and a double claim leave a key that routing cannot resolve to exactly
  # one owner, and the family is committed transactionally, so neither is
  # a state the cluster passes through on its way to a good one.
  defp coverage_faults(shards) do
    {faults, covered} =
      Enum.reduce(shards, {[], <<>>}, fn shard, {faults, covered} ->
        {[coverage_fault(covered, shard) | faults], max(covered, shard.end_key)}
      end)

    [trailing_gap(covered) | faults] |> Enum.reject(&is_nil/1) |> Enum.reverse()
  end

  defp coverage_fault(covered, %{start_key: start_key}) when start_key > covered,
    do: fault(:layout_gap, %{after_key: covered, before_key: start_key})

  defp coverage_fault(covered, %{start_key: start_key, end_key: end_key}) when start_key < covered,
    do: fault(:layout_overlap, %{covered_through: covered, start_key: start_key, end_key: end_key})

  defp coverage_fault(_covered, _shard), do: nil

  defp trailing_gap(covered) when covered < @end_of_keyspace,
    do: fault(:layout_gap, %{after_key: covered, before_key: @end_of_keyspace})

  defp trailing_gap(_covered), do: nil

  defp layout_faults(%LayoutResult{faults: faults}), do: faults
  defp layout_faults(_layout), do: []

  defp layout_notes(%LayoutResult{status: :unavailable, reason: reason}),
    do: [%Note{kind: :layout_unavailable, detail: %{reason: reason}}]

  defp layout_notes(%LayoutResult{containment: :undecidable}),
    do: [%Note{kind: :containment_undecidable, detail: %{reason: :legacy_shard_key_encoding}}]

  defp layout_notes(_layout), do: []

  # Skipped under a shard filter: the listing saw only what it was asked
  # for, so every tag it did not look at would read as one with no chunks.
  defp correspondence_notes(%LayoutResult{status: :recovered} = layout, shard_results, nil) do
    stored = shard_results |> Enum.map(& &1.shard_tag) |> Enum.reject(&is_nil/1) |> MapSet.new()
    named = layout.ranges_by_tag |> Map.keys() |> MapSet.new()

    correspondence_note(:shard_not_in_layout, MapSet.difference(stored, named)) ++
      correspondence_note(:layout_shard_without_chunks, MapSet.difference(named, stored))
  end

  defp correspondence_notes(_layout, _shard_results, _shards), do: []

  defp correspondence_note(kind, tags),
    do: tags |> Enum.sort() |> Enum.map(&%Note{kind: kind, shard_tag: &1, detail: %{}})

  # ------------------------------------------------------------------ shard

  defp check_shard(shard_tag, chunks) do
    chunks = Enum.sort_by(chunks, & &1.key)
    ranged = chunks |> Enum.filter(& &1.range) |> Enum.sort_by(fn chunk -> chunk.range end)
    {faults, gaps} = compare_ranges(ranged)

    %ShardResult{
      shard_tag: shard_tag,
      range: shard_range(ranged),
      range_analysis: if(Enum.all?(chunks, & &1.range), do: :complete, else: :partial),
      chunks: chunks,
      faults: Enum.map(faults, &%{&1 | shard_tag: shard_tag}),
      gaps: gaps
    }
  end

  defp shard_range([]), do: nil

  defp shard_range(ranged) do
    {Enum.min(Enum.map(ranged, fn chunk -> elem(chunk.range, 0) end)),
     Enum.max(Enum.map(ranged, fn chunk -> elem(chunk.range, 1) end))}
  end

  # Walks the shard's chunks in ascending range order against the highest
  # max seen so far, so a chunk wholly contained in an earlier one is
  # caught as readily as a partial straddle.
  defp compare_ranges([]), do: {[], []}

  defp compare_ranges([first | rest]) do
    {_covered, faults, gaps} =
      Enum.reduce(rest, {first, [], []}, fn chunk, {covered, faults, gaps} ->
        {_, covered_max} = covered.range
        {min, max} = chunk.range

        {faults, gaps} =
          if min <= covered_max do
            {[overlap_fault(covered, chunk) | faults], gaps}
          else
            {faults, [gap(covered, chunk) | gaps]}
          end

        {if(max > covered_max, do: chunk, else: covered), faults, gaps}
      end)

    {Enum.reverse(faults), Enum.reverse(gaps)}
  end

  defp overlap_fault(earlier, later) do
    fault(:chunk_range_overlap, %{
      earlier_key: earlier.key,
      earlier_range: earlier.range,
      later_key: later.key,
      later_range: later.range
    })
  end

  defp gap(earlier, later) do
    %{
      after_key: earlier.key,
      after_version: elem(earlier.range, 1),
      before_key: later.key,
      before_version: elem(later.range, 0)
    }
  end

  defp shard_faults(shard) do
    Enum.flat_map(shard.chunks, & &1.faults) ++ shard.faults
  end

  defp gap_notes(shard) do
    Enum.map(shard.gaps, &%Note{kind: :version_gap, shard_tag: shard.shard_tag, key: &1.before_key, detail: &1})
  end

  # -------------------------------------------------------------- snapshots

  defp check_snapshots(backend, shard_results, opts) do
    if Keyword.get(opts, :check_snapshots, true) do
      backend
      |> snapshot_keys(Keyword.get(opts, :shards))
      |> Enum.group_by(&snapshot_tag_from_key/1)
      |> Enum.sort_by(fn {shard_tag, _keys} -> shard_tag end)
      |> Enum.map(&check_newest_snapshot(backend, &1, shard_results))
    else
      []
    end
  end

  defp snapshot_keys(backend, nil), do: Enum.to_list(ObjectStorage.list(backend, "s/"))

  defp snapshot_keys(backend, shards),
    do: Enum.flat_map(shards, &Enum.to_list(ObjectStorage.list(backend, Keys.snapshots_prefix(&1))))

  defp snapshot_tag_from_key(key) do
    case Path.split(key) do
      ["s", shard_tag, _basename] -> shard_tag
      _ -> nil
    end
  end

  # Only the newest bundle is opened. `Snapshot.read_latest/1` takes the
  # first key its listing yields and has no fallback to the next, so the
  # newest is the only bundle a cold start will ever open — and inverted
  # names make lexicographically first mean newest. An older bundle that
  # is corrupt is unreachable and therefore harmless; one that is fine is
  # no help if the newest is not.
  defp check_newest_snapshot(backend, {shard_tag, keys}, shard_results) do
    key = keys |> Enum.sort() |> List.first()

    %{
      snapshot_result(backend, key)
      | shard_tag: shard_tag,
        count: length(keys),
        chunk_max_version: chunk_max_version(shard_results, shard_tag)
    }
  end

  # The key is judged before the object is fetched. A name that is not one
  # of ours names an object of unknown size holding nothing this module
  # could interpret, and the fault is already decided by the name.
  defp snapshot_result(backend, key) do
    case check_snapshot_key(key) do
      {[], _version} ->
        fetch_snapshot(backend, key)

      {faults, _version} ->
        shard_tag = snapshot_tag_from_key(key)
        %SnapshotResult{key: key, shard_tag: shard_tag, count: 1, faults: stamp(faults, key, shard_tag)}
    end
  end

  defp fetch_snapshot(backend, key) do
    case ObjectStorage.get(backend, key) do
      {:ok, binary} ->
        check_snapshot(key, binary)

      {:error, reason} ->
        shard_tag = snapshot_tag_from_key(key)
        fault = fault(:snapshot_unreadable, %{reason: reason})
        %SnapshotResult{key: key, shard_tag: shard_tag, count: 1, faults: stamp([fault], key, shard_tag)}
    end
  end

  defp check_snapshot_key(key) do
    case Path.split(key) do
      ["s", _shard_tag, basename] -> check_basename(basename, :unparsable_snapshot_key)
      _ -> {[fault(:malformed_snapshot_key, %{expected: "s/{shard}/{version}"})], nil}
    end
  end

  defp chunk_max_version(shard_results, shard_tag) do
    Enum.find_value(shard_results, fn shard ->
      if shard.shard_tag == shard_tag and shard.range, do: elem(shard.range, 1)
    end)
  end

  # ---------------------------------------------------------- index record

  # The bundle is `[data][index record]`, and the record is what
  # `SnapshotBundle.split_in_place/3` peels off to become the restored
  # shard's `idx` file. So it is validated first exactly the way the
  # restore path finds it — through `find_index_boundary/1`, which trusts
  # the footer's payload_size and the header's magic — and then the way
  # `IndexDatabase` reads it back, which is stricter in ways the restore
  # path never notices.
  defp check_index_record(binary, key_version) do
    case SnapshotBundle.find_index_boundary(binary) do
      {:ok, data_end, idx_size} ->
        check_record(binary, data_end, idx_size, key_version)

      {:error, reason} ->
        {[fault(:snapshot_index_record_invalid, %{reason: reason, bytes: byte_size(binary)})], nil}
    end
  end

  defp check_record(binary, data_end, idx_size, key_version) do
    <<_magic::unsigned-big-32, version::unsigned-big-64, header_payload_size::unsigned-big-32>> =
      binary_part(binary, data_end, @index_header_size)

    record = %{data_end: data_end, header_payload_size: header_payload_size, idx_size: idx_size, version: version}

    {version_faults(version, key_version) ++ payload_faults(binary, record), version}
  end

  # The key and the record are written from ONE value — `Snapshot.write/3`
  # is handed `Version.to_integer(durable_version)` and the record carries
  # the same `durable_version` — so a disagreement is proof of corruption.
  # It matters because the name, not the content, is the selection
  # predicate: `read_latest/1` picks the lexicographically first key, and
  # `SnapshotRetention` decides what to delete from key versions alone. A
  # bundle named above its contents wins both, and can get the shard's
  # real newest state pruned out from under it.
  defp version_faults(_version, nil), do: []
  defp version_faults(same, same), do: []

  defp version_faults(version, key_version),
    do: [fault(:snapshot_version_mismatch, %{key_version: key_version, record_version: version})]

  # `find_index_boundary/1` sizes the record from the FOOTER's copy of
  # payload_size and never looks at the header's, so a disagreement
  # between them restores without complaint — and then
  # `IndexDatabase.read_durable_version/2`, which matches the two against
  # each other, falls through to `Version.zero()`. The shard opens at
  # version zero, loads the page block for a version no record carries,
  # gets none, and comes up EMPTY with nothing raised anywhere. Nothing
  # below may read the payload through a length two sources disagree on.
  defp payload_faults(binary, record) do
    footer_payload_size = record.idx_size - @index_header_size - @index_footer_size

    if record.header_payload_size == footer_payload_size do
      base_faults(binary, record)
    else
      detail = %{header_payload_size: record.header_payload_size, footer_payload_size: footer_payload_size}
      [fault(:snapshot_index_size_disagreement, detail)]
    end
  end

  # A compaction's record points at ITSELF
  # (`IndexDatabase.build_snapshot_record/2` passes the version as its own
  # previous_version), and that self-loop is what terminates the page
  # chain. A record that points anywhere else is a delta out of a LIVE
  # append chain, where the rest of the chain is in the file it was taken
  # from and not in this bundle: the restored shard walks back to a
  # version no record here carries, stops, and serves the handful of pages
  # the delta happened to hold. `Snapshot.write/3` is put-if-not-exists,
  # so the poisoned bundle is permanent. This is the fault with the most
  # teeth in the snapshot namespace — it is silent everywhere else.
  defp base_faults(binary, record) do
    binary
    |> binary_part(record.data_end + @index_header_size, record.header_payload_size)
    |> decode_page_block()
    |> case do
      {:ok, previous_version, _pages} when previous_version == record.version ->
        []

      {:ok, previous_version, pages} ->
        [not_a_base(record.version, previous_version, pages)]

      :error ->
        [fault(:snapshot_index_undecodable, %{payload_size: record.header_payload_size})]
    end
  end

  defp not_a_base(version, previous_version, pages),
    do: fault(:snapshot_index_not_a_base, %{version: version, previous_version: previous_version, pages: pages})

  # `:safe`, because these are bytes off a disk nobody is vouching for and
  # an unbounded `binary_to_term/1` would let a corrupt object mint atoms
  # in the process running fsck. A compacted page map is integers,
  # binaries, tuples and maps — `Page.new/2` returns a binary — so nothing
  # legitimate needs the unsafe form.
  defp decode_page_block(payload) do
    case :erlang.binary_to_term(payload, [:safe]) do
      {<<previous_version::unsigned-big-64>>, pages_map} when is_map(pages_map) ->
        {:ok, previous_version, map_size(pages_map)}

      _other ->
        :error
    end
  rescue
    ArgumentError -> :error
  end

  # ---------------------------------------------------- snapshot continuity

  defp snapshot_notes(snapshot_results, shard_results, opts) do
    if Keyword.get(opts, :check_snapshots, true) do
      Enum.flat_map(snapshot_results, &continuity_note/1) ++ missing_snapshot_notes(snapshot_results, shard_results)
    else
      []
    end
  end

  # Nothing to compare: the record did not yield a version.
  defp continuity_note(%SnapshotResult{durable_version: nil}), do: []

  defp continuity_note(%SnapshotResult{durable_version: version, chunk_max_version: max})
       when is_integer(max) and version <= max, do: []

  # A snapshot AHEAD of its shard's chunks is ordinary operation, not a
  # defect. Olivine clamps its durable version to the KNOWN-COMMITTED
  # version (`Logic.advance_window/1`), which is what the commit proxies
  # report — not to the demux's last confirmed cut, which is what decides
  # when a chunk is written. The two lag independently, so a materializer
  # routinely persists a snapshot covering versions the ShardServer still
  # holds in its buffer. See "What it cannot prove" in the module doc for
  # why the forward-coverage question is not decidable from here either.
  defp continuity_note(result) do
    detail = %{durable_version: result.durable_version, chunk_max_version: result.chunk_max_version}
    [%Note{kind: :snapshot_ahead_of_chunks, shard_tag: result.shard_tag, key: result.key, detail: detail}]
  end

  # A shard with chunks and no snapshot cold starts by replaying its whole
  # history. That is correct — chunks are never deleted, so the replay is
  # always available — but it is slow, and today it is the NORMAL state:
  # nothing drives Olivine compaction (bedrock-947), so a running cluster
  # writes snapshots only on an idle spin-down.
  defp missing_snapshot_notes(snapshot_results, shard_results) do
    with_snapshots = MapSet.new(snapshot_results, & &1.shard_tag)

    shard_results
    |> Enum.map(& &1.shard_tag)
    |> Enum.reject(&(is_nil(&1) or &1 in with_snapshots))
    |> Enum.sort()
    |> Enum.map(&%Note{kind: :shard_without_snapshot, shard_tag: &1, detail: %{}})
  end

  defp stamp(faults, key, shard_tag), do: Enum.map(faults, &%{&1 | key: key, shard_tag: shard_tag})

  # ----------------------------------------------------------------- debris

  # Scratch files are hidden from `ObjectStorage.list/3` — they belong to
  # the backend, not its object namespace — so the only way to see them is
  # to walk the backend's own storage. Only LocalFilesystem has one to
  # walk; every other backend reports nothing rather than something wrong.
  defp debris_notes({LocalFilesystem, config}, shards) do
    root = Keyword.fetch!(config, :root)

    shards
    |> debris_roots(root)
    |> Enum.flat_map(&Path.wildcard(Path.join(&1, "**/#{@scratch_prefix}*"), match_dot: true))
    |> Enum.sort()
    |> Enum.map(&debris_note(&1, root))
  end

  defp debris_notes(_backend, _shards), do: []

  # A shard filter scopes the sweep too, so everything a filtered run
  # reports is about the shards that were asked for.
  defp debris_roots(nil, root), do: [root]
  defp debris_roots(shards, root), do: Enum.map(shards, &Path.join(root, Keys.chunks_prefix(&1)))

  defp debris_note(path, root) do
    key = Path.relative_to(path, root)

    bytes =
      case File.stat(path) do
        {:ok, %{size: size}} -> size
        {:error, _reason} -> nil
      end

    %Note{kind: :scratch_debris, shard_tag: shard_tag_from_key(key), key: key, detail: %{bytes: bytes}}
  end

  defp fault(kind, detail), do: %Fault{kind: kind, detail: detail}
end
