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

  **There are no checksums.** The chunk format carries none, so integrity
  here rests on magic bytes plus structural completeness: bit rot inside a
  transaction payload is caught only to the extent the transaction's own
  section CRCs catch it.

  ## What is legal

  Superseded and unreferenced chunks are not faults. Chunks are never
  deleted by design, so a shard that has snapshotted far past its oldest
  chunks still has all of them sitting there.

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
  alias Bedrock.SystemKeys
  alias Bedrock.SystemKeys.Reader, as: SystemKeysReader
  alias Bedrock.SystemKeys.Values

  @max_uint64 0xFFFFFFFFFFFFFFFF
  @scratch_prefix ".bedrock-tmp."
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

  defmodule Report do
    @moduledoc """
    The verdict on a store. `clean?` is true exactly when `faults` is empty;
    notes never make a store unclean.

    `layout` is `nil` when layout recovery was not attempted (`check_layout:
    false`); otherwise it is a `LayoutResult`, which may still be
    `:unavailable`.
    """

    @type t :: %__MODULE__{
            clean?: boolean(),
            chunk_count: non_neg_integer(),
            shards: [ShardResult.t()],
            layout: LayoutResult.t() | nil,
            faults: [Fault.t()],
            notes: [Note.t()]
          }

    defstruct [:clean?, :chunk_count, :layout, shards: [], faults: [], notes: []]
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

    faults = layout_faults(layout) ++ Enum.flat_map(shard_results, &shard_faults/1)

    notes =
      debris_notes(backend, shards) ++
        Enum.flat_map(shard_results, &gap_notes/1) ++
        layout_notes(layout) ++ correspondence_notes(layout, shard_results, shards)

    %Report{
      clean?: faults == [],
      chunk_count: shard_results |> Enum.map(&length(&1.chunks)) |> Enum.sum(),
      shards: shard_results,
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
      ["c", _shard_tag, basename] -> check_basename(basename)
      _ -> {[fault(:malformed_chunk_key, %{expected: "c/{shard}/{version}"})], nil}
    end
  end

  defp check_basename(basename) do
    with {:ok, inverted} <- Keys.parse_inverted_version(basename),
         true <- inverted <= @max_uint64,
         ^basename <- Keys.format_inverted_version(inverted) do
      {[], Keys.restore_version(inverted)}
    else
      _ -> {[fault(:unparsable_key, %{basename: basename})], nil}
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

  defp recover_layout(backend, opts) do
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
