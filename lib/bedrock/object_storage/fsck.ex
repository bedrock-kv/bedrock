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
  self-describing by listing alone, so there is no referential integrity to
  check: validation is structural, per chunk, and then per shard across the
  chunks a listing turns up.

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

  alias Bedrock.DataPlane.Transaction
  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.Chunk
  alias Bedrock.ObjectStorage.Keys
  alias Bedrock.ObjectStorage.LocalFilesystem

  @max_uint64 0xFFFFFFFFFFFFFFFF
  @scratch_prefix ".bedrock-tmp."

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

  defmodule Report do
    @moduledoc """
    The verdict on a store. `clean?` is true exactly when `faults` is empty;
    notes never make a store unclean.
    """

    @type t :: %__MODULE__{
            clean?: boolean(),
            chunk_count: non_neg_integer(),
            shards: [ShardResult.t()],
            faults: [Fault.t()],
            notes: [Note.t()]
          }

    defstruct [:clean?, :chunk_count, shards: [], faults: [], notes: []]
  end

  @doc """
  Checks every transaction chunk in a store.

  ## Options

  - `:shards` - only check these shard tags (default: every shard the
    listing turns up)
  - `:check_transactions` - decode each transaction slice and compare its
    commit version against its directory entry (default: `true`)

  Raises `ObjectStorage.ListError` if the store cannot be listed: a short
  listing would report chunks as absent without having looked for them,
  which is the exact failure this tool exists to find.
  """
  @spec check(ObjectStorage.backend(), keyword()) :: Report.t()
  def check(backend, opts \\ []) do
    shards = Keyword.get(opts, :shards)

    shard_results =
      backend
      |> chunk_keys(shards)
      |> Enum.map(&check_stored_chunk(backend, &1, opts))
      |> Enum.group_by(& &1.shard_tag)
      |> Enum.sort_by(fn {shard_tag, _} -> shard_tag end)
      |> Enum.map(fn {shard_tag, chunks} -> check_shard(shard_tag, chunks) end)

    faults = Enum.flat_map(shard_results, &shard_faults/1)
    notes = debris_notes(backend, shards) ++ Enum.flat_map(shard_results, &gap_notes/1)

    %Report{
      clean?: faults == [],
      chunk_count: shard_results |> Enum.map(&length(&1.chunks)) |> Enum.sum(),
      shards: shard_results,
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
  """
  @spec check_chunk(String.t(), binary(), keyword()) :: ChunkResult.t()
  def check_chunk(key, binary, opts \\ []) do
    shard_tag = shard_tag_from_key(key)
    {key_faults, key_version} = check_key(key)
    {body_faults, range, txn_count} = check_body(binary, key_version, opts)

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
      <<header_binary::binary-size(header_size), rest::binary>> ->
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
      <<directory_binary::binary-size(directory_size), data::binary>> = rest

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
      Enum.flat_map(directory, &transaction_fault(&1, binary_part(data, &1.offset, &1.length)))
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
