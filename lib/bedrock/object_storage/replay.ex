defmodule Bedrock.ObjectStorage.Replay do
  @moduledoc """
  Rebuilds the keyspace from object storage alone, and diffs it against what
  a running cluster serves.

  This is the only check in the family that asks whether the cluster is
  *right* rather than whether it is internally consistent. Everything it
  needs is already in the store: `ChunkReader.read_all_transactions/2`
  yields `{version, slice}` oldest-first for a shard with no cluster
  involvement at all, slices decode with `Transaction.mutations/1`, and the
  fold of those mutations is an independent image of the keyspace. Nothing
  the cluster reports about itself is consulted, so when the two disagree
  neither side gets a vote.

  `reconstruct/2` is the offline half and is what `mix bedrock.replay`
  prints. `diff/3` and `compare/3` are the oracle a chaos harness calls at
  quiesce; they return structured results and never print, because a test
  that parses stdout is a test that breaks when the formatting changes.

  ## What it replays, and how

  The shard layout is not in the store as a map — it is replayed out of the
  system shard's own chunks. `Fsck.recover_layout/2` does that (see its
  moduledoc for why the answer is only trusted when the system shard is
  structurally clean), and this module replays exactly the tags that layout
  names. Chunks filed under a tag the layout has forgotten are listed as
  `orphan_shards` and contribute nothing: the layout decides who owns the
  keyspace, and a tag nobody routes to owns none of it.

  Each shard is folded separately, oldest version first, applying mutations
  the way `Olivine.IndexUpdate.apply_mutation/3` applies them — including
  its rule that a `set`, `clear` or `clear_range` at or past
  `Bedrock.end_of_keyspace/0` is dropped rather than stored, because only
  the commit proxy synthesizes one and it does so to address a worker
  rather than a shard. Atomics resolve through `Bedrock.Internal.Atomics`
  against the value the fold has so far, with a missing value reading as
  `<<>>`, which is what the materializer does. Where the materializer and
  the specification disagree this module follows the *materializer*: it
  exists to find disagreement between two paths through the same code, not
  to relitigate what either should have done.

  The per-shard folds are then merged, and a key claimed by more than one
  shard is resolved to the newest write and reported in
  `multiply_claimed_keys`. That merge is exact while each key is owned by
  exactly one tag across the whole replayed history, which is true as long
  as nothing moves a shard boundary — the same condition `Fsck`'s
  containment check rests on. A `clear_range` recorded by the tag that owns
  a key today cannot suppress an older write to that key filed under a tag
  that owned it yesterday; if resharding ever lands, that is the case to
  revisit, and `multiply_claimed_keys` is the signal that it has.

  ## Anchoring the comparison, and what it costs

  Storage and a live cluster are both moving targets, and they cannot be
  pinned to a common version:

  - The store has no per-shard "durable through" marker. A shard's newest
    chunk names the last commit that *touched* that shard, and nothing
    distinguishes "flushed everything" from "has taken no writes lately" —
    the same ambiguity that makes a version gap unfaultable in `Fsck`. So
    `anchor` (the lowest such version across the shards that have chunks)
    is a lower bound on a version every shard is durable through, and
    `frontier` (the highest) is a lower bound on what the store has seen.
    Neither is a version the whole store is *exactly* at.
  - The client cannot ask for a past read version. A read version comes
    from the sequencer, so the live side is read at whatever version the
    cluster is at when it is asked.

  So there is no honest way to make the two sides meet at one version while
  the cluster is writing, and this module does not pretend otherwise.
  `diff/3` reports `soundness: :advisory` unless the caller passes
  `quiesced: true` to assert two things only the caller can know: that
  writes have stopped, and that the flush pipeline has drained. An
  `:advisory` diff is a list of candidates, every one of which has an
  innocent explanation — a commit the store has not flushed yet, or one it
  has flushed since the live capture.

  What the module *can* do without being told is exonerate one direction
  per key. `compare/3` captures the live side first and reconstructs after,
  so the store may legitimately be newer; a storage entry whose write
  version is above the live read version therefore explains itself, and is
  counted in `explained_by_skew` rather than reported. The other direction
  has no such out: a key present live and absent from storage is
  indistinguishable from a key whose flush has not happened yet.

  `through_version:` truncates the replay, which buys the other trade — a
  cross-shard-consistent image as of that version, at the cost of a diff
  against a live cluster that is certainly past it. It is there for callers
  who want the consistent image for its own sake.

  ## Fidelity

  A mutation fold gives **point-lookup equivalence** and nothing more.
  `image.keys` is exactly the set of keys a point read should find and the
  values it should return. It is not a page index, so it says nothing about
  what a *range* read would return — key order is recovered, but page
  boundaries, page chaining and the value locators a materializer actually
  dereferences are not reconstructed at all. A store whose keys are all
  correct and whose page index is corrupt looks clean here. Closing that
  gap means reconstructing the index, which is a much larger job than this
  one.

  ## Example

      backend = ObjectStorage.backend(LocalFilesystem, root: "/var/lib/bedrock/objects")

      {:ok, diff} =
        Replay.compare(backend, fn ->
          Repo.transact(fn ->
            {:ok, version} = Repo.read_version()
            pairs = Repo.get_range("", <<0xFF>>) |> Enum.to_list()
            {:ok, LiveCapture.new(version, pairs, range: {"", <<0xFF>>})}
          end)
        end, quiesced: true)

      diff.agreed?
      #=> false

      diff.missing_from_live
      #=> [%{key: "user/42", storage_value: "…", storage_version: 918_273}]
  """

  alias Bedrock.DataPlane.Transaction
  alias Bedrock.Internal.Atomics
  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.ChunkReader
  alias Bedrock.ObjectStorage.Fsck
  alias Bedrock.ObjectStorage.Fsck.LayoutResult
  alias Bedrock.ObjectStorage.Keys

  # `Bedrock.end_of_keyspace/0`, inlined the way the materializer inlines
  # it as `@max_key`: the exclusive top of every shard's range.
  @end_of_keyspace <<0xFF, 0xFF>>
  @default_limit 100

  defmodule ShardImage do
    @moduledoc """
    What one shard's chunks contributed.

    `max_chunk_version` is read from the newest chunk's NAME, which is what
    a reader selects on, and is the untruncated figure even when the replay
    was cut short by `through_version:`. `max_replayed_version` is the
    highest version actually folded in.
    """

    @type t :: %__MODULE__{
            shard_tag: String.t(),
            chunk_count: non_neg_integer(),
            transaction_count: non_neg_integer(),
            max_chunk_version: non_neg_integer() | nil,
            max_replayed_version: non_neg_integer() | nil,
            entries: %{Bedrock.key() => term()}
          }

    defstruct [
      :shard_tag,
      :max_chunk_version,
      :max_replayed_version,
      chunk_count: 0,
      transaction_count: 0,
      entries: %{}
    ]
  end

  defmodule Image do
    @moduledoc """
    The keyspace as object storage alone describes it.

    `keys` maps every live key to `%{value:, version:, shard_tag:}` —
    `version` being the commit version of the write that last set it, which
    is what lets `diff/3` exonerate a storage write that postdates a live
    capture. Cleared keys are absent, not present-and-empty.

    `frontier` and `anchor` are the highest and lowest per-shard
    `max_chunk_version`; see the module doc for what each is and is not
    evidence of. Both are `nil` when no replayed shard has chunks.
    """

    @type entry :: %{value: Bedrock.value(), version: non_neg_integer(), shard_tag: String.t()}

    @type t :: %__MODULE__{
            keys: %{Bedrock.key() => entry()},
            frontier: non_neg_integer() | nil,
            anchor: non_neg_integer() | nil,
            through_version: non_neg_integer() | nil,
            shards: [ShardImage.t()],
            shards_without_chunks: [String.t()],
            orphan_shards: [String.t()],
            multiply_claimed_keys: [Bedrock.key()],
            layout: LayoutResult.t()
          }

    defstruct [
      :frontier,
      :anchor,
      :through_version,
      :layout,
      keys: %{},
      shards: [],
      shards_without_chunks: [],
      orphan_shards: [],
      multiply_claimed_keys: []
    ]
  end

  defmodule LiveCapture do
    @moduledoc """
    What the cluster served, and the read version it served it at.

    Build one inside a single transaction so that `version` and `pairs`
    describe the same instant: acquire the read version, read the range,
    and materialize the pairs before the transaction ends. A lazy stream
    that outlives its transaction is a capture of nothing in particular.

    `range` is the half-open range the capture covers, and the image is
    filtered to it before diffing — so a harness that reads only the user
    keyspace does not get told that every system key is missing from live.
    """

    @type t :: %__MODULE__{
            version: non_neg_integer(),
            range: {Bedrock.key(), Bedrock.key()},
            pairs: [{Bedrock.key(), Bedrock.value()}]
          }

    defstruct [:version, :range, pairs: []]

    @end_of_keyspace <<0xFF, 0xFF>>

    @doc """
    Builds a capture, taking the read version either encoded or as an integer.

    ## Options

    - `:range` - the half-open range the pairs cover (default: the whole
      keyspace)
    """
    @spec new(Bedrock.version() | non_neg_integer(), [{Bedrock.key(), Bedrock.value()}], keyword()) :: t()
    def new(version, pairs, opts \\ []) do
      %__MODULE__{
        version: normalize_version(version),
        range: Keyword.get(opts, :range, {<<>>, @end_of_keyspace}),
        pairs: pairs
      }
    end

    defp normalize_version(<<version::unsigned-big-64>>), do: version
    defp normalize_version(version) when is_integer(version) and version >= 0, do: version
  end

  defmodule Diff do
    @moduledoc """
    What the two sides disagree about, and how much that disagreement is
    worth.

    `agreed?` is true exactly when all three difference lists are empty.
    The lists are capped by the `:limit` option; `counts` is not, so a
    truncated report still tells you how big the problem is.

    `soundness` is `:quiesced` only when the caller asserted it. See the
    module doc: nothing in the store can establish that the cluster has
    stopped writing and that its flush pipeline has drained, so the module
    will not claim it on the caller's behalf. `caveats` are the conditions
    it *can* see that weaken the comparison.
    """

    @type difference :: %{required(:key) => Bedrock.key(), optional(atom()) => term()}

    @type t :: %__MODULE__{
            agreed?: boolean(),
            soundness: :quiesced | :advisory,
            live_version: non_neg_integer(),
            storage_frontier: non_neg_integer() | nil,
            storage_anchor: non_neg_integer() | nil,
            range: {Bedrock.key(), Bedrock.key()},
            storage_key_count: non_neg_integer(),
            live_key_count: non_neg_integer(),
            missing_from_live: [difference()],
            missing_from_storage: [difference()],
            value_mismatches: [difference()],
            counts: %{atom() => non_neg_integer()},
            caveats: [atom()]
          }

    defstruct [
      :agreed?,
      :soundness,
      :live_version,
      :storage_frontier,
      :storage_anchor,
      :range,
      :storage_key_count,
      :live_key_count,
      :counts,
      missing_from_live: [],
      missing_from_storage: [],
      value_mismatches: [],
      caveats: []
    ]
  end

  # ------------------------------------------------------------ reconstruct

  @doc """
  Rebuilds the keyspace from the store's chunks.

  ## Options

  - `:layout` - a `LayoutResult` to replay against, rather than recovering
    one. A caller that has just run `Fsck.check/2` already has it.
  - `:shards` - only replay these shard tags. Orphan detection is skipped
    under a filter, for the reason `Fsck` skips its correspondence notes:
    a filtered listing cannot tell a tag that was not looked at from one
    that has no chunks.
  - `:through_version` - stop the replay at this version, for a
    cross-shard-consistent image as of it

  Fails rather than half-succeeding: a store whose chunks will not read or
  whose slices will not decode has no reconstruction, only a guess. Run
  `Fsck.check/2` first — every fault it reports is a reason a replay
  silently returns the wrong answer.
  """
  @spec reconstruct(ObjectStorage.backend(), keyword()) :: {:ok, Image.t()} | {:error, term()}
  def reconstruct(backend, opts \\ []) do
    with {:ok, layout} <- layout_for(backend, opts) do
      build_image(backend, layout, opts)
    end
  end

  @doc """
  Looks one key up in a reconstructed image.

  `:error` means the key is not in the store's keyspace — either never
  written, or cleared.
  """
  @spec fetch(Image.t(), Bedrock.key()) :: {:ok, Bedrock.value()} | :error
  def fetch(%Image{keys: keys}, key) do
    case Map.fetch(keys, key) do
      {:ok, %{value: value}} -> {:ok, value}
      :error -> :error
    end
  end

  defp layout_for(backend, opts) do
    case Keyword.get(opts, :layout) || Fsck.recover_layout(backend) do
      %LayoutResult{status: :recovered} = layout -> {:ok, layout}
      %LayoutResult{status: :unavailable, reason: reason} -> {:error, {:layout_unavailable, reason}}
    end
  end

  defp build_image(backend, layout, opts) do
    filter = Keyword.get(opts, :shards)
    through = Keyword.get(opts, :through_version)
    tags = replay_tags(layout, filter)

    with {:ok, shards} <- fold_shards(backend, tags, through) do
      {keys, claimed} = merge_shards(shards)

      {:ok,
       %Image{
         keys: keys,
         frontier: extreme(shards, &Enum.max/1),
         anchor: extreme(shards, &Enum.min/1),
         through_version: through,
         shards: shards,
         shards_without_chunks: for(shard <- shards, shard.chunk_count == 0, do: shard.shard_tag),
         orphan_shards: orphan_shards(backend, tags, filter),
         multiply_claimed_keys: claimed,
         layout: layout
       }}
    end
  end

  # The layout's own tags, as object storage spells them. `ranges_by_tag`
  # already carries every tag the replayed history named, which is the set
  # whose chunks are entitled to hold keyspace.
  defp replay_tags(layout, nil), do: layout.ranges_by_tag |> Map.keys() |> Enum.sort()
  defp replay_tags(layout, filter), do: layout |> replay_tags(nil) |> Enum.filter(&(&1 in filter))

  defp extreme(shards, pick) do
    case for(shard <- shards, shard.max_chunk_version, do: shard.max_chunk_version) do
      [] -> nil
      versions -> pick.(versions)
    end
  end

  defp orphan_shards(_backend, _tags, filter) when filter != nil, do: []

  defp orphan_shards(backend, tags, _filter) do
    backend
    |> ObjectStorage.list("c/")
    |> Stream.map(&shard_tag_from_key/1)
    |> Stream.reject(&(is_nil(&1) or &1 in tags))
    |> Enum.uniq()
    |> Enum.sort()
  end

  defp shard_tag_from_key(key) do
    case Path.split(key) do
      ["c", shard_tag | _] -> shard_tag
      _ -> nil
    end
  end

  # ----------------------------------------------------------------- folding

  defp fold_shards(backend, tags, through) do
    tags
    |> Enum.reduce_while({:ok, []}, fn tag, {:ok, acc} ->
      case fold_shard(backend, tag, through) do
        {:ok, shard} -> {:cont, {:ok, [shard | acc]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, shards} -> {:ok, Enum.reverse(shards)}
      {:error, _reason} = error -> error
    end
  end

  defp fold_shard(backend, shard_tag, through) do
    reader = ChunkReader.new(backend, shard_tag)
    keys = reader |> ChunkReader.list_chunks() |> Enum.to_list()

    reader
    |> ChunkReader.read_all_transactions()
    |> Enum.reduce_while({%{}, 0, nil}, &fold_slice(&1, &2, shard_tag, through))
    |> case do
      {:error, _reason} = error ->
        error

      {entries, count, replayed_max} ->
        {:ok,
         %ShardImage{
           shard_tag: shard_tag,
           chunk_count: length(keys),
           transaction_count: count,
           max_chunk_version: newest_chunk_version(keys),
           max_replayed_version: replayed_max,
           entries: entries
         }}
    end
  rescue
    e in ChunkReader.ReadError -> {:error, {:unreadable_chunk, e.key, e.reason}}
  end

  # The listing is newest-first, so the first key names the shard's highest
  # version. A name that will not parse is a fault `Fsck` reports; here it
  # only costs the shard its contribution to the frontier.
  defp newest_chunk_version([]), do: nil

  defp newest_chunk_version([newest | _rest]) do
    case Keys.extract_version(newest) do
      {:ok, version} -> version
      {:error, _reason} -> nil
    end
  end

  # Skipped rather than halted past the truncation point: the stream is
  # ascending only while the store is structurally sound, and a replay that
  # stopped early on an out-of-order chunk would silently drop the rest.
  defp fold_slice({version, _slice}, acc, _shard_tag, through) when is_integer(through) and version > through,
    do: {:cont, acc}

  defp fold_slice({version, slice}, {entries, count, replayed_max}, shard_tag, _through) do
    case apply_slice(entries, version, slice) do
      {:ok, entries} -> {:cont, {entries, count + 1, max(version, replayed_max || version)}}
      {:error, reason} -> {:halt, {:error, {:undecodable_transaction, shard_tag, version, reason}}}
    end
  end

  # A transaction with no MUTATIONS section mutates nothing; anything else
  # that will not decode means the fold below it is fiction.
  defp apply_slice(entries, version, slice) do
    case Transaction.mutations(slice) do
      {:ok, mutations} -> {:ok, Enum.reduce(mutations, entries, &apply_mutation(&1, version, &2))}
      {:error, :section_not_found} -> {:ok, entries}
      {:error, reason} -> {:error, reason}
    end
  end

  # These two clauses are `IndexUpdate.apply_mutation/3`'s first two,
  # verbatim in shape: a two- or three-element mutation naming a key at or
  # past the end of the keyspace is a privatized notice addressed to a
  # worker, and the materializer refuses to store it. Note that an
  # `{:atomic, op, key, value}` is a four-element tuple and so escapes both
  # — there too, deliberately, because this image is only useful if it
  # agrees with the materializer about what the materializer stores.
  defp apply_mutation({_op, key}, _version, entries) when key >= @end_of_keyspace, do: entries
  defp apply_mutation({_op, key, _value}, _version, entries) when key >= @end_of_keyspace, do: entries

  defp apply_mutation({:set, key, value}, version, entries), do: Map.put(entries, key, {:value, value, version})

  defp apply_mutation({:clear, key}, version, entries), do: Map.put(entries, key, {:cleared, version})

  # Tombstones rather than deletions, so the merge across shards can see
  # that this shard cleared the key at a version and not merely that it
  # does not hold it.
  defp apply_mutation({:clear_range, start_key, end_key}, version, entries) when start_key < end_key do
    entries
    |> Map.keys()
    |> Enum.filter(&(&1 >= start_key and &1 < end_key))
    |> Enum.reduce(entries, &Map.put(&2, &1, {:cleared, version}))
  end

  defp apply_mutation({:clear_range, _start_key, _end_key}, _version, entries), do: entries

  defp apply_mutation({:atomic, op, key, operand}, version, entries) do
    case Atomics.apply_operation(op, current_value(entries, key), operand) do
      nil -> Map.put(entries, key, {:cleared, version})
      value -> Map.put(entries, key, {:value, value, version})
    end
  end

  # A mutation shape this module does not know cannot be folded, and
  # guessing at one would put a wrong answer in an image whose whole value
  # is being right. Whatever introduces one teaches this function about it.
  defp apply_mutation(_mutation, _version, entries), do: entries

  # A key the materializer has never seen reads as `<<>>` for the purposes
  # of an atomic op, which is what makes `add` on a missing key the operand.
  defp current_value(entries, key) do
    case Map.get(entries, key) do
      {:value, value, _version} -> value
      _absent_or_cleared -> <<>>
    end
  end

  # ------------------------------------------------------------------ merging

  defp merge_shards(shards) do
    {winners, claims} =
      Enum.reduce(shards, {%{}, %{}}, fn shard, acc ->
        Enum.reduce(shard.entries, acc, &claim(&1, shard.shard_tag, &2))
      end)

    keys =
      for {key, {shard_tag, {:value, value, version}}} <- winners,
          into: %{},
          do: {key, %{value: value, version: version, shard_tag: shard_tag}}

    {keys, for({key, tags} <- Enum.sort(claims), MapSet.size(tags) > 1, do: key)}
  end

  defp claim({key, entry}, shard_tag, {winners, claims}) do
    {Map.update(winners, key, {shard_tag, entry}, &newer(&1, {shard_tag, entry})),
     Map.update(claims, key, MapSet.new([shard_tag]), &MapSet.put(&1, shard_tag))}
  end

  # Ties go to the shard already holding the key, which — since the shards
  # are folded in sorted tag order — makes the resolution deterministic
  # rather than merely arbitrary.
  defp newer({_tag, held} = holder, {_tag2, candidate} = challenger),
    do: if(version_of(candidate) > version_of(held), do: challenger, else: holder)

  defp version_of({:value, _value, version}), do: version
  defp version_of({:cleared, version}), do: version

  # --------------------------------------------------------------------- diff

  @doc """
  Diffs a reconstructed image against a live capture.

  Compares only inside the capture's range, and only by point lookup: see
  the module doc for what that does and does not establish.

  ## Options

  - `:quiesced` - assert that the cluster has stopped writing and that its
    flush pipeline has drained (default: `false`). Nothing in the store can
    establish this, and without it every difference has an innocent
    explanation, so the result is marked `:advisory`.
  - `:limit` - how many differences of each kind to list (default: 100).
    The counts are never capped.
  """
  @spec diff(Image.t(), LiveCapture.t(), keyword()) :: Diff.t()
  def diff(%Image{} = image, %LiveCapture{} = live, opts \\ []) do
    {start_key, end_key} = live.range
    stored = for {key, entry} <- image.keys, key >= start_key, key < end_key, into: %{}, do: {key, entry}
    served = Map.new(live.pairs)

    {differences, skew} =
      stored
      |> Map.keys()
      |> Enum.concat(Map.keys(served))
      |> Enum.uniq()
      |> Enum.sort()
      |> Enum.reduce({%{}, 0}, &classify(&1, stored, served, live.version, &2))

    build_diff(image, live, %{stored: stored, served: served, differences: differences, skew: skew}, opts)
  end

  @doc """
  Captures the live side, reconstructs the store, and diffs them.

  `live_fun` must return `{:ok, LiveCapture.t()}`, and is called before the
  chunks are folded. That order is deliberate: it puts the store on the
  *newer* side of whatever skew there is, and a storage write above the
  capture's read version explains itself (see the module doc). The other
  order leaves every difference unexplainable. Only the layout is recovered
  ahead of the capture, so a store that cannot be replayed at all costs
  nothing — and nothing moves a shard boundary, so recovering it early
  cannot go stale.

  Takes the same options as `reconstruct/2` and `diff/3`.
  """
  @spec compare(ObjectStorage.backend(), (-> {:ok, LiveCapture.t()} | {:error, term()}), keyword()) ::
          {:ok, Diff.t()} | {:error, term()}
  def compare(backend, live_fun, opts \\ []) when is_function(live_fun, 0) do
    with {:ok, layout} <- layout_for(backend, opts),
         {:ok, %LiveCapture{} = live} <- live_fun.(),
         {:ok, image} <- build_image(backend, layout, opts) do
      {:ok, diff(image, live, opts)}
    end
  end

  # A storage entry written above the live read version is not a
  # disagreement: the capture predates the write, and this module chose
  # that order precisely so it could say so. The reverse — a key live holds
  # and storage does not — has no equivalent out, because an unflushed
  # write and a lost one look identical from here.
  defp classify(key, stored, served, live_version, {differences, skew}) do
    case {Map.fetch(stored, key), Map.fetch(served, key)} do
      {{:ok, %{value: value}}, {:ok, value}} ->
        {differences, skew}

      {{:ok, %{version: version}}, _served} when version > live_version ->
        {differences, skew + 1}

      {{:ok, entry}, {:ok, live_value}} ->
        {record(differences, :value_mismatches, mismatch(key, entry, live_value)), skew}

      {{:ok, entry}, :error} ->
        {record(differences, :missing_from_live, absent_live(key, entry)), skew}

      {:error, {:ok, live_value}} ->
        {record(differences, :missing_from_storage, %{key: key, live_value: live_value}), skew}
    end
  end

  defp mismatch(key, entry, live_value) do
    %{
      key: key,
      storage_value: entry.value,
      live_value: live_value,
      storage_version: entry.version,
      shard_tag: entry.shard_tag
    }
  end

  defp absent_live(key, entry),
    do: %{key: key, storage_value: entry.value, storage_version: entry.version, shard_tag: entry.shard_tag}

  defp record(differences, kind, difference), do: Map.update(differences, kind, [difference], &[difference | &1])

  defp build_diff(image, live, compared, opts) do
    limit = Keyword.get(opts, :limit, @default_limit)
    kinds = [:missing_from_live, :missing_from_storage, :value_mismatches]
    found = Map.new(kinds, &{&1, compared.differences |> Map.get(&1, []) |> Enum.reverse()})
    counts = Map.new(kinds, fn kind -> {kind, length(found[kind])} end)

    %Diff{
      agreed?: Enum.all?(kinds, &(counts[&1] == 0)),
      soundness: if(Keyword.get(opts, :quiesced, false), do: :quiesced, else: :advisory),
      live_version: live.version,
      storage_frontier: image.frontier,
      storage_anchor: image.anchor,
      range: live.range,
      storage_key_count: map_size(compared.stored),
      live_key_count: map_size(compared.served),
      missing_from_live: Enum.take(found.missing_from_live, limit),
      missing_from_storage: Enum.take(found.missing_from_storage, limit),
      value_mismatches: Enum.take(found.value_mismatches, limit),
      counts: Map.put(counts, :explained_by_skew, compared.skew),
      caveats: caveats(image, live)
    }
  end

  # Everything here weakens the comparison without being a finding of its
  # own. A shard that has flushed nothing is the loudest: it has no chunks
  # at all, so the image says nothing whatsoever about the keys it owns.
  defp caveats(image, live) do
    Enum.reject(
      [
        if(image.shards_without_chunks != [], do: :shards_without_chunks),
        if(image.orphan_shards != [], do: :orphan_shards),
        if(image.multiply_claimed_keys != [], do: :multiply_claimed_keys),
        if(image.through_version, do: :replay_truncated),
        skew_caveat(image.frontier, live.version)
      ],
      &is_nil/1
    )
  end

  defp skew_caveat(nil, _live_version), do: nil
  defp skew_caveat(frontier, live_version) when frontier > live_version, do: :storage_ahead_of_live
  defp skew_caveat(frontier, live_version) when frontier < live_version, do: :live_ahead_of_storage
  defp skew_caveat(_frontier, _live_version), do: nil
end
