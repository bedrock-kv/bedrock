defmodule Bedrock.Cluster.Link.RoutingCache do
  @moduledoc """
  The node-wide routing cache: which materializer covers a key range.

  A partial, coalescing index of covering entries — FDB's
  `DatabaseContext locationCache`. Entries live until invalidated or
  dropped by a wiring push; staleness is backstopped by the client's
  retry loop, so there is no TTL.

  It is an ETS table rather than server state because EVERY transaction
  on the node reads it, and reading it must not be a message. A cached
  lookup through the owning `GenServer` measured 0.79µs and flattened
  under concurrency; the same lookup here is 0.06µs and gets faster as
  readers are added (0.04µs at 16-way). One process is no longer a
  serialization point for the node's whole read path.

  Writes still go through the Link, which owns the table: one writer
  keeps invalidation ordered against the pushes that trigger it, and the
  reply to a synchronous invalidate still means "the stale entries are
  gone".

  The index is keyed by END key, so a lookup is a ceiling search — the
  smallest range ending after the key — and then a check that the range
  actually starts at or before it. `Bedrock.end_of_keyspace/0`
  (`<<0xFF, 0xFF>>`) is the one INCLUSIVE end: it is the sentinel above
  every real key, not a boundary between ranges, so a key equal to it
  still belongs to the final range.
  """

  @type table :: atom()
  @type entry :: {Bedrock.key_range(), term()}

  @end_of_keyspace <<0xFF, 0xFF>>

  @doc """
  Creates the table. Called by the Link, which owns it.

  Public so any process on the node can READ without a message; readers
  never write. `read_concurrency` because the ratio is overwhelmingly
  reads — one write per cache miss, one read per key lookup.
  """
  @spec new(table()) :: table()
  def new(table), do: :ets.new(table, [:named_table, :public, :ordered_set, read_concurrency: true])

  @doc "Adds or replaces the covering entry for `start_key..end_key`."
  @spec insert(table(), Bedrock.key(), Bedrock.key(), term()) :: :ok
  def insert(table, start_key, end_key, value) do
    :ets.insert(table, {end_key, start_key, value})
    :ok
  end

  @doc "Drops every entry. The Link calls this on invalidation and on a wiring push."
  @spec clear(table()) :: :ok
  def clear(table) do
    :ets.delete_all_objects(table)
    :ok
  end

  @doc """
  The covering entry for `key`, or `:not_cached`.

  Read directly by the calling process — no message to the Link.
  """
  @spec lookup(table(), Bedrock.key()) :: {:ok, entry()} | :not_cached
  def lookup(table, key) do
    do_lookup(table, key)
  rescue
    # No table means no Link yet (or a restarted one). That is a MISS,
    # not a crash: the caller fetches the covering entry from a proxy and
    # the node warms up again. A dead cache must never be worse than an
    # empty one.
    ArgumentError -> :not_cached
  end

  defp do_lookup(table, key) do
    # The sentinel end is inclusive, so a key equal to it has to be tried
    # as an exact hit first: :ets.next/2 is strictly greater and would
    # step right past the range that owns it.
    case key == @end_of_keyspace and :ets.lookup(table, @end_of_keyspace) do
      [{end_key, start_key, value}] when key >= start_key -> {:ok, {{start_key, end_key}, value}}
      _no_sentinel_hit -> lookup_ceiling(table, key)
    end
  end

  defp lookup_ceiling(table, key) do
    case :ets.next(table, key) do
      :"$end_of_table" ->
        :not_cached

      end_key ->
        case :ets.lookup(table, end_key) do
          [{^end_key, start_key, value}] when key >= start_key -> {:ok, {{start_key, end_key}, value}}
          # The nearest range ending after the key starts after it too:
          # the key falls in a gap this partial index does not cover.
          _gap -> :not_cached
        end
    end
  end
end
