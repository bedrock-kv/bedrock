defmodule Bedrock.DataPlane.CommitProxy.RoutingData do
  @moduledoc """
  Immutable routing state for the commit proxy.

  Encapsulates all information needed to route mutations to logs:
  - `shards` - a `:gb_trees` map of `end_key => {tag, start_key}` for key → tag
    ceiling search (`end_key` is the shard's exclusive upper bound)
  - `log_map` - Map of index → log_id for golden ratio routing
  - `log_services` - Map of log_id → pid or {otp_name, node} for contacting logs
  - `materializers` - Map of tag → `{worker_id, node}` (strings, as committed
    to the `materializers/` keyspace family) for the client-facing routing
    projection; clients derive the callable ref
  - `replication_factor` - Number of logs per mutation

  The value is a plain immutable term: the commit proxy server is its only
  writer, applying committed metadata one batch at a time in commit-version
  order, and every finalization task routes from the snapshot the server
  handed it for its batch. Concurrent batches can never observe - or race -
  each other's updates, which is what lets these be ordinary data structures
  with no versions, locks, or shared tables.

  ## Lifecycle

  - `new_empty/0` - Creates empty routing data for dynamic population
  - `from_snapshot/1` - Builds routing data from a plain snapshot at unlock

  ## Shard Updates

  - `insert_shard/4` - Adds or updates a shard entry
  - `delete_shard/2` - Removes a shard entry

  Log wiring (`log_map`, `log_services`, `replication_factor`) is
  epoch-constant: seeded once at unlock and never mutated — changing log
  topology IS a recovery (bedrock-q67.41).
  """

  alias Bedrock.DataPlane.Log
  alias Bedrock.SystemKeys
  alias Bedrock.SystemKeys.Values

  @type shard_tree :: :gb_trees.tree(Bedrock.key(), {tag :: term(), start_key :: Bedrock.key()})

  @typedoc "A materializer ref as committed to the keyspace: worker id and node, both strings."
  @type materializer_ref :: {worker_id :: String.t(), node :: String.t()}

  @type t :: %__MODULE__{
          shards: shard_tree(),
          log_map: %{non_neg_integer() => Log.id()},
          log_services: %{Log.id() => {atom(), node()} | pid()},
          materializers: %{Bedrock.range_tag() => materializer_ref()},
          replication_factor: pos_integer()
        }

  @typedoc """
  A plain-data description of routing state, safe to send between processes
  and nodes. `from_snapshot/1` turns it into runnable routing data.
  """
  @type snapshot :: %{
          optional(:materializers) => %{Bedrock.range_tag() => materializer_ref()},
          shard_layout: %{Bedrock.key() => {tag :: term(), start_key :: Bedrock.key()}},
          log_map: %{non_neg_integer() => Log.id()},
          log_services: %{Log.id() => {atom(), node()} | pid()},
          replication_factor: pos_integer()
        }

  defstruct [:shards, :log_map, :log_services, :replication_factor, materializers: %{}]

  @typedoc """
  The client-facing covering entry for one key: the shard's bounds, its
  tag, and the raw materializer ref. Log wiring stays proxy-internal.
  """
  @type covering_entry ::
          {start_key :: Bedrock.key(), end_key :: Bedrock.key(), tag :: term(), materializer_ref()}

  @doc """
  The committed materializer assignment for a shard tag, or
  `{:error, :not_found}` when the keyspace names none.

  This answers a worker's rejoin validation (FDB's storage-server rejoin
  through the proxy's txnStateStore: absence means `worker_removed`) from
  the same routing view that serves clients — one authority, two readers.
  """
  @spec resolve_materializer(t(), non_neg_integer()) ::
          {:ok, {worker_id :: String.t(), node :: String.t()}} | {:error, :not_found}
  def resolve_materializer(%__MODULE__{materializers: materializers}, tag) do
    case Map.fetch(materializers, tag) do
      {:ok, ref} -> {:ok, ref}
      :error -> {:error, :not_found}
    end
  end

  @doc """
  The covering entry for one key: a ceiling walk over the shard tree plus
  the tag's committed materializer ref.

  Served to clients by the commit proxy (FDB's `GetKeyServerLocations`
  answered from `keyInfo`) — one entry per ask, O(log n), never a bulk
  projection of a map that can number in the thousands. Locations are
  unverified hints: a stale entry costs the client a retry, never a
  wrong answer. `{:error, :not_found}` covers both a key beyond every
  boundary and a shard whose tag names no materializer — to the client
  both are an unroutable key.
  """
  @spec covering_entry(t(), Bedrock.key()) :: {:ok, covering_entry()} | {:error, :not_found}
  def covering_entry(%__MODULE__{shards: shards, materializers: materializers}, key) do
    with {:ok, end_key, {tag, start_key}} <- ceiling_entry(shards, key),
         {:ok, ref} <- Map.fetch(materializers, tag) do
      {:ok, {start_key, end_key, tag, ref}}
    else
      _ -> {:error, :not_found}
    end
  end

  # First entry with end_key > key (end keys are exclusive bounds).
  defp ceiling_entry(shards, key) do
    key
    |> :gb_trees.iterator_from(shards)
    |> :gb_trees.next()
    |> case do
      {end_key, value, _iter} when end_key > key -> {:ok, end_key, value}
      {_same_key, _value, iter} -> next_entry(iter)
      :none -> {:error, :not_found}
    end
  end

  defp next_entry(iter) do
    case :gb_trees.next(iter) do
      {end_key, value, _iter} -> {:ok, end_key, value}
      :none -> {:error, :not_found}
    end
  end

  @doc """
  Builds routing data from a plain snapshot.
  """
  @spec from_snapshot(snapshot()) :: t()
  def from_snapshot(
        %{
          shard_layout: shard_layout,
          log_map: log_map,
          log_services: log_services,
          replication_factor: replication_factor
        } = snapshot
      ) do
    shards =
      Enum.reduce(shard_layout, :gb_trees.empty(), fn {end_key, {tag, start_key}}, tree ->
        :gb_trees.enter(end_key, {tag, start_key}, tree)
      end)

    %__MODULE__{
      shards: shards,
      log_map: log_map,
      log_services: log_services,
      materializers: Map.get(snapshot, :materializers, %{}),
      replication_factor: replication_factor
    }
  end

  @doc """
  Creates empty routing data.

  Starts with no shards, no logs, and replication factor of 1. Shard and
  materializer entries populate incrementally as metadata mutations
  arrive; log wiring is epoch-constant and only `from_snapshot/1` sets it.
  """
  @spec new_empty() :: t()
  def new_empty do
    %__MODULE__{
      shards: :gb_trees.empty(),
      log_map: %{},
      log_services: %{},
      materializers: %{},
      replication_factor: 1
    }
  end

  @doc """
  Inserts or updates a shard entry.

  Called from apply_mutations/2 when processing shard_key mutations.
  """
  @spec insert_shard(t(), binary(), term(), Bedrock.key()) :: t()
  def insert_shard(%__MODULE__{shards: shards} = routing_data, end_key, tag, start_key) do
    %{routing_data | shards: :gb_trees.enter(end_key, {tag, start_key}, shards)}
  end

  @doc """
  Deletes a shard entry.

  Called from apply_mutations/2 when processing shard_key clear mutations.
  """
  @spec delete_shard(t(), binary()) :: t()
  def delete_shard(%__MODULE__{shards: shards} = routing_data, end_key) do
    %{routing_data | shards: :gb_trees.delete_any(end_key, shards)}
  end

  @doc """
  Applies metadata mutations to update routing data.

  Handles shard_key and materializer_key mutations; layout_log mutations
  are deliberately ignored (see the fold's layout_log clause).

  ## Parameters

  - `routing_data` - Current routing data
  - `updates` - List of `{version, [mutations]}` tuples from resolver

  ## Returns

  Updated routing data with applied mutations.
  """
  @spec apply_mutations(t(), [{Bedrock.version(), [term()]}]) :: t()
  def apply_mutations(%__MODULE__{} = routing_data, updates) do
    Enum.reduce(updates, routing_data, fn {_version, mutations}, acc ->
      Enum.reduce(mutations, acc, &apply_mutation/2)
    end)
  end

  defp apply_mutation({:set, key, value}, routing_data) do
    case SystemKeys.parse_key(key) do
      {:shard_key, end_key} ->
        # Undecodable values are ignored; the routing table keeps its last
        # good entry rather than crashing the commit proxy.
        case Values.decode_shard_key_entry(value) do
          {:ok, {tag, start_key}} -> insert_shard(routing_data, end_key, tag, start_key)
          {:error, _} -> routing_data
        end

      {:layout_log, _log_id} ->
        # Log wiring is epoch-constant and seed-only: changing log
        # topology IS a recovery (FDB: a new generation, a new
        # logSystemConfig), so the only layout_log mutations that ever
        # flow through a window are the recovery transaction's rewrite of
        # the very set this proxy was just seeded with. Folding them was
        # a second way of managing one table, guarding a hazard the
        # recovery boundary precludes. The layout/logs/ FAMILY stays
        # durable for other consumers and cluster-introspection tools —
        # this fold just isn't a reader. Materializer refs below
        # deliberately DO ride persisted data: they are client-facing
        # hints, FDB serverList style, and the Distributor mutates them
        # mid-epoch (bedrock-q67.21).
        routing_data

      {:materializer_key, tag} ->
        # Undecodable values are ignored, keeping the last good ref.
        case Values.decode_materializer_ref(value) do
          {:ok, ref} -> %{routing_data | materializers: Map.put(routing_data.materializers, tag, ref)}
          {:error, _} -> routing_data
        end

      _ ->
        routing_data
    end
  end

  defp apply_mutation({:clear, key}, routing_data) do
    case SystemKeys.parse_key(key) do
      {:shard_key, end_key} ->
        delete_shard(routing_data, end_key)

      {:layout_log, _log_id} ->
        # Epoch-constant; see the set clause.
        routing_data

      {:materializer_key, tag} ->
        %{routing_data | materializers: Map.delete(routing_data.materializers, tag)}

      _ ->
        routing_data
    end
  end

  # Mirrors Metadata's clear_range semantics: delete every known entry whose
  # full system key falls in [start_key, end_key). Recovery rewrites use this
  # to drop stale shard/log entries before re-writing the current layout.
  defp apply_mutation({:clear_range, start_key, end_key}, routing_data) do
    routing_data
    |> clear_shards_in_range(start_key, end_key)
    |> clear_materializers_in_range(start_key, end_key)
  end

  defp apply_mutation(_mutation, routing_data), do: routing_data

  defp clear_shards_in_range(%__MODULE__{shards: shards} = routing_data, start_key, end_key) do
    cleared =
      shards
      |> :gb_trees.keys()
      |> Enum.filter(fn shard_end_key ->
        full_key = SystemKeys.shard_key(shard_end_key)
        full_key >= start_key and full_key < end_key
      end)
      |> Enum.reduce(shards, &:gb_trees.delete_any/2)

    %{routing_data | shards: cleared}
  end

  defp clear_materializers_in_range(%__MODULE__{materializers: materializers} = routing_data, start_key, end_key) do
    kept =
      Map.reject(materializers, fn {tag, _ref} ->
        full_key = SystemKeys.materializer_key(tag)
        full_key >= start_key and full_key < end_key
      end)

    %{routing_data | materializers: kept}
  end
end
