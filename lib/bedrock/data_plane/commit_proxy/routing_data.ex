defmodule Bedrock.DataPlane.CommitProxy.RoutingData do
  @moduledoc """
  Immutable routing state for the commit proxy.

  Encapsulates all information needed to route mutations to logs:
  - `shards` - a `:gb_trees` map of `end_key => {tag, start_key}` for key → tag
    ceiling search (`end_key` is the shard's exclusive upper bound)
  - `log_map` - Map of index → log_id for golden ratio routing
  - `log_services` - Map of log_id → pid or {otp_name, node} for contacting logs
  - `materializers` - Map of tag → `%{worker_id => node}` (strings, as
    committed to the `materializers/` keyspace family): a shard's MEMBER
    SET, from which `covering_entry/2` picks the client-facing ref;
    clients derive the callable ref from that pick
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
  alias Bedrock.DataPlane.ShardRouter
  alias Bedrock.SystemKeys
  alias Bedrock.SystemKeys.Values

  @type shard_tree :: :gb_trees.tree(Bedrock.key(), {tag :: term(), start_key :: Bedrock.key()})

  @typedoc "A materializer ref handed to a client: worker id and node, both strings."
  @type materializer_ref :: {worker_id :: String.t(), node :: String.t()}

  @typedoc """
  One shard's committed members: worker id to node. A set, because a
  shard may be served by more than one materializer (bedrock-q67.21.9).
  """
  @type members :: %{Bedrock.Service.Worker.id() => String.t()}

  @type t :: %__MODULE__{
          shards: shard_tree(),
          log_map: %{non_neg_integer() => Log.id()},
          log_services: %{Log.id() => {atom(), node()} | pid()},
          materializers: %{Bedrock.range_tag() => members()},
          replication_factor: pos_integer()
        }

  @typedoc """
  A plain-data description of routing state, safe to send between processes
  and nodes. `from_snapshot/1` turns it into runnable routing data.
  """
  @type snapshot :: %{
          optional(:materializers) => %{Bedrock.range_tag() => members()},
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
  A shard tag's committed members, or `{:error, :not_found}` when the
  keyspace names none.

  This answers a worker's rejoin validation (FDB's storage-server rejoin
  through the proxy's txnStateStore: absence from the set means
  `worker_removed`) from the same routing view that serves clients — one
  authority, two readers asking different questions. The worker asks
  MEMBERSHIP, not resolution: with several members per shard, the ref a
  client happens to be routed to says nothing about whether any other
  member still belongs.
  """
  @spec materializer_members(t(), non_neg_integer()) :: {:ok, members()} | {:error, :not_found}
  def materializer_members(%__MODULE__{materializers: materializers}, tag) do
    case Map.fetch(materializers, tag) do
      {:ok, members} when members != %{} -> {:ok, members}
      _ -> {:error, :not_found}
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
    with {end_key, {tag, start_key}} <- ShardRouter.ceiling_entry(shards, key),
         {:ok, ref} <- pick_member(Map.get(materializers, tag, %{})) do
      {:ok, {start_key, end_key, tag, ref}}
    else
      _ -> {:error, :not_found}
    end
  end

  @doc """
  The client-facing pick among a shard's members: real coverage beats
  the placeholder (which only parks), and the choice is deterministic
  so every proxy answers alike and a client's retry lands consistently.

  THE one pick. The distributor points the placeholder at a shard's
  members through this same function, so the member recovery unlocks,
  the member clients are routed to, and the member parked reads drain
  into cannot disagree. Load- and locality-aware selection is
  bedrock-q67.46's to add here, once.
  """
  @spec pick_member(members()) :: {:ok, materializer_ref()} | :error
  def pick_member(members) when map_size(members) == 0, do: :error

  def pick_member(members) do
    placeholder = SystemKeys.placeholder_worker_id()

    case members |> Map.delete(placeholder) |> Enum.min(fn -> nil end) do
      nil -> {:ok, {placeholder, Map.fetch!(members, placeholder)}}
      {worker_id, node} -> {:ok, {worker_id, node}}
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

  Handles shard_key and materializer_key mutations. Any other system key
  is ignored: log wiring is epoch-constant and rides the unlock seed, and
  an unrecognized family is forward-compatibility, not an error.

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

      {:materializer_key, tag, worker_id} ->
        # Undecodable values are ignored, keeping the last good member set.
        case Values.decode_materializer_node(value) do
          {:ok, node} -> put_member(routing_data, tag, worker_id, node)
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

      {:materializer_key, tag, worker_id} ->
        drop_member(routing_data, tag, worker_id)

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

  # A tag's entry is the member set itself; an emptied set is dropped, so
  # "no members" and "no tag" are the same absence and coverage never
  # hinges on which one a caller happens to observe.
  defp put_member(%__MODULE__{materializers: materializers} = routing_data, tag, worker_id, node) do
    members = materializers |> Map.get(tag, %{}) |> Map.put(worker_id, node)
    %{routing_data | materializers: Map.put(materializers, tag, members)}
  end

  defp drop_member(%__MODULE__{materializers: materializers} = routing_data, tag, worker_id) do
    case materializers |> Map.get(tag, %{}) |> Map.delete(worker_id) do
      empty when empty == %{} -> %{routing_data | materializers: Map.delete(materializers, tag)}
      members -> %{routing_data | materializers: Map.put(materializers, tag, members)}
    end
  end

  defp clear_materializers_in_range(%__MODULE__{materializers: materializers} = routing_data, start_key, end_key) do
    kept =
      materializers
      |> Enum.map(fn {tag, members} ->
        {tag,
         Map.reject(members, fn {worker_id, _node} ->
           full_key = SystemKeys.materializer_key(tag, worker_id)
           full_key >= start_key and full_key < end_key
         end)}
      end)
      |> Enum.reject(fn {_tag, members} -> members == %{} end)
      |> Map.new()

    %{routing_data | materializers: kept}
  end
end
