defmodule Bedrock.SystemKeys do
  @moduledoc """
  The `\\xFF/system` keys Bedrock writes and reads.

  Every key defined here has a named purpose: `shard_keys/<end_key>` feeds
  each commit proxy's routing view (through resolver metadata windows) and
  the next recovery's materializer bootstrap; `materializers/<tag>/<worker_id>` entries
  feed the client-facing routing projection served by commit proxies
  (FDB's `serverList/` analogue - interfaces ride the keyspace), answer
  worker rejoin validation, and give recovery's materializer bootstrap
  its re-adoption input (`read_prior_refs`) and the persistence phase's
  diff base. To clients the members are hints, unverified and cached;
  to recovery they are durable state. `distributor_lock/{owner,
  write}` is the distributor's write fence (FDB's MoveKeys lock,
  bedrock-q67.21): opaque UIDs read-checked-written inside every
  mutating distributor transaction, so ownership is enforced by the
  commit pipeline itself. A system key without a
  reader is inventory, not communication - unread MACHINERY is deleted,
  while durable observability keys stay by decision, named as such;
  families return here when their readers do (config authority with
  bedrock-q67.25).
  """

  alias Bedrock.Service.Worker

  @system_prefix "\xff/system"

  @doc "Distributor write-fence owner UID: `distributor_lock/owner` (FDB's moveKeysLockOwnerKey)"
  @spec distributor_lock_owner() :: Bedrock.key()
  def distributor_lock_owner, do: "#{@system_prefix}/distributor_lock/owner"

  @doc "Distributor write-fence write UID: `distributor_lock/write` (FDB's moveKeysLockWriteKey)"
  @spec distributor_lock_write() :: Bedrock.key()
  def distributor_lock_write, do: "#{@system_prefix}/distributor_lock/write"

  @doc "Shard boundary entry: `shard_keys/<end_key>` -> `{tag, start_key}` (ceiling search)"
  @spec shard_key(Bedrock.key()) :: Bedrock.key()
  def shard_key(end_key), do: "#{@system_prefix}/shard_keys/#{end_key}"

  @doc "Prefix covering every shard boundary entry"
  @spec shard_keys_prefix() :: Bedrock.key()
  def shard_keys_prefix, do: "#{@system_prefix}/shard_keys/"

  @doc """
  Membership entry: `materializers/<tag>/<worker_id>` -> node string.

  Tag-major with the worker id IN the key, so one family answers both
  questions FDB needs two for: a prefix scan over a tag gives the shard's
  members (FDB's range-major `keyServers/<range>` -> team), and each
  member is individually addressable for removal (FDB's server-major
  `serverKeys/<server>/<range>`). A worker owns exactly one shard and
  notices broadcast on the shard's tag, so no per-worker index is needed.
  Membership is expressed by key PRESENCE; removal is a clear.
  """
  @spec materializer_key(Bedrock.range_tag(), Worker.id()) :: Bedrock.key()
  def materializer_key(tag, worker_id) when is_integer(tag) and is_binary(worker_id),
    do: "#{@system_prefix}/materializers/#{tag}/#{worker_id}"

  @doc """
  Prefix covering one tag's members.

  The family's standard triple mirrors FDB's (`SystemData.cpp`):
  `materializers_prefix/0` is `serverKeysRange`, `materializer_key/2` is
  `serverKeysKey`, and this is `serverKeysPrefixFor` — the scan that
  answers "who serves this shard" without decoding the whole family.
  """
  @spec materializer_tag_prefix(Bedrock.range_tag()) :: Bedrock.key()
  def materializer_tag_prefix(tag) when is_integer(tag), do: "#{@system_prefix}/materializers/#{tag}/"

  @doc """
  The PRE-q67.21.9 single-valued membership key: `materializers/<tag>`.

  Built only so recovery can CLEAR it once the members it held have been
  rewritten in the set-valued shape. Nothing writes this key any more.
  """
  @spec legacy_materializer_key(Bedrock.range_tag()) :: Bedrock.key()
  def legacy_materializer_key(tag) when is_integer(tag), do: "#{@system_prefix}/materializers/#{tag}"

  @doc "Prefix covering every materializer membership entry"
  @spec materializers_prefix() :: Bedrock.key()
  def materializers_prefix, do: "#{@system_prefix}/materializers/"

  @doc """
  The reserved worker id the distributor's placeholder registers under.

  A keyspace-level convention, not a distributor detail: routing reads it
  to prefer real coverage over parking, so it belongs where the family's
  semantics are defined rather than behind a control-plane module.
  """
  @spec placeholder_worker_id() :: Worker.id()
  def placeholder_worker_id, do: "distributor-placeholder"

  @doc """
  Parses a system key into its family. Unknown system keys parse as
  `:unknown` (forward compatibility); non-system keys as `:error`.
  """
  @spec parse_key(Bedrock.key()) ::
          {:shard_key, Bedrock.key()}
          | {:materializer_key, Bedrock.range_tag(), Worker.id()}
          | {:legacy_materializer_key, Bedrock.range_tag()}
          | {:distributor_lock, :owner | :write}
          | :unknown
          | :error
  def parse_key(<<@system_prefix, "/distributor_lock/owner">>), do: {:distributor_lock, :owner}
  def parse_key(<<@system_prefix, "/distributor_lock/write">>), do: {:distributor_lock, :write}
  def parse_key(<<@system_prefix, "/shard_keys/", rest::binary>>), do: {:shard_key, rest}

  def parse_key(<<@system_prefix, "/materializers/", rest::binary>>) do
    case String.split(rest, "/", parts: 2) do
      [tag_string, worker_id] when worker_id != "" -> materializer_key_or_unknown(tag_string, worker_id)
      # Pre-q67.21.9: the family was single-valued, materializers/<tag>
      # -> packed {worker_id, node}. Clusters written by that code still
      # hold these, so they are recognized rather than read as garbage
      # (bedrock-q67.21.21).
      [tag_string] -> legacy_materializer_key_or_unknown(tag_string)
      _ -> :unknown
    end
  end

  def parse_key(<<@system_prefix, _rest::binary>>), do: :unknown
  def parse_key(_key), do: :error

  defp materializer_key_or_unknown(tag_string, worker_id) do
    case Integer.parse(tag_string) do
      {tag, ""} -> {:materializer_key, tag, worker_id}
      _ -> :unknown
    end
  end

  defp legacy_materializer_key_or_unknown(tag_string) do
    case Integer.parse(tag_string) do
      {tag, ""} -> {:legacy_materializer_key, tag}
      _ -> :unknown
    end
  end
end
