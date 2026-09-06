defmodule Bedrock.SystemKeys.Values do
  @moduledoc """
  Explicit value encodings for the `\\xFF/system` keys Bedrock writes.

  One codec per family, chosen deliberately: FDB tuple encoding for the
  scalar shapes here. Encoders are for trusted writers and raise on invalid
  input; decoders handle durable bytes and never raise. Decoding durable
  bytes must never create atoms.

  The surface is exactly the written families - `shard_keys/` entries
  (the routing boundary map), `materializers/` membership (the node as
  a string; consumers derive the callable `{otp_name, node}` ref from it
  and the worker id in the key, so the no-atoms rule holds through
  decode), and `config/` parameters. Families return here when their
  readers do.
  """

  alias Bedrock.Encoding.Tuple, as: TupleEncoding

  @type decode_error :: {:error, :invalid_encoding | :invalid_type | :unknown_family}

  @doc """
  Encodes a shard key entry: `{tag, start_key}`.

  The key carries the shard's `end_key`; the value carries the tag and
  the range's start key, and readers consume the carried start key
  verbatim - a single entry is self-describing, no adjacency
  reconstruction.
  """
  @spec encode_shard_key_entry(Bedrock.range_tag(), Bedrock.key()) :: binary()
  def encode_shard_key_entry(tag, start_key) when is_integer(tag) and is_binary(start_key),
    do: TupleEncoding.pack({tag, start_key})

  @doc """
  Encodes a materializer membership value: the member's node, a string.

  The worker id lives in the key (`materializers/<tag>/<worker_id>`), so
  the value carries only what a consumer cannot derive: the node. Both
  halves are string-encoded on purpose - never atoms or pids - so
  decoding durable bytes never creates atoms. The worker's OTP name is
  derivable from the id (`cluster.otp_name_for_worker/1`); conversion to
  a callable ref happens at the consumer.
  """
  @spec encode_materializer_node(String.t()) :: binary()
  def encode_materializer_node(node) when is_binary(node), do: TupleEncoding.pack(node)

  @doc """
  Encodes a `config/<name>` value: a positive integer.

  Every member of the family is a count or a duration today, so the
  family has one codec and `decode_for/2` needs no per-name dispatch.
  FDB stores its `\\xff/conf/` values as decimal text and parses them per
  key (`DatabaseConfiguration::setInternal`); we keep the repo's tuple
  encoding, which is self-describing enough that a wrongly-typed value
  fails the decode instead of silently reading as zero.
  """
  @spec encode_config_integer(pos_integer()) :: binary()
  def encode_config_integer(value) when is_integer(value) and value > 0, do: TupleEncoding.pack(value)

  def encode_config_integer(other) do
    raise ArgumentError, "config parameter value must be a positive integer: #{inspect(other)}"
  end

  @doc "Decodes a `config/<name>` value to `{:ok, integer}`."
  @spec decode_config_integer(binary()) :: {:ok, pos_integer()} | decode_error()
  def decode_config_integer(binary), do: safe_unpack(binary, &(is_integer(&1) and &1 > 0))

  @doc "Decodes a shard key entry to `{:ok, {tag, start_key}}`."
  @spec decode_shard_key_entry(binary()) :: {:ok, {Bedrock.range_tag(), Bedrock.key()}} | decode_error()
  def decode_shard_key_entry(binary),
    do: safe_unpack(binary, &match?({tag, start_key} when is_integer(tag) and is_binary(start_key), &1))

  @doc "Decodes a materializer membership value to `{:ok, node}` (string)."
  @spec decode_materializer_node(binary()) :: {:ok, String.t()} | decode_error()
  def decode_materializer_node(binary), do: safe_unpack(binary, &is_binary/1)

  @doc """
  Decodes a value given its parsed system key (from
  `Bedrock.SystemKeys.parse_key/1`).
  """
  @spec decode_for(term(), binary()) :: {:ok, term()} | decode_error()
  def decode_for({:distributor_lock, _which}, value), do: decode_lock_uid(value)
  def decode_for({:shard_key, _end_key}, value), do: decode_shard_key_entry(value)
  def decode_for({:materializer_key, _tag, _worker_id}, value), do: decode_materializer_node(value)
  def decode_for({:config, _name}, value), do: decode_config_integer(value)
  def decode_for(_unknown, _value), do: {:error, :unknown_family}

  defp safe_unpack(binary, valid?) when is_binary(binary) do
    value = TupleEncoding.unpack(binary)

    if valid?.(value) do
      {:ok, value}
    else
      {:error, :invalid_type}
    end
  rescue
    _ -> {:error, :invalid_encoding}
  end

  defp safe_unpack(_not_binary, _valid?), do: {:error, :invalid_encoding}

  @doc """
  Encodes a distributor-lock UID: an opaque 16-byte token stored raw
  (there is nothing to structure; the value's only property is
  freshness-and-equality).
  """
  @spec encode_lock_uid(binary()) :: binary()
  def encode_lock_uid(uid) when is_binary(uid) and byte_size(uid) == 16, do: uid

  def encode_lock_uid(other) do
    raise ArgumentError, "distributor lock UID must be a 16-byte binary: #{inspect(other)}"
  end

  @doc "Decodes a distributor-lock UID; never raises, never creates atoms."
  @spec decode_lock_uid(binary()) :: {:ok, binary()} | decode_error()
  def decode_lock_uid(uid) when is_binary(uid) and byte_size(uid) == 16, do: {:ok, uid}
  def decode_lock_uid(_other), do: {:error, :invalid_encoding}
end
