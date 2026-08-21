defmodule Bedrock.SystemKeys.Values do
  @moduledoc """
  Explicit value encodings for the `\\xFF/system` keys Bedrock writes.

  One codec per family, chosen deliberately: FDB tuple encoding for the
  scalar shapes here. Encoders are for trusted writers and raise on invalid
  input; decoders handle durable bytes and never raise. Decoding durable
  bytes must never create atoms.

  The surface is exactly the written families - `shard_keys/` entries
  (the routing boundary map), `layout/logs/` tag lists (no code reader by
  design: kept durable for introspection, see `Bedrock.SystemKeys`), and
  `materializers/` refs (worker id and node as strings; consumers derive
  the callable `{otp_name, node}` ref, so the no-atoms rule holds through
  decode). Families return here when their readers do.
  """

  alias Bedrock.Encoding.Tuple, as: TupleEncoding

  @type decode_error :: {:error, :invalid_encoding | :invalid_type | :unknown_family}

  @doc "Encodes a list of integer range tags (log descriptor)."
  @spec encode_tag_list([Bedrock.range_tag()]) :: binary()
  def encode_tag_list(tags) when is_list(tags) do
    if Enum.all?(tags, &is_integer/1) do
      TupleEncoding.pack(tags)
    else
      raise ArgumentError, "tag list must contain only integers: #{inspect(tags)}"
    end
  end

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
  Encodes a materializer ref: `{worker_id, node}`, both strings.

  Refs are string-encoded on purpose - never atoms or pids - so decoding
  durable bytes never creates atoms. The worker's OTP name is derivable
  from the id (`cluster.otp_name_for_worker/1`); conversion to a callable
  ref happens at the consumer.
  """
  @spec encode_materializer_ref(String.t(), String.t()) :: binary()
  def encode_materializer_ref(worker_id, node) when is_binary(worker_id) and is_binary(node),
    do: TupleEncoding.pack({worker_id, node})

  @doc "Decodes a list of integer range tags."
  @spec decode_tag_list(binary()) :: {:ok, [Bedrock.range_tag()]} | decode_error()
  def decode_tag_list(binary), do: safe_unpack(binary, &(is_list(&1) and Enum.all?(&1, fn t -> is_integer(t) end)))

  @doc "Decodes a shard key entry to `{:ok, {tag, start_key}}`."
  @spec decode_shard_key_entry(binary()) :: {:ok, {Bedrock.range_tag(), Bedrock.key()}} | decode_error()
  def decode_shard_key_entry(binary),
    do: safe_unpack(binary, &match?({tag, start_key} when is_integer(tag) and is_binary(start_key), &1))

  @doc "Decodes a materializer ref to `{:ok, {worker_id, node}}` (strings)."
  @spec decode_materializer_ref(binary()) :: {:ok, {String.t(), String.t()}} | decode_error()
  def decode_materializer_ref(binary),
    do: safe_unpack(binary, &match?({worker_id, node} when is_binary(worker_id) and is_binary(node), &1))

  @doc """
  Decodes a value given its parsed system key (from
  `Bedrock.SystemKeys.parse_key/1`).
  """
  @spec decode_for(term(), binary()) :: {:ok, term()} | decode_error()
  def decode_for({:distributor_lock, _which}, value), do: decode_lock_uid(value)
  def decode_for({:shard_key, _end_key}, value), do: decode_shard_key_entry(value)
  def decode_for({:layout_log, _log_id}, value), do: decode_tag_list(value)
  def decode_for({:materializer_key, _tag}, value), do: decode_materializer_ref(value)
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
