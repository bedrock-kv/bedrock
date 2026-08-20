defmodule Bedrock.SystemKeys.Values do
  @moduledoc """
  Explicit value encodings for the `\\xFF/system` keys Bedrock writes.

  One codec per family, chosen deliberately: FDB tuple encoding for the
  scalar shapes here. Encoders are for trusted writers and raise on invalid
  input; decoders handle durable bytes and never raise. Decoding durable
  bytes must never create atoms.

  The surface is exactly the families with readers - `shard_keys/` entries
  (the routing boundary map) and `layout/logs/` tag lists. Families return
  here when their readers do.
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

  The key carries the shard's `end_key`; the value carries the tag and the
  range's start key. (Readers currently rebuild start keys from adjacency -
  the explicit start key exists so a future reader of a single entry does
  not have to.)
  """
  @spec encode_shard_key_entry(Bedrock.range_tag(), Bedrock.key()) :: binary()
  def encode_shard_key_entry(tag, start_key) when is_integer(tag) and is_binary(start_key),
    do: TupleEncoding.pack({tag, start_key})

  @doc "Decodes a list of integer range tags."
  @spec decode_tag_list(binary()) :: {:ok, [Bedrock.range_tag()]} | decode_error()
  def decode_tag_list(binary), do: safe_unpack(binary, &(is_list(&1) and Enum.all?(&1, fn t -> is_integer(t) end)))

  @doc "Decodes a shard key entry to `{:ok, {tag, start_key}}`."
  @spec decode_shard_key_entry(binary()) :: {:ok, {Bedrock.range_tag(), Bedrock.key()}} | decode_error()
  def decode_shard_key_entry(binary),
    do: safe_unpack(binary, &match?({tag, start_key} when is_integer(tag) and is_binary(start_key), &1))

  @doc """
  Decodes a value given its parsed system key (from
  `Bedrock.SystemKeys.parse_key/1`).
  """
  @spec decode_for(term(), binary()) :: {:ok, term()} | decode_error()
  def decode_for({:shard_key, _end_key}, value), do: decode_shard_key_entry(value)
  def decode_for({:layout_log, _log_id}, value), do: decode_tag_list(value)
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
end
