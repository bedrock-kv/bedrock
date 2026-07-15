defmodule Bedrock.SystemKeys.Values do
  @moduledoc """
  Explicit value encodings for the `\\xFF/system` keyspace.

  Every system-key value family gets a deliberate encoding instead of
  `:erlang.term_to_binary/1`, which is BEAM-only and unsafe to decode from
  durable storage. Encoders are used by trusted writers (the director's
  recovery persistence phase) and raise on invalid input; decoders never
  raise -- they return `{:ok, value}` or `{:error, reason}` so consumers can
  skip malformed values.

  ## Encodings by family

  Simple scalar families use `Bedrock.Encoding.Tuple` (FDB tuple encoding:
  self-describing type tags, language-neutral, order-preserving). The key
  itself identifies the family, so no extra version byte is carried:

    * integers -- epochs, cluster parameters, timestamps, recovery attempt
    * booleans -- cluster policies (encoded as integer 0/1)
    * atoms -- recovery state (encoded as string)
    * ids -- layout id (integer or binary)
    * node lists -- cluster coordinators (encoded as strings)
    * tag lists -- log descriptors (lists of range tags)
    * shard key entries -- `{tag, start_key}` tuples

  Structured families (`config_monolithic`, `layout_services`) hold nested
  maps/tuples with atoms, so they use a versioned explicit term encoding
  (leading version byte, currently `0x01`). The codec supports exactly nil,
  booleans, atoms, binaries, 64-bit integers, floats, tuples, lists, and
  maps; PIDs, refs, and funs are rejected at encode time -- writers must
  sanitize runtime references first.

  FlatBuffer families (`shard/1` -> `Bedrock.SystemKeys.ShardMetadata`,
  `materializer_key/1` -> `Bedrock.SystemKeys.MaterializerList`) are already
  explicitly encoded and are passed through opaque here.

  Atoms are re-created with `String.to_atom/1` on decode. The `\\xFF`
  keyspace is only writable by privileged control-plane components, and the
  atom population (config field names, service kinds, node names) is bounded
  by cluster configuration.
  """

  import Bitwise, only: [<<<: 2]

  alias Bedrock.Encoding.Tuple, as: TupleEncoding

  @structured_version 0x01

  @typedoc "A parsed system key, as returned by `Bedrock.SystemKeys.parse_key/1`"
  @type parsed_key ::
          {:layout_log, String.t()}
          | {:shard_key, String.t()}
          | {:shard, String.t()}
          | {:materializer_key, String.t()}
          | :layout_services
          | :layout_id
          | {:cluster, :coordinators | :epoch}
          | {:cluster_policy, :volunteer_nodes}
          | {:cluster_parameter, atom()}
          | {:recovery, :attempt | :state | :last_completed}
          | :config_monolithic
          | :epoch_legacy
          | :last_recovery_legacy

  @type decode_error :: {:error, :invalid_encoding | :invalid_type | :unsupported_version | :unknown_family}

  # ============================================================================
  # Encoders (trusted writers; raise on invalid input)
  # ============================================================================

  @doc "Encodes an integer (epoch, parameter, timestamp, recovery attempt)."
  @spec encode_integer(integer()) :: binary()
  def encode_integer(i) when is_integer(i), do: TupleEncoding.pack(i)

  @doc "Encodes a boolean policy value as integer 0/1."
  @spec encode_boolean(boolean()) :: binary()
  def encode_boolean(true), do: TupleEncoding.pack(1)
  def encode_boolean(false), do: TupleEncoding.pack(0)

  @doc "Encodes an atom as its string representation."
  @spec encode_atom(atom()) :: binary()
  def encode_atom(a) when is_atom(a), do: TupleEncoding.pack(Atom.to_string(a))

  @doc "Encodes a layout id (integer or binary)."
  @spec encode_id(integer() | binary()) :: binary()
  def encode_id(id) when is_integer(id) or is_binary(id), do: TupleEncoding.pack(id)

  @doc "Encodes a list of node atoms as strings (cluster coordinators)."
  @spec encode_node_list([node()]) :: binary()
  def encode_node_list(nodes) when is_list(nodes), do: TupleEncoding.pack(Enum.map(nodes, &Atom.to_string/1))

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
  range's start key so readers (materializer bootstrap) can reconstruct the
  full shard layout without relying on adjacency.
  """
  @spec encode_shard_key_entry(Bedrock.range_tag(), Bedrock.key()) :: binary()
  def encode_shard_key_entry(tag, start_key) when is_integer(tag) and is_binary(start_key),
    do: TupleEncoding.pack({tag, start_key})

  @doc """
  Encodes a structured term (`config_monolithic`, `layout_services`) with a
  leading version byte.

  Supports nil, booleans, atoms, binaries, 64-bit integers, floats, tuples,
  lists, and maps. Raises `ArgumentError` for anything else (PIDs, refs,
  funs) -- writers must sanitize runtime references first.
  """
  @spec encode_structured(term()) :: binary()
  def encode_structured(term), do: IO.iodata_to_binary([@structured_version | encode_term(term)])

  # ============================================================================
  # Decoders (never raise)
  # ============================================================================

  @doc "Decodes an integer value."
  @spec decode_integer(binary()) :: {:ok, integer()} | decode_error()
  def decode_integer(binary), do: safe_unpack(binary, &is_integer/1)

  @doc "Decodes a boolean policy value."
  @spec decode_boolean(binary()) :: {:ok, boolean()} | decode_error()
  def decode_boolean(binary) do
    case safe_unpack(binary, &(&1 in [0, 1])) do
      {:ok, 1} -> {:ok, true}
      {:ok, 0} -> {:ok, false}
      error -> error
    end
  end

  @doc "Decodes an atom value."
  @spec decode_atom(binary()) :: {:ok, atom()} | decode_error()
  def decode_atom(binary) do
    with {:ok, string} <- safe_unpack(binary, &is_binary/1) do
      {:ok, String.to_atom(string)}
    end
  end

  @doc "Decodes a layout id (integer or binary)."
  @spec decode_id(binary()) :: {:ok, integer() | binary()} | decode_error()
  def decode_id(binary), do: safe_unpack(binary, &(is_integer(&1) or is_binary(&1)))

  @doc "Decodes a list of node atoms."
  @spec decode_node_list(binary()) :: {:ok, [node()]} | decode_error()
  def decode_node_list(binary) do
    with {:ok, strings} <- safe_unpack(binary, &(is_list(&1) and Enum.all?(&1, fn s -> is_binary(s) end))) do
      {:ok, Enum.map(strings, &String.to_atom/1)}
    end
  end

  @doc "Decodes a list of integer range tags."
  @spec decode_tag_list(binary()) :: {:ok, [Bedrock.range_tag()]} | decode_error()
  def decode_tag_list(binary), do: safe_unpack(binary, &(is_list(&1) and Enum.all?(&1, fn t -> is_integer(t) end)))

  @doc "Decodes a shard key entry to `{:ok, {tag, start_key}}`."
  @spec decode_shard_key_entry(binary()) :: {:ok, {Bedrock.range_tag(), Bedrock.key()}} | decode_error()
  def decode_shard_key_entry(binary),
    do: safe_unpack(binary, &match?({tag, start_key} when is_integer(tag) and is_binary(start_key), &1))

  @doc "Decodes a versioned structured term."
  @spec decode_structured(binary()) :: {:ok, term()} | decode_error()
  def decode_structured(<<@structured_version, payload::binary>>) do
    case decode_term(payload) do
      {:ok, term, <<>>} -> {:ok, term}
      _ -> {:error, :invalid_encoding}
    end
  end

  def decode_structured(<<_version, _rest::binary>>), do: {:error, :unsupported_version}
  def decode_structured(_), do: {:error, :invalid_encoding}

  @doc """
  Decodes a value given its parsed system key (from
  `Bedrock.SystemKeys.parse_key/1`).

  FlatBuffer families (`{:shard, tag}` and `{:materializer_key, end_key}`)
  are returned opaque -- their consumers decode them with the appropriate
  schema module.
  """
  @spec decode_for(parsed_key(), binary()) :: {:ok, term()} | decode_error()
  def decode_for({:shard_key, _end_key}, value), do: decode_shard_key_entry(value)
  def decode_for({:shard, _tag}, value), do: {:ok, value}
  def decode_for({:materializer_key, _end_key}, value), do: {:ok, value}
  def decode_for({:layout_log, _log_id}, value), do: decode_tag_list(value)
  def decode_for(:layout_services, value), do: decode_structured(value)
  def decode_for(:layout_id, value), do: decode_id(value)
  def decode_for({:cluster, :coordinators}, value), do: decode_node_list(value)
  def decode_for({:cluster, :epoch}, value), do: decode_integer(value)
  def decode_for({:cluster_policy, _name}, value), do: decode_boolean(value)
  def decode_for({:cluster_parameter, _name}, value), do: decode_integer(value)
  def decode_for({:recovery, :state}, value), do: decode_atom(value)
  def decode_for({:recovery, _name}, value), do: decode_integer(value)
  def decode_for(:config_monolithic, value), do: decode_structured(value)
  def decode_for(:epoch_legacy, value), do: decode_integer(value)
  def decode_for(:last_recovery_legacy, value), do: decode_integer(value)
  # Defensive catch-all: a key family added to SystemKeys.parse_key/1 without
  # a matching decoder must degrade to a skipped value, not a crash.
  def decode_for(_parsed, _value), do: {:error, :unknown_family}

  # ============================================================================
  # Tuple-encoding safety wrapper
  # ============================================================================

  @spec safe_unpack(binary(), (term() -> boolean())) :: {:ok, term()} | decode_error()
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

  # ============================================================================
  # Versioned structured term codec (v1)
  # ============================================================================

  @t_nil 0x00
  @t_true 0x01
  @t_false 0x02
  @t_atom 0x03
  @t_binary 0x04
  @t_int 0x05
  @t_float 0x06
  @t_tuple 0x07
  @t_list 0x08
  @t_map 0x09

  @int_min -(1 <<< 63)
  @int_max (1 <<< 63) - 1

  @spec encode_term(term()) :: iodata()
  defp encode_term(nil), do: <<@t_nil>>
  defp encode_term(true), do: <<@t_true>>
  defp encode_term(false), do: <<@t_false>>

  defp encode_term(atom) when is_atom(atom) do
    string = Atom.to_string(atom)
    [<<@t_atom, byte_size(string)::16>>, string]
  end

  defp encode_term(binary) when is_binary(binary), do: [<<@t_binary, byte_size(binary)::32>>, binary]

  defp encode_term(int) when is_integer(int) and int >= @int_min and int <= @int_max, do: <<@t_int, int::signed-64>>

  defp encode_term(float) when is_float(float), do: <<@t_float, float::float-64>>

  defp encode_term(tuple) when is_tuple(tuple) do
    elements = Tuple.to_list(tuple)
    [<<@t_tuple, length(elements)::16>> | Enum.map(elements, &encode_term/1)]
  end

  defp encode_term(list) when is_list(list), do: [<<@t_list, length(list)::32>> | Enum.map(list, &encode_term/1)]

  defp encode_term(map) when is_map(map) and not is_struct(map) do
    pairs = Enum.sort(Map.to_list(map))
    [<<@t_map, length(pairs)::32>> | Enum.map(pairs, fn {k, v} -> [encode_term(k), encode_term(v)] end)]
  end

  defp encode_term(unsupported) do
    raise ArgumentError, "unsupported term in structured system-key value: #{inspect(unsupported)}"
  end

  @spec decode_term(binary()) :: {:ok, term(), binary()} | :error
  defp decode_term(<<@t_nil, rest::binary>>), do: {:ok, nil, rest}
  defp decode_term(<<@t_true, rest::binary>>), do: {:ok, true, rest}
  defp decode_term(<<@t_false, rest::binary>>), do: {:ok, false, rest}

  defp decode_term(<<@t_atom, size::16, string::binary-size(size), rest::binary>>),
    do: {:ok, String.to_atom(string), rest}

  defp decode_term(<<@t_binary, size::32, binary::binary-size(size), rest::binary>>), do: {:ok, binary, rest}
  defp decode_term(<<@t_int, int::signed-64, rest::binary>>), do: {:ok, int, rest}
  defp decode_term(<<@t_float, float::float-64, rest::binary>>), do: {:ok, float, rest}

  defp decode_term(<<@t_tuple, count::16, rest::binary>>) do
    with {:ok, elements, rest} <- decode_terms(rest, count, []) do
      {:ok, List.to_tuple(elements), rest}
    end
  end

  defp decode_term(<<@t_list, count::32, rest::binary>>), do: decode_terms(rest, count, [])

  defp decode_term(<<@t_map, count::32, rest::binary>>), do: decode_map_pairs(rest, count, %{})

  defp decode_term(_), do: :error

  @spec decode_terms(binary(), non_neg_integer(), [term()]) :: {:ok, [term()], binary()} | :error
  defp decode_terms(rest, 0, acc), do: {:ok, Enum.reverse(acc), rest}

  defp decode_terms(binary, count, acc) do
    with {:ok, term, rest} <- decode_term(binary) do
      decode_terms(rest, count - 1, [term | acc])
    end
  end

  @spec decode_map_pairs(binary(), non_neg_integer(), map()) :: {:ok, map(), binary()} | :error
  defp decode_map_pairs(rest, 0, acc), do: {:ok, acc, rest}

  defp decode_map_pairs(binary, count, acc) do
    with {:ok, key, rest} <- decode_term(binary),
         {:ok, value, rest} <- decode_term(rest) do
      decode_map_pairs(rest, count - 1, Map.put(acc, key, value))
    end
  end
end
