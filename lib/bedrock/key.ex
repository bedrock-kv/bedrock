defmodule Bedrock.Key do
  @moduledoc """
  Utilities for working with binary keys in Bedrock.

  Binary keys in Bedrock are ordered lexicographically, and this module provides
  utilities for key manipulation, ordering operations, and tuple packing/unpacking.

  ## Tuple Packing

  This module provides tuple packing and unpacking functionality that allows
  complex data structures (tuples, integers, floats, binaries, nil) to be
  encoded as binary keys that maintain lexicographic ordering.
  """

  import Bitwise

  @type t :: binary()

  @doc """
  Returns the next possible key after the given key in lexicographic order.

  This is useful for creating exclusive upper bounds in range operations.
  For example, if you want all keys that start with "prefix", you would
  use the range from "prefix" to `key_after("prefix")`.

  ## Examples

      iex> Bedrock.Key.key_after("abc")
      "abc\\0"

      iex> Bedrock.Key.key_after("")
      "\\0"

  """
  @spec key_after(Bedrock.key()) :: binary()
  def key_after(key) when is_binary(key), do: key <> <<0>>

  def to_range(key) when is_binary(key), do: {key, key_after(key)}

  @doc """
  Returns the lexicographically next key after the given key by incrementing
  the last non-0xFF byte.

  This is useful for creating tight upper bounds in prefix ranges. Unlike
  `key_after/1` which appends a null byte, `strinc/1` creates the minimal
  lexicographically next key.

  ## Examples

      iex> Bedrock.Key.strinc("abc")
      "abd"

      iex> Bedrock.Key.strinc(<<0, 1, 2>>)
      <<0, 1, 3>>
      iex> Bedrock.Key.strinc("hello")
      "hellp"

  ## Errors

  Raises an `ArgumentError` if the key contains only 0xFF bytes.

      iex> Bedrock.Key.strinc(<<0xFF, 0xFF>>)
      ** (ArgumentError) Key must contain at least one byte not equal to 0xFF

  """
  @spec strinc(binary()) :: binary()
  def strinc(key) when is_binary(key) do
    prefix = rstrip_ff(key)
    prefix_len = byte_size(prefix)
    head = binary_part(prefix, 0, prefix_len - 1)
    tail = :binary.at(prefix, prefix_len - 1)
    head <> <<tail + 1>>
  end

  defp rstrip_ff(<<>>), do: raise(ArgumentError, "Key must contain at least one byte not equal to 0xFF")

  defp rstrip_ff(key) do
    key_len = byte_size(key)

    case :binary.at(key, key_len - 1) do
      0xFF -> key |> binary_part(0, key_len - 1) |> rstrip_ff()
      _ -> key
    end
  end

  # Packing/Unpacking Functions

  @doc """
  Packs a value (nil, tuple, list, number, or binary) into a binary key.

  Lists and tuples have separate encodings to preserve type identity. The packed format
  maintains lexicographic ordering, allowing for efficient range queries over
  structured data.

  ## Examples

      iex> Bedrock.Key.pack({"users", 123})
      <<5, 1, 117, 115, 101, 114, 115, 0, 21, 123, 0>>

      iex> Bedrock.Key.pack(["users", 123])
      <<6, 1, 117, 115, 101, 114, 115, 0, 21, 123, 0>>

      iex> Bedrock.Key.pack(42)
      <<21, 42>>

  """
  @spec pack(nil | tuple() | list() | number() | Bedrock.key()) :: Bedrock.key()
  def pack(unpacked), do: unpacked |> to_iolist() |> :erlang.iolist_to_binary()

  @doc """
  Unpacks a binary key back into its original value.

  ## Examples

      iex> key = Bedrock.Key.pack({"users", 123})
      iex> Bedrock.Key.unpack(key)
      {"users", 123}

  """
  @spec unpack(Bedrock.key()) :: nil | tuple() | list() | number() | Bedrock.key()
  def unpack(packed) do
    packed
    |> unpack_value()
    |> case do
      {value, <<>>} -> value
      {_value, rest} -> raise ArgumentError, "Extra data after key: #{Base.encode16(rest)}"
    end
  end

  @doc """
  Converts a value to an iolist representation for packing.
  """
  def to_iolist(unpacked, tail \\ []), do: unpacked |> pack_value(tail) |> :lists.reverse()

  # Type Tags
  @stop_marker 0x00
  @bytes_tag 0x01
  @nested_tuple_tag 0x05
  @nested_list_tag 0x06
  @int_zero_tag 0x14
  @float_tag 0x21

  # Encoding Functions

  defp pack_value(nil, acc), do: [<<0x00, 0xFF>> | acc]

  defp pack_value(binary, acc) when is_binary(binary),
    do: [@stop_marker | pack_binary(binary, 0, binary, [<<@bytes_tag>> | acc])]

  defp pack_value(integer, acc) when is_integer(integer), do: pack_integer(integer, acc)
  defp pack_value(float, acc) when is_float(float), do: pack_float(float, acc)

  defp pack_value(list, acc) when is_list(list),
    do: [@stop_marker | pack_list_elements(list, [<<@nested_list_tag>> | acc])]

  defp pack_value(tuple, acc) when is_tuple(tuple),
    do: [@stop_marker | tuple |> Tuple.to_list() |> pack_list_elements([<<@nested_tuple_tag>> | acc])]

  defp pack_value(unsupported, _), do: raise(ArgumentError, "Unsupported data type: #{inspect(unsupported)}")

  defp pack_list_elements([], acc), do: acc
  defp pack_list_elements([element | rest], acc), do: pack_list_elements(rest, pack_value(element, acc))

  defp pack_binary(<<0x00, rest::binary>>, 0, _original, acc), do: pack_binary(rest, 0, rest, [<<0x00, 0xFF>> | acc])

  defp pack_binary(<<0x00, rest::binary>>, offset, original, acc),
    do: pack_binary(rest, 0, rest, [<<0x00, 0xFF>>, binary_part(original, 0, offset) | acc])

  defp pack_binary(<<_byte, rest::binary>>, offset, original, acc), do: pack_binary(rest, offset + 1, original, acc)
  defp pack_binary(_, 0, original, acc), do: [original | acc]
  defp pack_binary(_, offset, original, acc), do: [binary_part(original, 0, offset) | acc]

  defp pack_integer(0, acc), do: [<<@int_zero_tag>> | acc]
  defp pack_integer(i, acc) when i > 0, do: pack_pos_integer(i, acc)
  defp pack_integer(i, acc) when i < 0, do: pack_neg_integer(-i, acc)

  defp pack_pos_integer(i, acc) when i <= 0xFF, do: [<<i>>, 0x15 | acc]
  defp pack_pos_integer(i, acc) when i <= 0xFFFF, do: [<<i::16>>, 0x16 | acc]
  defp pack_pos_integer(i, acc) when i <= 0xFFFFFF, do: [<<i::24>>, 0x17 | acc]
  defp pack_pos_integer(i, acc) when i <= 0xFFFFFFFF, do: [<<i::32>>, 0x18 | acc]
  defp pack_pos_integer(i, acc) when i <= 0xFFFFFFFFFF, do: [<<i::40>>, 0x19 | acc]
  defp pack_pos_integer(i, acc) when i <= 0xFFFFFFFFFFFF, do: [<<i::48>>, 0x1A | acc]
  defp pack_pos_integer(i, acc) when i <= 0xFFFFFFFFFFFFFF, do: [<<i::56>>, 0x1B | acc]
  defp pack_pos_integer(i, acc) when i <= 0xFFFFFFFFFFFFFFFF, do: [<<i::64>>, 0x1C | acc]

  defp pack_neg_integer(i, acc) when i <= 0xFF, do: [<<bnot(i)>>, 0x13 | acc]
  defp pack_neg_integer(i, acc) when i <= 0xFFFF, do: [<<bnot(i)::16>>, 0x12 | acc]
  defp pack_neg_integer(i, acc) when i <= 0xFFFFFF, do: [<<bnot(i)::24>>, 0x11 | acc]
  defp pack_neg_integer(i, acc) when i <= 0xFFFFFFFF, do: [<<bnot(i)::32>>, 0x10 | acc]
  defp pack_neg_integer(i, acc) when i <= 0xFFFFFFFFFF, do: [<<bnot(i)::40>>, 0x0F | acc]
  defp pack_neg_integer(i, acc) when i <= 0xFFFFFFFFFFFF, do: [<<bnot(i)::48>>, 0x0E | acc]
  defp pack_neg_integer(i, acc) when i <= 0xFFFFFFFFFFFFFF, do: [<<bnot(i)::56>>, 0x0D | acc]
  defp pack_neg_integer(i, acc) when i <= 0xFFFFFFFFFFFFFFFF, do: [<<bnot(i)::64>>, 0x0C | acc]

  defp pack_float(float, acc), do: [<<@float_tag, float::float-size(64)>> | acc]

  # Decoding Functions

  defp unpack_value(<<0x00, 0xFF, rest::binary>>), do: {nil, rest}
  defp unpack_value(<<@bytes_tag, rest::binary>>), do: unpack_binary(rest)
  defp unpack_value(<<@int_zero_tag, rest::binary>>), do: {0, rest}

  # Positive integers - direct pattern matching by size
  defp unpack_value(<<0x15, integer::8, rest::binary>>), do: {integer, rest}
  defp unpack_value(<<0x16, integer::16, rest::binary>>), do: {integer, rest}
  defp unpack_value(<<0x17, integer::24, rest::binary>>), do: {integer, rest}
  defp unpack_value(<<0x18, integer::32, rest::binary>>), do: {integer, rest}
  defp unpack_value(<<0x19, integer::40, rest::binary>>), do: {integer, rest}
  defp unpack_value(<<0x1A, integer::48, rest::binary>>), do: {integer, rest}
  defp unpack_value(<<0x1B, integer::56, rest::binary>>), do: {integer, rest}
  defp unpack_value(<<0x1C, integer::64, rest::binary>>), do: {integer, rest}

  # Negative integers - direct complement decoding with proper masking
  defp unpack_value(<<0x13, complement::8, rest::binary>>), do: {-band(bnot(complement), 0xFF), rest}
  defp unpack_value(<<0x12, complement::16, rest::binary>>), do: {-band(bnot(complement), 0xFFFF), rest}
  defp unpack_value(<<0x11, complement::24, rest::binary>>), do: {-band(bnot(complement), 0xFFFFFF), rest}
  defp unpack_value(<<0x10, complement::32, rest::binary>>), do: {-band(bnot(complement), 0xFFFFFFFF), rest}
  defp unpack_value(<<0x0F, complement::40, rest::binary>>), do: {-band(bnot(complement), 0xFFFFFFFFFF), rest}
  defp unpack_value(<<0x0E, complement::48, rest::binary>>), do: {-band(bnot(complement), 0xFFFFFFFFFFFF), rest}
  defp unpack_value(<<0x0D, complement::56, rest::binary>>), do: {-band(bnot(complement), 0xFFFFFFFFFFFFFF), rest}
  defp unpack_value(<<0x0C, complement::64, rest::binary>>), do: {-band(bnot(complement), 0xFFFFFFFFFFFFFFFF), rest}

  defp unpack_value(<<@float_tag, float::float-size(64), rest::binary>>), do: {float, rest}

  defp unpack_value(<<@nested_tuple_tag, rest::binary>>) do
    {elements, rest} = unpack_list_elements(rest, [])
    {List.to_tuple(elements), rest}
  end

  defp unpack_value(<<@nested_list_tag, rest::binary>>), do: unpack_list_elements(rest, [])
  defp unpack_value(<<@stop_marker, rest::binary>>), do: {:stop, rest}
  defp unpack_value(x) when is_binary(x), do: raise(ArgumentError, "Unsupported or malformed data: #{Base.encode16(x)}")
  defp unpack_value(x), do: raise(ArgumentError, "Unsupported or malformed data: #{inspect(x)}")

  defp unpack_binary(binary), do: unpack_binary(binary, 0, binary, [])

  # Found escaped null - emit slice if any, then add null byte
  defp unpack_binary(<<0x00, 0xFF, rest::binary>>, 0, _original, acc),
    do: unpack_binary(rest, 0, rest, [<<0x00>> | acc])

  defp unpack_binary(<<0x00, 0xFF, rest::binary>>, offset, original, acc),
    do: unpack_binary(rest, 0, rest, [<<0x00>>, binary_part(original, 0, offset) | acc])

  # Found terminating null - emit final slice if any and finish
  defp unpack_binary(<<@stop_marker, rest::binary>>, 0, _original, acc),
    do: {acc |> :lists.reverse() |> :erlang.iolist_to_binary(), rest}

  defp unpack_binary(<<@stop_marker, rest::binary>>, offset, original, acc),
    do: {[binary_part(original, 0, offset) | acc] |> :lists.reverse() |> :erlang.iolist_to_binary(), rest}

  # Regular byte - continue scanning
  defp unpack_binary(<<_byte, rest::binary>>, offset, original, acc), do: unpack_binary(rest, offset + 1, original, acc)

  # Unexpected end
  defp unpack_binary(<<>>, _offset, _original, _acc), do: raise(ArgumentError, "Unexpected end of binary data")

  defp unpack_list_elements(binary, acc) do
    case unpack_value(binary) do
      {:stop, rest} -> {Enum.reverse(acc), rest}
      {value, rest} -> unpack_list_elements(rest, [value | acc])
    end
  end
end
