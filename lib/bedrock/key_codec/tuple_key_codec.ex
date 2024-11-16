defmodule Bedrock.KeyCodec.TupleKeyCodec do
  # Public API

  def encode_key(key) when is_tuple(key), do: {:ok, encode_value(key)}
  def encode_key(key) when is_number(key), do: {:ok, encode_value(key)}
  def encode_key(key) when is_binary(key), do: {:ok, encode_value(key)}
  def encode_key(_), do: {:error, :invalid_key}

  def decode_key(encoded_key) do
    {key, <<>>} = decode_value(encoded_key)
    {:ok, key}
  end

  # Type Tags
  @bytes_tag 0x01
  @nested_tuple_tag 0x05
  @int_zero_tag 0x14
  @float_tag 0x21

  # Stop Marker
  @stop_marker <<0x00>>

  # Encoding Functions

  defp encode_value(nil), do: <<0x00, 0xFF>>

  defp encode_value(binary) when is_binary(binary) do
    <<@bytes_tag>> <> escape_null_bytes(binary) <> @stop_marker
  end

  defp encode_value(integer) when is_integer(integer), do: encode_integer(integer)

  defp encode_value(float) when is_float(float), do: encode_float(float)

  defp encode_value(tuple) when is_tuple(tuple),
    do:
      <<@nested_tuple_tag>> <>
        Enum.map_join(Tuple.to_list(tuple), &encode_value/1) <>
        @stop_marker

  defp encode_value(_), do: raise(ArgumentError, "Unsupported data type")

  defp escape_null_bytes(data) do
    data
    |> :binary.bin_to_list()
    |> Enum.flat_map(fn
      0x00 -> [0x00, 0xFF]
      byte -> [byte]
    end)
    |> :binary.list_to_bin()
  end

  defp encode_integer(0), do: <<@int_zero_tag>>

  defp encode_integer(integer) when integer > 0 do
    bytes = :binary.encode_unsigned(integer)
    length = byte_size(bytes)
    prefix = @int_zero_tag + length
    <<prefix>> <> bytes
  end

  defp encode_integer(integer) when integer < 0 do
    abs_bytes = :binary.encode_unsigned(-integer)
    length = byte_size(abs_bytes)
    prefix = @int_zero_tag - length
    complemented = complement_bytes(abs_bytes)
    <<prefix>> <> complemented
  end

  defp complement_bytes(bytes) when is_binary(bytes) do
    bytes
    |> :binary.bin_to_list()
    |> Enum.map(&Bitwise.bxor(&1, 0xFF))
    |> :binary.list_to_bin()
  end

  defp encode_float(float), do: <<@float_tag, float::float-size(64)>>

  # Decoding Functions

  defp decode_value(<<0x00, 0xFF, rest::binary>>), do: {nil, rest}

  defp decode_value(<<@bytes_tag, rest::binary>>), do: extract_escaped_data(rest)

  defp decode_value(<<@int_zero_tag, rest::binary>>), do: {0, rest}

  defp decode_value(<<tag, rest::binary>>)
       when tag >= @int_zero_tag and tag < @int_zero_tag + 8 do
    <<integer::size(tag - @int_zero_tag)-unit(8), rest::binary>> = rest
    {integer, rest}
  end

  defp decode_value(<<tag, rest::binary>>) when tag > @int_zero_tag - 8 and tag < @int_zero_tag do
    <<complemented::binary-size(@int_zero_tag - tag), rest::binary>> = rest
    integer = :binary.decode_unsigned(complement_bytes(complemented))
    {-integer, rest}
  end

  defp decode_value(<<@float_tag, float::float-size(64), rest::binary>>), do: {float, rest}

  defp decode_value(<<@nested_tuple_tag, rest::binary>>) do
    {elements, rest} = decode_elements(rest, [])
    {List.to_tuple(elements), rest}
  end

  defp decode_value(<<@stop_marker, rest::binary>>), do: {:stop, rest}

  defp decode_value(x),
    do: raise(ArgumentError, "Unsupported or malformed data: #{Base.encode16(x)}")

  defp extract_escaped_data(binary, acc \\ "") do
    case binary do
      <<0x00, 0xFF, rest::binary>> -> extract_escaped_data(rest, acc <> <<0x00>>)
      <<0x00, rest::binary>> -> {acc, rest}
      <<byte, rest::binary>> -> extract_escaped_data(rest, acc <> <<byte>>)
      <<>> -> raise ArgumentError, "Unexpected end of binary data"
    end
  end

  defp decode_elements(binary, acc) do
    case decode_value(binary) do
      {:stop, rest} -> {Enum.reverse(acc), rest}
      {value, rest} -> decode_elements(rest, [value | acc])
    end
  end
end
