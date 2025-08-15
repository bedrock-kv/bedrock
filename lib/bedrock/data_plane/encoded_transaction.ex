defmodule Bedrock.DataPlane.EncodedTransaction do
  alias Bedrock.DataPlane.Version

  @moduledoc """
  This module, `Bedrock.DataPlane.EncodedTransaction`, provides functions to
  encode and decode `Bedrock.DataPlane.Log.Transaction.t()` comprised of a
  version and a map of key-value pairs into a binary format with embedded
  versioning and CRC32 checksums for integrity checking.

  The main functionalities include:

  - Encoding transactions into a binary format suitable for transmission or
    storage.
  - Decoding the binary format back into the original transaction structure.

  - Transforming transactions by filtering or excluding data based on specified
    key ranges.

  These operations are designed to facilitate reliable and efficient handling of
  transactions in distributed systems.

  ## Data Structure

  An encoded transaction binary has the following structure:

  - 8-byte version (big endian)
  - 4-byte size of the following key-value frames (big endian)
  - key-value frames:
    - 4-byte size of the frame (big endian)
    - 2-byte size of the key (big endian)
    - key
    - value
  - 4-byte CRC32 checksum of the key-value frames (big endian)

  Key-value frames are sorted by key in ascending order for faster processing.
  """

  alias Bedrock.DataPlane.Log.Transaction

  @type t :: binary()

  @spec version(t()) :: Bedrock.version()
  def version(<<version::binary-size(8), _::binary>>), do: Version.from_bytes(version)

  @spec keys(t()) :: [binary()]
  def keys(
        <<_version::unsigned-big-64, size_in_bytes::unsigned-big-32,
          payload::binary-size(size_in_bytes), _::unsigned-big-32>>
      ) do
    <<key_section_size::unsigned-big-32, key_section::binary-size(key_section_size),
      _value_section::binary>> = payload

    decode_section(key_section)
  end

  @spec key_count(t()) :: non_neg_integer()
  def key_count(
        <<_version::unsigned-big-64, size_in_bytes::unsigned-big-32,
          payload::binary-size(size_in_bytes), _::unsigned-big-32>>
      ) do
    count_keys_in_payload(payload)
  end

  defp count_keys_in_payload(payload) do
    <<key_section_size::unsigned-big-32, key_section::binary-size(key_section_size),
      _value_section::binary>> = payload

    count_items_in_section(key_section, 0)
  end

  defp count_items_in_section(<<>>, count), do: count

  defp count_items_in_section(
         <<size::unsigned-big-16, _item::binary-size(size), rest::binary>>,
         count
       ) do
    count_items_in_section(rest, count + 1)
  end

  @spec validate(binary()) :: {:ok, t()} | {:error, :crc32_mismatch} | {:error, :invalid}
  def validate(
        <<_version::unsigned-big-64, size_in_bytes::unsigned-big-32,
          payload::binary-size(size_in_bytes), crc32::unsigned-big-32>> = transaction
      ) do
    if crc32 != :erlang.crc32(payload) do
      {:error, :crc32_mismatch}
    else
      {:ok, transaction}
    end
  end

  @spec validate(binary()) :: {:ok, t()} | {:error, :crc32_mismatch} | {:error, :invalid}
  def validate(_), do: {:error, :invalid}

  @doc """
  Encodes a transaction tuple into a binary format that includes the version
  and a sorted list of key-value frames. The encoding process includes
  calculating the CRC32 checksum for integrity verification of the encoded data.

  ## Parameters
    - `transaction` ({version, writes}): A tuple containing the version and
      a map of key-value binary pairs to be encoded, or a list of pre-sorted
      key-value pairs. All keys and values are expected to be binaries.

  ## Returns
    - `t()`: An opaque binary that represents the encoded transaction.
  """
  @spec encode({Bedrock.version(), %{Bedrock.key() => Bedrock.value()}}) :: t()
  def encode({version, writes}) when is_map(writes) do
    {version, writes |> Enum.sort()}
    |> iodata_encode()
    |> to_binary()
  end

  @spec encode({Bedrock.version(), [{Bedrock.key(), Bedrock.value()}]}) :: t()
  def encode({version, presorted_pairs}) when is_list(presorted_pairs) do
    {version, presorted_pairs}
    |> iodata_encode()
    |> to_binary()
  end

  @spec iodata_encode({Bedrock.version(), [{binary(), binary()}]}) :: iodata()
  def iodata_encode({version, sorted_pairs}) do
    {key_frames, value_frames, key_section_size, value_section_size} =
      encode_columnar_frames_from_pairs(sorted_pairs)

    payload_iodata = [<<key_section_size::unsigned-big-32>>, key_frames, value_frames]
    total_payload_size = 4 + key_section_size + value_section_size

    wrap_with_version_and_crc32(payload_iodata, version, total_payload_size)
  end

  defp encode_columnar_frames_from_pairs(pairs),
    do: encode_columnar_frames_from_pairs_acc(pairs, [], [], 0, 0)

  defp encode_columnar_frames_from_pairs_acc(
         [],
         key_acc,
         value_acc,
         key_section_size,
         value_section_size
       ) do
    {:lists.reverse(key_acc), :lists.reverse(value_acc), key_section_size, value_section_size}
  end

  defp encode_columnar_frames_from_pairs_acc(
         [{key, value} | pairs],
         key_acc,
         value_acc,
         key_section_size,
         value_section_size
       ) do
    key_size = byte_size(key)
    value_size = byte_size(value)

    encode_columnar_frames_from_pairs_acc(
      pairs,
      [key, <<key_size::unsigned-big-16>> | key_acc],
      [value, <<value_size::unsigned-big-32>> | value_acc],
      key_section_size + 2 + key_size,
      value_section_size + 4 + value_size
    )
  end

  @doc """
  Decodes a binary transaction back into its original form as a version and a
  map of key-value pairs.

  The binary transaction is expected to be formatted with a CRC32 checksum for
  integrity verification. In case of a mismatch in CRC32, an error is returned.

  ## Parameters
    - `transaction` (t()): The encoded binary transaction to be decoded.

  ## Returns
    - `{:ok, {version, writes}}`: On successful decoding, returns a tuple
      containing the version and the decoded key-value map.
    - `{:error, :crc32_mismatch}`: If the CRC32 of the transaction payload
      does not match, indicating the transaction may be corrupted.
  """
  @spec decode(t()) ::
          {:ok, Transaction.t()}
          | {:error, :crc32_mismatch}
          | {:error, :invalid_binary_format}
  def decode(
        <<version::binary-size(8), size_in_bytes::unsigned-big-32,
          payload::binary-size(size_in_bytes), crc32::unsigned-big-32>>
      ) do
    if crc32 != :erlang.crc32(payload) do
      {:error, :crc32_mismatch}
    else
      {:ok, {version, decode_columnar_sections(payload)}}
    end
  rescue
    FunctionClauseError ->
      {:error, :invalid_binary_format}
  end

  # Catch-all for invalid binary format
  def decode(_), do: {:error, :invalid_binary_format}

  @spec decode!(t()) :: Transaction.t()
  def decode!(encoded_transaction) do
    case decode(encoded_transaction) do
      {:ok, transaction} ->
        transaction

      {:error, :crc32_mismatch} ->
        raise("Transaction decode failed: CRC32 checksum mismatch indicates corruption")

      {:error, :invalid_binary_format} ->
        raise(
          "Transaction decode failed: invalid binary format, expected encoded transaction structure"
        )
    end
  end

  defp decode_columnar_sections(payload) do
    <<key_section_size::unsigned-big-32, key_section::binary-size(key_section_size),
      value_section::binary>> = payload

    keys = decode_section_16bit(key_section)
    values = decode_section_32bit(value_section)

    Enum.zip(keys, values) |> Map.new()
  end

  defp decode_section_16bit(data), do: decode_section_16bit_acc(data, [])

  defp decode_section_16bit_acc(<<>>, acc), do: Enum.reverse(acc)

  defp decode_section_16bit_acc(
         <<size::unsigned-big-16, item::binary-size(size), rest::binary>>,
         acc
       ) do
    decode_section_16bit_acc(rest, [item | acc])
  end

  defp decode_section_32bit(data), do: decode_section_32bit_acc(data, [])

  defp decode_section_32bit_acc(<<>>, acc), do: Enum.reverse(acc)

  defp decode_section_32bit_acc(
         <<size::unsigned-big-32, item::binary-size(size), rest::binary>>,
         acc
       ) do
    decode_section_32bit_acc(rest, [item | acc])
  end

  defp decode_section(data), do: decode_section_16bit(data)

  @doc """
  Filters keys and values from a transaction that are outside the specified key range.
  """
  @spec transform_by_removing_keys_outside_of_range(t(), Bedrock.key_range()) :: t()
  def transform_by_removing_keys_outside_of_range(t, key_range) do
    iodata_transform_by_removing_keys_outside_of_range(t, key_range)
    |> to_binary()
  end

  @spec iodata_transform_by_removing_keys_outside_of_range(t(), Bedrock.key_range()) :: iodata()
  def iodata_transform_by_removing_keys_outside_of_range(
        <<version::binary-size(8), n_bytes::unsigned-big-32, payload::binary-size(n_bytes),
          _::unsigned-big-32>> = original_transaction,
        {min_key, max_key_ex}
      ) do
    <<key_section_size::unsigned-big-32, key_section::binary-size(key_section_size),
      value_section::binary>> = payload

    keys = decode_section(key_section)
    values = decode_section(value_section)

    original_pairs = Enum.zip(keys, values)

    filtered_pairs =
      original_pairs
      |> Enum.filter(fn {key, _value} ->
        key >= min_key and (max_key_ex == :end or key < max_key_ex)
      end)

    cond do
      # All keys are in range - return original
      length(filtered_pairs) == length(original_pairs) ->
        original_transaction

      # No keys in range - return empty transaction
      Enum.empty?(filtered_pairs) ->
        encode_empty_transaction(version)

      # Some keys in range - rebuild columnar structure
      true ->
        {key_frames, value_frames, key_section_size, value_section_size} =
          encode_columnar_frames_from_pairs(filtered_pairs)

        payload_iodata = [<<key_section_size::unsigned-big-32>>, key_frames, value_frames]
        total_payload_size = 4 + key_section_size + value_section_size

        wrap_with_version_and_crc32(
          payload_iodata,
          Version.from_bytes(version),
          total_payload_size
        )
    end
  end

  @doc """
  Transforms an encoded transaction by excluding all values, leaving only keys.
  """
  @spec transform_by_excluding_values(t()) :: t()
  def transform_by_excluding_values(t) do
    t
    |> iodata_transform_by_excluding_values()
    |> to_binary()
  end

  @spec iodata_transform_by_excluding_values(t()) :: iodata()
  def iodata_transform_by_excluding_values(
        <<version::binary-size(8), n_bytes::unsigned-big-32, payload::binary-size(n_bytes),
          _::unsigned-big-32>>
      ) do
    <<key_section_size::unsigned-big-32, key_section::binary-size(key_section_size),
      _value_section::binary>> = payload

    keys = decode_section(key_section)
    empty_pairs = Enum.map(keys, fn key -> {key, <<>>} end)

    {key_frames, value_frames, key_section_size, value_section_size} =
      encode_columnar_frames_from_pairs(empty_pairs)

    payload_iodata = [<<key_section_size::unsigned-big-32>>, key_frames, value_frames]
    total_payload_size = 4 + key_section_size + value_section_size

    wrap_with_version_and_crc32(payload_iodata, Version.from_bytes(version), total_payload_size)
  end

  defp wrap_with_version_and_crc32(kv_frames, version, payload_size),
    do: [
      [version, <<payload_size::unsigned-big-32>>],
      kv_frames,
      <<:erlang.crc32(kv_frames)::unsigned-big-32>>
    ]

  defp encode_empty_transaction(version) do
    empty_payload = <<0::unsigned-big-32>>

    [
      version,
      <<4::unsigned-big-32>>,
      empty_payload,
      <<:erlang.crc32(empty_payload)::unsigned-big-32>>
    ]
  end

  defp to_binary(iodata), do: IO.iodata_to_binary(iodata)
end
