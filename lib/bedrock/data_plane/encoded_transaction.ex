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

  @spec key_count(t()) :: non_neg_integer()
  def key_count(
        <<_version::unsigned-big-64, size_in_bytes::unsigned-big-32,
          payload::binary-size(size_in_bytes), _::unsigned-big-32>>
      ) do
    count_keys_in_payload(payload, 0)
  end

  defp count_keys_in_payload(<<>>, count), do: count

  defp count_keys_in_payload(
         <<n_bytes::unsigned-big-32, n_key_bytes::unsigned-big-16, _key::binary-size(n_key_bytes),
           _value::binary-size(n_bytes - n_key_bytes - 2), remainder::binary>>,
         count
       ) do
    count_keys_in_payload(remainder, count + 1)
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
      a map of key-value binary pairs to be encoded. All keys and values are
      expected to be binaries.

  ## Returns
    - `t()`: An opaque binary that represents the encoded transaction.
  """
  @spec encode(Transaction.t()) :: t()
  def encode(transaction) do
    transaction
    |> iodata_encode()
    |> IO.iodata_to_binary()
  end

  @spec iodata_encode(Transaction.t()) :: iodata()
  def iodata_encode({version, writes}) do
    writes
    |> Map.keys()
    |> Enum.sort()
    |> encode_key_value_frames(writes)
    |> wrap_with_version_and_crc32(version)
  end

  @spec encode_key_value_frames([binary()], %{binary() => binary()}) :: iodata()
  defp encode_key_value_frames([], _), do: []

  defp encode_key_value_frames([key | keys], writes),
    do: [
      encode_key_value_frame(key, writes[key])
      | encode_key_value_frames(keys, writes)
    ]

  @spec encode_key_value_frame(binary(), binary()) :: iodata()
  defp encode_key_value_frame(key, value) do
    n_key_bytes = byte_size(key)
    n_value_bytes = byte_size(value)
    n_bytes = n_value_bytes + n_key_bytes + 2
    [<<n_bytes::unsigned-big-32, n_key_bytes::unsigned-big-16>>, key, value]
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
  @spec decode(t()) :: {:ok, Transaction.t()} | {:error, :crc32_mismatch}
  def decode(
        <<version::binary-size(8), size_in_bytes::unsigned-big-32,
          payload::binary-size(size_in_bytes), crc32::unsigned-big-32>>
      ) do
    if crc32 != :erlang.crc32(payload) do
      {:error, :crc32_mismatch}
    else
      {:ok, {version, decode_key_value_frames(payload)}}
    end
  end

  @spec decode!(t()) :: Transaction.t()
  def decode!(encoded_transaction) do
    case decode(encoded_transaction) do
      {:ok, transaction} ->
        transaction

      {:error, :crc32_mismatch} ->
        raise("Transaction decode failed: CRC32 checksum mismatch indicates corruption")
    end
  rescue
    FunctionClauseError ->
      reraise(
        "Transaction decode failed: invalid binary format, expected encoded transaction structure",
        __STACKTRACE__
      )
  end

  @spec decode_key_value_frames(binary()) :: %{binary() => binary()}
  defp decode_key_value_frames(<<>>), do: %{}

  defp decode_key_value_frames(
         <<n_bytes::unsigned-big-32, n_key_bytes::unsigned-big-16, key::binary-size(n_key_bytes),
           value::binary-size(n_bytes - n_key_bytes - 2), remainder::binary>>
       ) do
    remainder
    |> decode_key_value_frames()
    |> Map.put(key, value)
  end

  @doc """
  Filters keys (and values) from a transaction that are outside the specified
  key range. Since the key-value frames are sorted, we can skip over anything
  that precedes the min_key, and stop when we reach the max_key_ex. We attempt
  to be as efficient as possible by avoiding unnecessary copying of the binary
  data.
  """
  @spec transform_by_removing_keys_outside_of_range(t(), Bedrock.key_range()) :: t()
  def transform_by_removing_keys_outside_of_range(t, key_range) do
    iodata_transform_by_removing_keys_outside_of_range(t, key_range)
    |> IO.iodata_to_binary()
  end

  @spec iodata_transform_by_removing_keys_outside_of_range(t(), Bedrock.key_range()) :: iodata()
  def iodata_transform_by_removing_keys_outside_of_range(
        <<version::binary-size(8), n_bytes::unsigned-big-32, payload::binary-size(n_bytes),
          _::unsigned-big-32>> =
          original_transaction,
        {min_key, max_key_ex}
      ) do
    if max_key_ex == :end do
      # We only need to check the minimum key.
      start = filter_gte_min_key(payload, min_key, 0)
      {start, n_bytes - start}
    else
      # We need to check both the minimum and maximum keys.
      filter_gte_min_key_lt_max_key(payload, min_key, max_key_ex, {0, 0})
    end
    |> case do
      # The transaction is entirely inside the range.
      {_, ^n_bytes} ->
        original_transaction

      # The transaction is entirely outside the range.
      {_, 0} ->
        <<version::binary, 0::unsigned-big-32, 0::unsigned-big-32>>

      # The transaction is partially inside the range, slice out the relevant
      # key-value frames and reassemble the transaction.
      {start, size_in_bytes} ->
        payload
        |> binary_part(start, size_in_bytes)
        |> wrap_with_version_and_crc32(Version.from_bytes(version))
    end
  end

  @spec filter_gte_min_key(binary(), binary(), non_neg_integer()) :: non_neg_integer()
  defp filter_gte_min_key(<<>>, _min_key, start),
    do: start

  defp filter_gte_min_key(
         <<n_bytes::unsigned-big-32, n_key_bytes::unsigned-big-16, key::binary-size(n_key_bytes),
           _::binary-size(n_bytes - n_key_bytes - 2), remainder::binary>>,
         min_key,
         start
       ) do
    if key < min_key do
      filter_gte_min_key(remainder, min_key, start + 4 + n_bytes)
    else
      start
    end
  end

  @spec filter_gte_min_key_lt_max_key(
          binary(),
          binary(),
          binary(),
          {non_neg_integer(), non_neg_integer()}
        ) :: {non_neg_integer(), non_neg_integer()}
  defp filter_gte_min_key_lt_max_key(<<>>, _, _, range),
    do: range

  defp filter_gte_min_key_lt_max_key(
         <<n_bytes::unsigned-big-32, n_key_bytes::unsigned-big-16, key::binary-size(n_key_bytes),
           _::binary-size(n_bytes - n_key_bytes - 2), remainder::binary>>,
         min_key,
         max_key_ex,
         {start, size} = range
       ) do
    cond do
      # The key is larger than our range, . All the others must be too, so we
      # can stop.
      key >= max_key_ex ->
        range

      # The key is below the range, so we can skip it.
      key < min_key ->
        filter_gte_min_key_lt_max_key(
          remainder,
          min_key,
          max_key_ex,
          {start + 4 + n_bytes, size - 4 - n_bytes}
        )

      # Here's a keeper, so we slice out the key-value frame and continue.
      true ->
        filter_gte_min_key_lt_max_key(remainder, min_key, max_key_ex, {start, size + 4 + n_bytes})
    end
  end

  @doc """
  Transforms an encoded transaction by excluding the values of all key-value
  pairs, leaving only the keys in the encoded structure.

  This function is useful when there is a need to handle transactions where
  only the keys are relevant, and the values can be excluded (like when
  reloading a Resolver from a cold-state using the Logs).

  The transformation preserves the integrity of the encoded transaction by
  recalculating the CRC32 checksum based on the modified structure.
  """
  @spec transform_by_excluding_values(t()) :: t()
  def transform_by_excluding_values(t) do
    t
    |> iodata_transform_by_excluding_values()
    |> IO.iodata_to_binary()
  end

  @spec iodata_transform_by_excluding_values(t()) :: iodata()
  def iodata_transform_by_excluding_values(
        <<version::binary-size(8), n_bytes::unsigned-big-32, payload::binary-size(n_bytes),
          _::unsigned-big-32>>
      ) do
    payload
    |> exclude_values_from_payload()
    |> wrap_with_version_and_crc32(Version.from_bytes(version))
  end

  @spec exclude_values_from_payload(binary()) :: iodata()
  defp exclude_values_from_payload(<<>>), do: []

  defp exclude_values_from_payload(
         <<n_bytes::unsigned-big-32, n_key_bytes::unsigned-big-16, key::binary-size(n_key_bytes),
           _::binary-size(n_bytes - n_key_bytes - 2), remainder::binary>>
       ) do
    [
      [<<2 + n_key_bytes::unsigned-big-32, n_key_bytes::unsigned-big-16, key::binary>>]
      | exclude_values_from_payload(remainder)
    ]
  end

  @spec wrap_with_version_and_crc32(iodata(), Bedrock.version()) :: iodata()
  defp wrap_with_version_and_crc32(kv_frames, version),
    do: [
      [version, <<IO.iodata_length(kv_frames)::unsigned-big-32>>],
      kv_frames,
      <<:erlang.crc32(kv_frames)::unsigned-big-32>>
    ]
end
