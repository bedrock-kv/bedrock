defmodule Bedrock.DataPlane.Log.Shale.TransactionStreams do
  @moduledoc """
  A module for handling transaction streams with operations like limiting,
  filtering, and halting based on conditions.
  """

  alias Bedrock.DataPlane.Log.Shale.Segment
  alias Bedrock.DataPlane.Transaction

  @wal_magic_number <<"BED0">>
  @wal_eof_version <<0xFFFFFFFFFFFFFFFF::unsigned-big-64>>

  @spec from_segments([Segment.t()], Bedrock.version()) ::
          {:ok, Enumerable.t(Transaction.encoded())} | {:error, :not_found}
  def from_segments([], _target_version), do: {:error, :not_found}

  def from_segments(segments, target_version) do
    case find_segments_from_target(segments, target_version) do
      [] ->
        {:error, :not_found}

      segments ->
        stream =
          segments
          |> Stream.flat_map(fn segment ->
            segment
            |> Segment.transactions()
            # Convert from newest-first to oldest-first
            |> Enum.reverse()
          end)
          |> Stream.filter(fn transaction ->
            version = Transaction.commit_version!(transaction)
            version > target_version
          end)

        # Check if stream has any elements
        case Enum.take(stream, 1) do
          [] -> {:error, :not_found}
          _ -> {:ok, stream}
        end
    end
  end

  # Find segments starting from the first one that could contain transactions > target_version
  defp find_segments_from_target(segments, target_version), do: find_valid_segments(segments, target_version, [])

  defp find_valid_segments([segment | rest], target_version, acc),
    do: find_valid_segments(rest, target_version, [segment | acc])

  defp find_valid_segments([], _target_version, acc), do: Enum.reverse(acc)

  @spec from_list_of_transactions((-> [Transaction.encoded()] | nil)) :: Enumerable.t(Transaction.encoded())
  def from_list_of_transactions(transactions_fn) do
    Stream.resource(
      transactions_fn,
      fn
        transactions when is_list(transactions) ->
          {transactions, nil}

        nil ->
          {:halt, nil}
      end,
      fn nil -> :ok end
    )
  end

  @doc """
  Streams transactions from the segment.

  This function returns a Stream that iterates through the transactions
  in the given segment. Each transaction is validated using its checksum
  (CRC32), and the stream yields either valid transactions, identifies
  end-of-file markers, or flags corrupted data. Offsets are tracked so that
  append operations can be performed safely.
  """
  @spec from_file!(path_to_file :: String.t()) ::
          Enumerable.t({Transaction.encoded() | :eof | :corrupted, non_neg_integer()})
  def from_file!(path_to_file) do
    Stream.resource(
      fn ->
        case File.read!(path_to_file) do
          <<@wal_magic_number, bytes::binary>> ->
            {4, bytes}

          other ->
            {:error, {:invalid_wal_format, byte_size(other)}}
        end
      end,
      fn
        {:error, _reason} = error ->
          {:halt, error}

        {offset,
         <<version_binary::binary-size(8), size_in_bytes::unsigned-big-32, payload::binary-size(size_in_bytes),
           crc32::unsigned-big-32, remaining_bytes::binary>>} ->
          cond do
            @wal_eof_version == version_binary ->
              {:halt, nil}

            :erlang.crc32(payload) == crc32 ->
              {[payload], {offset + 16 + size_in_bytes, remaining_bytes}}

            true ->
              nil
          end

        {_, offset} ->
          error = {:error, {:corrupted, offset}}
          {[error], error}
      end,
      fn _ -> :ok end
    )
  end

  @spec until_version(Enumerable.t(), Bedrock.version()) :: Enumerable.t()
  def until_version(stream, nil), do: stream

  def until_version(stream, last_version) do
    Stream.transform(stream, last_version, fn
      encoded_transaction, last_version ->
        case Transaction.commit_version(encoded_transaction) do
          {:ok, version} when version <= last_version ->
            {[encoded_transaction], last_version}

          _ ->
            {:halt, nil}
        end
    end)
  end

  @doc """
  Limits the number of transactions in the stream based on the given version.
  """
  @spec at_most(Enumerable.t(), pos_integer()) :: Enumerable.t()
  def at_most(stream, limit) do
    Stream.transform(stream, limit, fn
      transaction, 0 -> {:halt, transaction}
      transaction, n -> {[transaction], n - 1}
    end)
  end
end
