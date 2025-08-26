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
          {:ok, Enumerable.t()} | {:error, :not_found}
  def from_segments([], _target_version), do: {:error, :not_found}

  def from_segments(segments, target_version) do
    # Collect all transactions from all segments that match the criteria
    all_valid_transactions =
      segments
      |> Enum.flat_map(fn segment ->
        segment
        |> Segment.transactions()
        |> Enum.filter(fn transaction ->
          version = Transaction.extract_commit_version!(transaction)
          version > target_version
        end)
      end)
      |> Enum.sort_by(fn transaction ->
        # Sort by version to ensure proper ordering
        Transaction.extract_commit_version!(transaction)
      end)

    case all_valid_transactions do
      [] -> {:error, :not_found}
      transactions -> {:ok, from_list_of_transactions(fn -> transactions end)}
    end
  end

  @spec from_list_of_transactions((-> [Transaction.encoded()] | nil)) :: Enumerable.t()
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
        case Transaction.extract_commit_version(encoded_transaction) do
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
