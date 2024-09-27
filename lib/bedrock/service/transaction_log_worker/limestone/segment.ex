defmodule Bedrock.Service.TransactionLogWorker.Limestone.Segment do
  use Bedrock, :types
  use Bedrock.Cluster, :types

  @type t :: %__MODULE__{}
  defstruct [:path, :size]

  alias Bedrock.DataPlane.Transaction

  @version_bits 6
  @kv_bits 32 - @version_bits
  @empty_segment_header <<"SGMT", 8::big-integer-size(32)>>

  @doc """
  Allocate a new segment segment and ensure that it is the given size.
  """
  @spec allocate(file_path :: binary(), size :: pos_integer()) ::
          {:ok, t()} | {:error, :path_does_not_exist}
  def allocate(file_path, size) do
    with segment <- %__MODULE__{path: file_path, size: size},
         :ok <- clear(segment) do
      {:ok, segment}
    end
  end

  def clear(segment) do
    with {:ok, fd} <- :file.open(segment.path |> String.to_charlist(), [:write, :binary, :raw]),
         :ok <- :file.allocate(fd, 0, segment.size),
         :ok <- :file.pwrite(fd, 0, @empty_segment_header),
         :ok <- :file.close(fd) do
      :ok
    else
      {:error, :eisdir} -> raise "not implemented"
      {:error, :enoent} -> {:error, :path_does_not_exist}
    end
  end

  @doc """
  Create a new segment from the given file path. We stat the file to get the
  size, ensuring that it exists.
  """
  @spec from_path(file_path :: String.t()) :: {:ok, t()} | {:error, :does_not_exist}
  def from_path(file_path) do
    File.stat(file_path)
    |> case do
      {:ok, stat} ->
        {:ok,
         %__MODULE__{
           path: file_path,
           size: stat.size
         }}

      {:error, :enoent} ->
        {:error, :does_not_exist}
    end
  end

  @doc """
  Rename the segment's file.
  """
  @spec rename(segment :: t(), new_path :: String.t()) :: {:ok, t()} | {:error, :does_not_exist}
  def rename(segment, new_path) do
    :file.rename(segment.path |> String.to_charlist(), new_path |> String.to_charlist())
    |> case do
      :ok -> {:ok, %{segment | path: new_path}}
      {:error, :enoent} -> {:error, :does_not_exist}
    end
  end

  @doc """
  Construct a stream that will read key/value pairs from the segment. For
  efficiency, we read the entire segment into memory and then stream from
  there, decoding the key/value pairs as they are consumed.
  """
  @spec stream!(segment :: t()) :: Enumerable.t()
  def stream!(segment) do
    Stream.resource(
      fn ->
        <<"SGMT", n_bytes::big-unsigned-integer-size(32), bytes::binary-size(n_bytes - 8),
          _trailing_bytes::binary>> = File.read!(segment.path)

        bytes
      end,
      fn
        <<crc32::big-integer-size(32), version_size::big-integer-size(@version_bits),
          key_values_size::big-integer-size(@kv_bits), version::binary-size(version_size),
          key_values::binary-size(key_values_size), remaining_bytes::binary>> ->
          calc_crc32 = :erlang.crc32(version) |> :erlang.crc32(key_values)

          crc32 == calc_crc32 || raise "CRC32 mismatch"

          {[Transaction.new(version, key_values)], remaining_bytes}

        <<>> ->
          {:halt, []}
      end,
      fn _ -> :ok end
    )
  end

  defmodule Writer do
    @moduledoc """
    A struct that represents a writer for a segment.
    """
    defstruct [:fd, :write_offset, :bytes_remaining]

    @typedoc """
    A `Writer` is a handle to a segment that can be used to write transcations
    to the segment. It is a stateful object that keeps track of the current
    write offset and the number of bytes remaining in the segment.
    """
    @type t :: %__MODULE__{
            fd: File.file_descriptor(),
            write_offset: pos_integer(),
            bytes_remaining: pos_integer()
          }
  end

  @spec writer(t()) :: {:ok, Writer.t()} | {:error, atom()}
  def writer(segment) do
    with {:ok, fd} <-
           :file.open(segment.path |> String.to_charlist(), [:write, :read, :raw, :binary]),
         :ok <- :file.pwrite(fd, 0, @empty_segment_header) do
      {:ok,
       %Writer{
         fd: fd,
         write_offset: 8,
         bytes_remaining: segment.size - 8
       }}
    end
  end

  @spec close(writer :: Writer.t()) :: :ok | {:error, atom()}
  def close(%Writer{} = writer) do
    with :ok <-
           :file.pwrite(
             writer.fd,
             0,
             <<"SGMT", writer.write_offset::big-unsigned-integer-size(32)>>
           ) do
      :file.close(writer.fd)
    end
  end

  @spec append(Writer.t(), [Transaction.t()]) ::
          {:ok, {written :: non_neg_integer(), remaining :: non_neg_integer()}, Writer.t()}
          | {:error, atom()}
  def append(%Writer{} = writer, transactions) do
    {encoded_key_values, remainder, bytes_remaining} =
      encode_transactions(transactions, writer.bytes_remaining)

    :file.pwrite(writer.fd, writer.write_offset, encoded_key_values)
    |> case do
      :ok ->
        new_write_offset = writer.write_offset + IO.iodata_length(encoded_key_values)

        {:ok, {length(encoded_key_values), length(remainder)},
         %{writer | write_offset: new_write_offset, bytes_remaining: bytes_remaining}}

      {:error, _reason} = error ->
        error
    end
  end

  @spec encode_transactions(
          [transaction()],
          (bytes_available :: non_neg_integer())
          | {encoded :: iolist(), bytes_remaining :: non_neg_integer()}
        ) ::
          {encoded :: iodata(), remainder :: [key_value()], bytes_remaining :: non_neg_integer()}
  defp encode_transactions([], {encoded, bytes_remaining}),
    do: {encoded |> Enum.reverse(), [], bytes_remaining}

  defp encode_transactions(
         [transaction | remainder] = transactions,
         {encoded, bytes_remaining}
       ) do
    {version, key_values} = Transaction.encode(transaction)

    version_size = byte_size(version)
    key_values_size = byte_size(key_values)
    total_size = 4 + 4 + version_size + key_values_size

    if total_size <= bytes_remaining do
      header = <<
        version_size::big-unsigned-integer-size(@version_bits),
        key_values_size::big-unsigned-integer-size(@kv_bits)
      >>

      crc32 =
        :erlang.crc32(version)
        |> :erlang.crc32(key_values)

      encode_transactions(
        remainder,
        {[[<<crc32::big-unsigned-integer-size(32)>>, header, version, key_values] | encoded],
         bytes_remaining - total_size}
      )
    else
      {encoded |> Enum.reverse(), transactions, bytes_remaining}
    end
  end

  defp encode_transactions(transactions, bytes_available) when is_integer(bytes_available),
    do: encode_transactions(transactions, {[], bytes_available})
end
