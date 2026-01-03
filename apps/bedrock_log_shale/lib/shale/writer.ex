defmodule Bedrock.DataPlane.Log.Shale.Writer do
  @moduledoc """
  A struct that represents a writer for a segment.
  """

  alias Bedrock.DataPlane.Transaction

  defstruct [:fd, :write_offset, :bytes_remaining]

  @wal_eof_version <<0xFFFFFFFFFFFFFFFF::unsigned-big-64>>
  @eof_marker <<@wal_eof_version::binary, 0::unsigned-big-32, 0::unsigned-big-32>>
  @empty_segment_header <<"BED0">> <> @eof_marker

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

  @spec open(path_to_file :: String.t()) :: {:ok, t()} | {:error, File.posix()}
  def open(path_to_file) do
    with {:ok, stat} <- File.stat(path_to_file),
         {:ok, fd} <- File.open(path_to_file, [:write, :read, :raw, :binary]),
         :ok <- :file.pwrite(fd, 0, @empty_segment_header) do
      {:ok,
       %__MODULE__{
         fd: fd,
         write_offset: 4,
         bytes_remaining: stat.size - 4 - 16
       }}
    end
  end

  @spec close(writer :: t() | nil) :: :ok | {:error, File.posix()}
  def close(nil), do: :ok
  def close(%__MODULE__{} = writer), do: :file.close(writer.fd)

  @spec append(t(), Transaction.encoded(), Bedrock.version()) ::
          {:ok, t()} | {:error, :segment_full} | {:error, File.posix()}
  def append(%__MODULE__{} = writer, transaction, _commit_version)
      when writer.bytes_remaining < 16 + byte_size(transaction), do: {:error, :segment_full}

  def append(%__MODULE__{} = writer, transaction, commit_version) do
    # Wrap transaction in log format: [version, size, payload, crc32]
    payload_size = byte_size(transaction)
    crc32 = :erlang.crc32(transaction)

    log_entry = <<
      commit_version::binary-size(8),
      payload_size::unsigned-big-32,
      transaction::binary,
      crc32::unsigned-big-32
    >>

    writer.fd
    |> :file.pwrite(writer.write_offset, [log_entry, @eof_marker])
    |> case do
      :ok ->
        size_of_entry = byte_size(log_entry)
        new_write_offset = writer.write_offset + size_of_entry
        new_bytes_remaining = writer.bytes_remaining - size_of_entry
        {:ok, %{writer | write_offset: new_write_offset, bytes_remaining: new_bytes_remaining}}

      {:error, _reason} = error ->
        error
    end
  end
end
