defmodule Bedrock.DataPlane.Log.Shale.Writer do
  @moduledoc """
  A struct that represents a writer for a segment.
  """

  alias Bedrock.DataPlane.Transaction

  defstruct [:fd, :write_offset, :bytes_remaining, :sync_fun]

  @wal_eof_version <<0xFFFFFFFFFFFFFFFF::unsigned-big-64>>
  @eof_marker <<@wal_eof_version::binary, 0::unsigned-big-32, 0::unsigned-big-32>>
  @wal_magic_number <<"BED1">>
  @header_size 12

  @typedoc """
  A `Writer` is a handle to a segment that can be used to write transcations
  to the segment. It is a stateful object that keeps track of the current
  write offset and the number of bytes remaining in the segment.
  """
  @type t :: %__MODULE__{
          fd: File.file_descriptor(),
          write_offset: pos_integer(),
          bytes_remaining: pos_integer(),
          sync_fun: (File.file_descriptor() -> :ok | {:error, File.posix()})
        }

  @spec open(path_to_file :: String.t(), previous_version :: Bedrock.version(), opts :: keyword()) ::
          {:ok, t()} | {:error, File.posix()}
  def open(path_to_file, previous_version, opts \\ []) do
    sync_fun = Keyword.get(opts, :sync_fun, &:file.sync/1)
    empty_segment_header = <<@wal_magic_number, previous_version::binary-size(8), @eof_marker>>

    with {:ok, stat} <- File.stat(path_to_file),
         {:ok, fd} <- File.open(path_to_file, [:write, :read, :raw, :binary]) do
      # Write header - close fd on failure to avoid leak
      case :file.pwrite(fd, 0, empty_segment_header) do
        :ok ->
          {:ok,
           %__MODULE__{
             fd: fd,
             write_offset: @header_size,
             bytes_remaining: stat.size - @header_size - 16,
             sync_fun: sync_fun
           }}

        {:error, reason} ->
          File.close(fd)
          {:error, reason}
      end
    end
  end

  @spec close(writer :: t() | nil) :: :ok | {:error, File.posix()}
  def close(nil), do: :ok
  def close(%__MODULE__{} = writer), do: :file.close(writer.fd)

  @doc "Persists an empty segment header and its replay cursor."
  @spec sync(t()) :: :ok | {:error, File.posix()}
  def sync(%__MODULE__{} = writer), do: writer.sync_fun.(writer.fd)

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
        case writer.sync_fun.(writer.fd) do
          :ok ->
            size_of_entry = byte_size(log_entry)
            new_write_offset = writer.write_offset + size_of_entry
            new_bytes_remaining = writer.bytes_remaining - size_of_entry
            {:ok, %{writer | write_offset: new_write_offset, bytes_remaining: new_bytes_remaining}}

          {:error, _reason} = error ->
            error
        end

      {:error, _reason} = error ->
        error
    end
  end
end
