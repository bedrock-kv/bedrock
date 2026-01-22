defmodule Bedrock.ObjectStorage.ChunkWriter do
  @moduledoc """
  Writes transaction chunks to object storage with hybrid sizing.

  ChunkWriter accumulates transactions and flushes them to object storage
  when either condition is met:
  - Size threshold reached (e.g., 64MB)
  - Time gap exceeded (e.g., 5 minutes since last transaction)

  ## Hybrid Sizing Strategy

  Since transaction IDs are microsecond timestamps, time-based cutting is
  equivalent to version-based. When no new transactions arrive for the
  configured time gap, the current buffer is flushed even if small.

  Background compaction (wxf-6) will later merge small chunks into larger ones.

  ## Usage

      {:ok, writer} = ChunkWriter.new(backend, "a",
        size_threshold: 64 * 1024 * 1024,  # 64MB
        time_gap_ms: 5 * 60 * 1000         # 5 minutes
      )

      {:ok, writer} = ChunkWriter.add_transaction(writer, version, data)
      {:ok, writer} = ChunkWriter.add_transaction(writer, version2, data2)

      # Check if flush is needed (call periodically or after adds)
      case ChunkWriter.maybe_flush(writer) do
        {:ok, writer, :flushed} -> # chunk was written
        {:ok, writer, :not_needed} -> # nothing to flush
      end

      # Force flush remaining transactions
      {:ok, writer} = ChunkWriter.flush(writer)
  """

  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.Chunk
  alias Bedrock.ObjectStorage.Keys

  @default_size_threshold 64 * 1024 * 1024
  @default_time_gap_ms 5 * 1000

  @type version :: non_neg_integer()
  @type transaction_data :: binary()

  @type t :: %__MODULE__{
          backend: ObjectStorage.backend(),
          shard_tag: String.t(),
          size_threshold: pos_integer(),
          time_gap_ms: pos_integer(),
          buffer: [{version(), transaction_data()}],
          buffer_size: non_neg_integer(),
          last_add_time: integer() | nil,
          chunks_written: non_neg_integer()
        }

  defstruct [
    :backend,
    :shard_tag,
    :size_threshold,
    :time_gap_ms,
    buffer: [],
    buffer_size: 0,
    last_add_time: nil,
    chunks_written: 0
  ]

  @doc """
  Creates a new chunk writer.

  ## Options

  - `:size_threshold` - Flush when buffer exceeds this size (default: 64MB)
  - `:time_gap_ms` - Flush when this many ms pass without new transactions (default: 5 min)
  """
  @spec new(ObjectStorage.backend(), String.t(), keyword()) :: {:ok, t()}
  def new(backend, shard_tag, opts \\ []) do
    writer = %__MODULE__{
      backend: backend,
      shard_tag: shard_tag,
      size_threshold: Keyword.get(opts, :size_threshold, @default_size_threshold),
      time_gap_ms: Keyword.get(opts, :time_gap_ms, @default_time_gap_ms)
    }

    {:ok, writer}
  end

  @doc """
  Adds a transaction to the writer's buffer.

  Transactions should be added in version order. After adding, call
  `maybe_flush/1` to check if a flush is needed.
  """
  @spec add_transaction(t(), version(), transaction_data()) :: {:ok, t()}
  def add_transaction(%__MODULE__{} = writer, version, data) when is_binary(data) do
    writer = %{
      writer
      | buffer: [{version, data} | writer.buffer],
        buffer_size: writer.buffer_size + byte_size(data),
        last_add_time: System.monotonic_time(:millisecond)
    }

    {:ok, writer}
  end

  @doc """
  Checks if a flush is needed and performs it if so.

  Flush triggers:
  - Buffer size >= size_threshold
  - Time since last add >= time_gap_ms (and buffer not empty)

  ## Returns

  - `{:ok, writer, :flushed}` - Chunk was written
  - `{:ok, writer, :not_needed}` - No flush needed
  - `{:error, reason}` - Flush failed
  """
  @spec maybe_flush(t()) :: {:ok, t(), :flushed | :not_needed} | {:error, term()}
  def maybe_flush(%__MODULE__{buffer: []} = writer) do
    {:ok, writer, :not_needed}
  end

  def maybe_flush(%__MODULE__{} = writer) do
    cond do
      size_threshold_reached?(writer) ->
        case do_flush(writer) do
          {:ok, writer} -> {:ok, writer, :flushed}
          error -> error
        end

      time_gap_exceeded?(writer) ->
        case do_flush(writer) do
          {:ok, writer} -> {:ok, writer, :flushed}
          error -> error
        end

      true ->
        {:ok, writer, :not_needed}
    end
  end

  @doc """
  Forces a flush of the current buffer, even if thresholds haven't been reached.

  Does nothing if buffer is empty.
  """
  @spec flush(t()) :: {:ok, t()} | {:error, term()}
  def flush(%__MODULE__{buffer: []} = writer) do
    {:ok, writer}
  end

  def flush(%__MODULE__{} = writer) do
    do_flush(writer)
  end

  @doc """
  Returns the current buffer size in bytes.
  """
  @spec buffer_size(t()) :: non_neg_integer()
  def buffer_size(%__MODULE__{buffer_size: size}), do: size

  @doc """
  Returns the number of transactions in the buffer.
  """
  @spec buffer_count(t()) :: non_neg_integer()
  def buffer_count(%__MODULE__{buffer: buffer}), do: length(buffer)

  @doc """
  Returns the number of chunks written so far.
  """
  @spec chunks_written(t()) :: non_neg_integer()
  def chunks_written(%__MODULE__{chunks_written: count}), do: count

  @doc """
  Returns true if the buffer is empty.
  """
  @spec empty?(t()) :: boolean()
  def empty?(%__MODULE__{buffer: []}), do: true
  def empty?(%__MODULE__{}), do: false

  # Private helpers

  defp size_threshold_reached?(%__MODULE__{buffer_size: size, size_threshold: threshold}) do
    size >= threshold
  end

  defp time_gap_exceeded?(%__MODULE__{last_add_time: nil}), do: false

  defp time_gap_exceeded?(%__MODULE__{last_add_time: last_add, time_gap_ms: gap}) do
    now = System.monotonic_time(:millisecond)
    now - last_add >= gap
  end

  defp do_flush(%__MODULE__{buffer: buffer} = writer) do
    # Buffer is stored in reverse order, so reverse to get ascending version order
    transactions = Enum.reverse(buffer)

    case Chunk.encode(transactions) do
      {:ok, chunk_binary} ->
        # Get max version for chunk naming
        {max_version, _data} = List.last(transactions)
        key = Keys.chunk_path(writer.shard_tag, max_version)

        case ObjectStorage.put_if_not_exists(writer.backend, key, chunk_binary) do
          :ok ->
            writer = %{
              writer
              | buffer: [],
                buffer_size: 0,
                last_add_time: nil,
                chunks_written: writer.chunks_written + 1
            }

            {:ok, writer}

          {:error, :already_exists} ->
            # Chunk already exists - this is idempotent, treat as success
            writer = %{
              writer
              | buffer: [],
                buffer_size: 0,
                last_add_time: nil,
                chunks_written: writer.chunks_written + 1
            }

            {:ok, writer}

          {:error, reason} ->
            {:error, {:write_failed, reason}}
        end

      {:error, reason} ->
        {:error, {:encode_failed, reason}}
    end
  end
end
