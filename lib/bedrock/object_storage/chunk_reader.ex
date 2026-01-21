defmodule Bedrock.ObjectStorage.ChunkReader do
  @moduledoc """
  Reads transaction chunks from object storage.

  ChunkReader provides efficient access to transaction data stored in chunks,
  supporting:

  - Listing chunks in version order (newest first due to inverted keys)
  - Seeking to a specific version
  - Forward scanning from a version
  - Lazy streaming for replay workflows

  ## Replay Workflow

  Cold start playback uses the following approach:

  1. List chunks (lazy stream, fetches pages as needed)
  2. Find the chunk containing the target version
  3. Seek within that chunk using the directory
  4. Read forward through successive chunks

  ## Example

      reader = ChunkReader.new(backend, "a")

      # List all chunks (newest first)
      chunks = ChunkReader.list_chunks(reader) |> Enum.to_list()

      # Read transactions from a specific version forward
      transactions = ChunkReader.read_from_version(reader, target_version)
      |> Enum.each(fn {version, data} -> process(version, data) end)
  """

  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.Chunk
  alias Bedrock.ObjectStorage.Keys

  @type version :: non_neg_integer()
  @type transaction_data :: binary()

  @type t :: %__MODULE__{
          backend: ObjectStorage.backend(),
          shard_tag: String.t()
        }

  defstruct [:backend, :shard_tag]

  @doc """
  Creates a new chunk reader for a shard.
  """
  @spec new(ObjectStorage.backend(), String.t()) :: t()
  def new(backend, shard_tag) do
    %__MODULE__{
      backend: backend,
      shard_tag: shard_tag
    }
  end

  @doc """
  Lists chunk keys in the shard.

  Returns a lazy stream of chunk keys in lexicographic order.
  Due to inverted version keys, this means newest chunks first.

  ## Options

  - `:limit` - Maximum number of chunks to return
  """
  @spec list_chunks(t(), keyword()) :: Enumerable.t()
  def list_chunks(%__MODULE__{} = reader, opts \\ []) do
    prefix = Keys.chunks_prefix(reader.shard_tag)
    ObjectStorage.list(reader.backend, prefix, opts)
  end

  @doc """
  Lists chunk metadata (key and version range) without reading full chunks.

  Returns a lazy stream of `{key, min_version, max_version}` tuples.
  Requires reading the header of each chunk.
  """
  @spec list_chunk_metadata(t(), keyword()) :: Enumerable.t()
  def list_chunk_metadata(%__MODULE__{} = reader, opts \\ []) do
    reader
    |> list_chunks(opts)
    |> Stream.map(fn key ->
      case read_chunk_header(reader, key) do
        {:ok, header} -> {key, header.min_version, header.max_version}
        {:error, _} -> nil
      end
    end)
    |> Stream.reject(&is_nil/1)
  end

  @doc """
  Reads a complete chunk by key.

  ## Returns

  - `{:ok, chunk}` - Decoded chunk
  - `{:error, reason}` - Read or decode failed
  """
  @spec read_chunk(t(), String.t()) :: {:ok, Chunk.t()} | {:error, term()}
  def read_chunk(%__MODULE__{backend: backend}, key) do
    case ObjectStorage.get(backend, key) do
      {:ok, binary} -> Chunk.decode(binary)
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Reads just the header of a chunk.

  Useful for determining version range without reading full chunk.
  """
  @spec read_chunk_header(t(), String.t()) :: {:ok, Chunk.header()} | {:error, term()}
  def read_chunk_header(%__MODULE__{backend: backend}, key) do
    header_size = Chunk.header_size()

    case ObjectStorage.get(backend, key) do
      {:ok, binary} when byte_size(binary) >= header_size ->
        header_binary = binary_part(binary, 0, header_size)
        Chunk.decode_header(header_binary)

      {:ok, _} ->
        {:error, :truncated_chunk}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Finds the chunk containing a specific version.

  Returns the chunk key if found, or `nil` if no chunk contains that version.
  Searches through chunks (newest first) until finding one where
  `min_version <= target <= max_version`.
  """
  @spec find_chunk_for_version(t(), version()) :: String.t() | nil
  def find_chunk_for_version(%__MODULE__{} = reader, target_version) do
    reader
    |> list_chunk_metadata()
    |> Enum.find(fn {_key, min_version, max_version} ->
      target_version >= min_version and target_version <= max_version
    end)
    |> case do
      {key, _, _} -> key
      nil -> nil
    end
  end

  @doc """
  Reads all transactions from a specific version forward.

  Returns a lazy stream of `{version, data}` tuples starting from the first
  transaction with version >= target_version.

  The stream:
  1. Finds the chunk containing the target version
  2. Seeks to the first transaction >= target within that chunk
  3. Continues through subsequent chunks in version order

  ## Options

  - `:limit` - Maximum number of transactions to return
  """
  @spec read_from_version(t(), version(), keyword()) :: Enumerable.t()
  def read_from_version(%__MODULE__{} = reader, target_version, opts \\ []) do
    limit = Keyword.get(opts, :limit)

    Stream.resource(
      fn -> init_read_state(reader, target_version, limit) end,
      &read_next_transaction/1,
      fn _ -> :ok end
    )
  end

  @doc """
  Reads all transactions from all chunks.

  Returns a lazy stream of `{version, data}` tuples in ascending version order
  (reads chunks from oldest to newest).
  """
  @spec read_all_transactions(t(), keyword()) :: Enumerable.t()
  def read_all_transactions(%__MODULE__{} = reader, opts \\ []) do
    limit = Keyword.get(opts, :limit)

    # Get all chunk keys (newest first due to inverted keys)
    chunk_keys = reader |> list_chunks() |> Enum.to_list()

    # Reverse to get oldest first (ascending version order)
    chunk_keys_ascending = Enum.reverse(chunk_keys)

    Stream.resource(
      fn -> init_all_read_state(reader, chunk_keys_ascending, limit) end,
      &read_next_transaction/1,
      fn _ -> :ok end
    )
  end

  @doc """
  Gets the latest (highest) version in the shard.

  Returns `nil` if no chunks exist.
  """
  @spec latest_version(t()) :: version() | nil
  def latest_version(%__MODULE__{} = reader) do
    reader
    |> list_chunks(limit: 1)
    |> Enum.take(1)
    |> case do
      [key] ->
        case Keys.extract_version(key) do
          {:ok, version} -> version
          {:error, _} -> nil
        end

      [] ->
        nil
    end
  end

  @doc """
  Gets the oldest (lowest) version in the shard.

  Note: This requires scanning all chunks, so it's less efficient than `latest_version/1`.
  Returns `nil` if no chunks exist.
  """
  @spec oldest_version(t()) :: version() | nil
  def oldest_version(%__MODULE__{} = reader) do
    # Get all chunks and find the one with lowest min_version
    reader
    |> list_chunk_metadata()
    |> Enum.reduce(nil, fn {_key, min_version, _max_version}, acc ->
      case acc do
        nil -> min_version
        current when min_version < current -> min_version
        current -> current
      end
    end)
  end

  # Private: Stream state management

  # State: {reader, chunk_keys_to_process, current_transactions, remaining_limit}
  defp init_read_state(reader, target_version, limit) do
    # Find all chunks and filter to those that might contain versions >= target
    chunk_keys_with_versions =
      reader
      |> list_chunk_metadata()
      |> Enum.filter(fn {_key, _min, max_version} ->
        # Keep chunks where max >= target (might have relevant transactions)
        max_version >= target_version
      end)
      |> Enum.to_list()

    # Sort by max_version ascending (oldest first for forward reading)
    sorted =
      Enum.sort_by(chunk_keys_with_versions, fn {_key, min_version, _max} -> min_version end)

    # Find first chunk that could contain target and start from there
    {chunks_to_process, first_chunk_transactions} =
      case find_starting_chunk(reader, sorted, target_version) do
        {remaining_chunks, transactions} -> {remaining_chunks, transactions}
        nil -> {[], []}
      end

    {reader, chunks_to_process, first_chunk_transactions, limit}
  end

  defp init_all_read_state(reader, chunk_keys, limit) do
    {reader, chunk_keys, [], limit}
  end

  defp find_starting_chunk(_reader, [], _target_version), do: nil

  defp find_starting_chunk(reader, [{key, _min, _max} | rest], target_version) do
    case read_chunk(reader, key) do
      {:ok, chunk} ->
        case Chunk.find_first_entry_gte(chunk.directory, target_version) do
          {index, _entry} ->
            # Found! Extract transactions from this index forward
            transactions =
              chunk
              |> Chunk.extract_transactions()
              |> Enum.drop(index)

            {rest, transactions}

          nil ->
            # Target is beyond this chunk, try next
            find_starting_chunk(reader, rest, target_version)
        end

      {:error, _} ->
        # Skip failed chunk, try next
        find_starting_chunk(reader, rest, target_version)
    end
  end

  defp read_next_transaction({_reader, _chunks, _txns, 0}) do
    {:halt, nil}
  end

  defp read_next_transaction({_reader, [], [], _limit}) do
    {:halt, nil}
  end

  defp read_next_transaction({reader, chunks, [txn | rest], limit}) do
    new_limit = if limit, do: limit - 1
    {[txn], {reader, chunks, rest, new_limit}}
  end

  defp read_next_transaction({reader, [key | rest_chunks], [], limit}) when is_binary(key) do
    # Load next chunk - key is a string path
    case read_chunk(reader, key) do
      {:ok, chunk} ->
        transactions = Chunk.extract_transactions(chunk)
        read_next_transaction({reader, rest_chunks, transactions, limit})

      {:error, _} ->
        # Skip failed chunk
        read_next_transaction({reader, rest_chunks, [], limit})
    end
  end

  defp read_next_transaction({reader, [{key, _min, _max} | rest_chunks], [], limit}) do
    # Load next chunk - from metadata tuple
    case read_chunk(reader, key) do
      {:ok, chunk} ->
        transactions = Chunk.extract_transactions(chunk)
        read_next_transaction({reader, rest_chunks, transactions, limit})

      {:error, _} ->
        # Skip failed chunk
        read_next_transaction({reader, rest_chunks, [], limit})
    end
  end
end
