defmodule Bedrock.ObjectStorage.Chunk do
  @moduledoc """
  Chunk format for storing transaction data in object storage.

  ## Binary Format

  Chunks store a sequence of transactions with a header and directory for
  efficient seeking. The format supports HTTP Range requests by placing
  the directory before the data.

  ```
  [Header - 32 bytes]
    - 4 bytes: Magic number (0x42444348 = "BDCH")
    - 1 byte:  Format version (0x01)
    - 1 byte:  Flags (reserved)
    - 2 bytes: Reserved
    - 8 bytes: Min version (lowest txn version in chunk)
    - 8 bytes: Max version (highest txn version in chunk)
    - 4 bytes: Transaction count
    - 4 bytes: Directory size in bytes

  [Directory - variable size]
    Array of directory entries, one per transaction:
    - 8 bytes: Transaction version
    - 4 bytes: Data offset from start of data section
    - 4 bytes: Data length

  [Data section - variable size]
    Concatenated transaction data (BRDT format)
  ```

  ## Read Workflow

  1. Range request for header (32 bytes) to get directory size
  2. Range request for directory (header.directory_size bytes)
  3. Binary search directory for target version
  4. Range request for data from offset to end (or specific range)

  ## Version Ordering

  Transactions within a chunk are stored in ascending version order.
  The chunk is named by its max version (highest version it contains).
  """

  @magic_number 0x42444348
  @format_version 0x01
  @header_size 32
  @directory_entry_size 16

  @type version :: non_neg_integer()
  @type transaction_data :: binary()

  @type header :: %{
          magic: non_neg_integer(),
          format_version: non_neg_integer(),
          flags: non_neg_integer(),
          min_version: version(),
          max_version: version(),
          txn_count: non_neg_integer(),
          directory_size: non_neg_integer()
        }

  @type directory_entry :: %{
          version: version(),
          offset: non_neg_integer(),
          length: non_neg_integer()
        }

  @type t :: %{
          header: header(),
          directory: [directory_entry()],
          data: binary()
        }

  @doc "Returns the fixed header size in bytes."
  @spec header_size() :: pos_integer()
  def header_size, do: @header_size

  @doc "Returns the size of each directory entry in bytes."
  @spec directory_entry_size() :: pos_integer()
  def directory_entry_size, do: @directory_entry_size

  @doc """
  Encodes a list of transactions into chunk format.

  Transactions must be provided as `{version, data}` tuples in ascending
  version order.

  ## Parameters

  - `transactions` - List of `{version, binary_data}` tuples, sorted by version

  ## Returns

  - `{:ok, chunk_binary}` - Encoded chunk
  - `{:error, reason}` - Encoding failed
  """
  @spec encode([{version(), transaction_data()}]) :: {:ok, binary()} | {:error, term()}
  def encode([]) do
    {:error, :empty_chunk}
  end

  def encode(transactions) when is_list(transactions) do
    # Build data section and directory entries
    {directory_entries, data_parts, _offset} =
      Enum.reduce(transactions, {[], [], 0}, fn {version, data}, {entries, parts, offset} ->
        length = byte_size(data)

        entry = %{
          version: version,
          offset: offset,
          length: length
        }

        {[entry | entries], [data | parts], offset + length}
      end)

    # Reverse to maintain order
    directory_entries = Enum.reverse(directory_entries)
    data = IO.iodata_to_binary(Enum.reverse(data_parts))

    # Extract version range
    %{version: min_version} = List.first(directory_entries)
    %{version: max_version} = List.last(directory_entries)
    txn_count = length(directory_entries)
    directory_size = txn_count * @directory_entry_size

    # Encode header
    header_binary =
      encode_header(%{
        magic: @magic_number,
        format_version: @format_version,
        flags: 0,
        min_version: min_version,
        max_version: max_version,
        txn_count: txn_count,
        directory_size: directory_size
      })

    # Encode directory
    directory_binary = encode_directory(directory_entries)

    {:ok, IO.iodata_to_binary([header_binary, directory_binary, data])}
  end

  @doc """
  Decodes a chunk header from binary data.

  ## Returns

  - `{:ok, header}` - Parsed header
  - `{:error, reason}` - Invalid header
  """
  @spec decode_header(binary()) :: {:ok, header()} | {:error, term()}
  def decode_header(
        <<@magic_number::unsigned-big-32, format_version::unsigned-8, flags::unsigned-8, _reserved::binary-size(2),
          min_version::unsigned-big-64, max_version::unsigned-big-64, txn_count::unsigned-big-32,
          directory_size::unsigned-big-32>>
      ) do
    {:ok,
     %{
       magic: @magic_number,
       format_version: format_version,
       flags: flags,
       min_version: min_version,
       max_version: max_version,
       txn_count: txn_count,
       directory_size: directory_size
     }}
  end

  def decode_header(<<magic::unsigned-big-32, _rest::binary>>) when magic != @magic_number do
    {:error, {:invalid_magic, magic}}
  end

  def decode_header(_) do
    {:error, :invalid_header}
  end

  @doc """
  Decodes directory entries from binary data.

  ## Parameters

  - `binary` - Directory section binary
  - `count` - Number of entries to decode

  ## Returns

  - `{:ok, entries}` - List of directory entries
  - `{:error, reason}` - Invalid directory
  """
  @spec decode_directory(binary(), non_neg_integer()) ::
          {:ok, [directory_entry()]} | {:error, term()}
  def decode_directory(binary, count) do
    decode_directory_entries(binary, count, [])
  end

  defp decode_directory_entries(_binary, 0, acc) do
    {:ok, Enum.reverse(acc)}
  end

  defp decode_directory_entries(
         <<version::unsigned-big-64, offset::unsigned-big-32, length::unsigned-big-32, rest::binary>>,
         count,
         acc
       ) do
    entry = %{version: version, offset: offset, length: length}
    decode_directory_entries(rest, count - 1, [entry | acc])
  end

  defp decode_directory_entries(_, _, _) do
    {:error, :invalid_directory}
  end

  @doc """
  Decodes a complete chunk from binary data.

  ## Returns

  - `{:ok, chunk}` - Parsed chunk with header, directory, and data
  - `{:error, reason}` - Invalid chunk
  """
  @spec decode(binary()) :: {:ok, t()} | {:error, term()}
  def decode(binary) when byte_size(binary) >= @header_size do
    <<header_binary::binary-size(@header_size), rest::binary>> = binary

    with {:ok, header} <- decode_header(header_binary),
         dir_size = header.directory_size,
         true <- byte_size(rest) >= dir_size,
         <<dir_binary::binary-size(dir_size), data::binary>> = rest,
         {:ok, directory} <- decode_directory(dir_binary, header.txn_count) do
      {:ok, %{header: header, directory: directory, data: data}}
    else
      false -> {:error, :truncated_chunk}
      error -> error
    end
  end

  def decode(_) do
    {:error, :truncated_chunk}
  end

  @doc """
  Extracts transactions from a decoded chunk.

  Returns a list of `{version, data}` tuples.
  """
  @spec extract_transactions(t()) :: [{version(), transaction_data()}]
  def extract_transactions(%{directory: directory, data: data}) do
    Enum.map(directory, fn %{version: version, offset: offset, length: length} ->
      txn_data = binary_part(data, offset, length)
      {version, txn_data}
    end)
  end

  @doc """
  Finds the directory entry for a specific version using binary search.

  Returns the entry if found, or `nil` if not present.
  """
  @spec find_entry([directory_entry()], version()) :: directory_entry() | nil
  def find_entry(directory, target_version) do
    binary_search(directory, target_version, 0, length(directory) - 1)
  end

  defp binary_search(_directory, _target, low, high) when low > high, do: nil

  defp binary_search(directory, target, low, high) do
    mid = div(low + high, 2)
    entry = Enum.at(directory, mid)

    cond do
      entry.version == target -> entry
      entry.version < target -> binary_search(directory, target, mid + 1, high)
      true -> binary_search(directory, target, low, mid - 1)
    end
  end

  @doc """
  Finds the first directory entry with version >= target.

  Useful for seeking to a position for forward scanning.
  """
  @spec find_first_entry_gte([directory_entry()], version()) :: {non_neg_integer(), directory_entry()} | nil
  def find_first_entry_gte(directory, target_version) do
    find_first_gte(directory, target_version, 0, length(directory) - 1, nil)
  end

  defp find_first_gte(_directory, _target, low, high, result) when low > high, do: result

  defp find_first_gte(directory, target, low, high, result) do
    mid = div(low + high, 2)
    entry = Enum.at(directory, mid)

    if entry.version >= target do
      # This could be our answer, but there might be an earlier one
      find_first_gte(directory, target, low, mid - 1, {mid, entry})
    else
      find_first_gte(directory, target, mid + 1, high, result)
    end
  end

  @doc """
  Calculates the byte range needed to read from a specific version to the end.

  Returns `{start_byte, end_byte}` for use with HTTP Range requests.
  The range is inclusive on both ends.
  """
  @spec byte_range_from_version(t(), version()) ::
          {:ok, {non_neg_integer(), non_neg_integer()}} | {:error, :version_not_found}
  def byte_range_from_version(%{header: header, directory: directory, data: data}, target_version) do
    case find_first_entry_gte(directory, target_version) do
      {_index, entry} ->
        data_section_start = @header_size + header.directory_size
        start_byte = data_section_start + entry.offset
        end_byte = data_section_start + byte_size(data) - 1
        {:ok, {start_byte, end_byte}}

      nil ->
        {:error, :version_not_found}
    end
  end

  # Private encoding helpers

  defp encode_header(%{
         magic: magic,
         format_version: format_version,
         flags: flags,
         min_version: min_version,
         max_version: max_version,
         txn_count: txn_count,
         directory_size: directory_size
       }) do
    <<
      magic::unsigned-big-32,
      format_version::unsigned-8,
      flags::unsigned-8,
      0::16,
      min_version::unsigned-big-64,
      max_version::unsigned-big-64,
      txn_count::unsigned-big-32,
      directory_size::unsigned-big-32
    >>
  end

  defp encode_directory(entries) do
    entries
    |> Enum.map(fn %{version: version, offset: offset, length: length} ->
      <<version::unsigned-big-64, offset::unsigned-big-32, length::unsigned-big-32>>
    end)
    |> IO.iodata_to_binary()
  end
end
