defmodule Bedrock.DataPlane.Storage.Olivine.IndexDatabase do
  @moduledoc """
  Append-only file-based storage for Olivine index version blocks.

  ## File Format

  Each record represents a version block containing modified pages:

      [Header: 16 bytes]
      - magic: 0x4F4C5644 (4 bytes) - "OLVD"
      - version: (8 bytes) - commit version (binary)
      - payload_size: (4 bytes) - size of payload

      [Payload: variable bytes]
      - :erlang.term_to_binary({previous_version, pages_map}, [:compressed])

      [Footer: 4 bytes]
      - payload_size: (4 bytes) - repeated for backward scan

  ## Recovery

  The durable version is the version of the last complete record in the file.
  Recovery reads the last 20 bytes (footer + header start) to extract it.

  ## Reading

  Page blocks are loaded by scanning backward from EOF until the target
  version is found. This supports the iterative page chain loading pattern.
  """

  alias Bedrock.DataPlane.Storage.Olivine.Index.Page
  alias Bedrock.DataPlane.Version

  @magic_number 0x4F4C5644
  @header_size 16
  @footer_size 4
  @min_record_size @header_size + @footer_size

  @type t :: %__MODULE__{
          file: :file.fd(),
          file_offset: non_neg_integer(),
          file_name: [char()],
          durable_version: Bedrock.version(),
          last_block_empty: boolean(),
          last_block_offset: non_neg_integer(),
          last_block_previous_version: Bedrock.version() | nil
        }

  defstruct [
    :file,
    :file_offset,
    :file_name,
    :durable_version,
    :last_block_empty,
    :last_block_offset,
    :last_block_previous_version
  ]

  @spec open(otp_name :: atom(), file_path :: String.t()) ::
          {:ok, t()} | {:error, :system_limit | :badarg | File.posix()}
  def open(_otp_name, file_path) do
    # Replace basename with "idx"
    dir = Path.dirname(file_path)
    path = String.to_charlist(Path.join(dir, "idx"))

    with {:ok, file} <- :file.open(path, [:raw, :binary, :read, :write]),
         {:ok, file_size} <- :file.position(file, {:eof, 0}) do
      durable_version = read_durable_version(file, file_size)

      {:ok,
       %__MODULE__{
         file: file,
         file_name: path,
         file_offset: file_size,
         durable_version: durable_version,
         last_block_empty: false,
         last_block_offset: 0,
         last_block_previous_version: nil
       }}
    end
  end

  @spec close(t()) :: :ok
  def close(index_db) do
    _ = safe_call(fn -> :file.sync(index_db.file) end)
    _ = safe_call(fn -> :file.close(index_db.file) end)
    :ok
  end

  defp safe_call(fun) do
    fun.()
  catch
    _, _ -> :ok
  end

  @spec durable_version(t()) :: Bedrock.version()
  def durable_version(index_db), do: index_db.durable_version

  @spec load_durable_version(t()) ::
          {:ok, Bedrock.version()} | {:error, :not_found}
  def load_durable_version(%{durable_version: version}), do: {:ok, version}

  @spec load_pages_from_version(t(), Bedrock.version()) :: %{Page.id() => {Page.t(), Page.id()}}
  def load_pages_from_version(index_db, version) do
    case load_page_block(index_db, version) do
      {:ok, pages_map, _next_version} -> pages_map
      {:error, :not_found} -> %{}
    end
  end

  @doc """
  Load the page block for a given version, returning the pages and the next version in the chain.
  Returns {pages_map, next_version} where next_version is the previous version that forms a chain.
  """
  @spec load_page_block(t(), Bedrock.version()) ::
          {:ok, %{Page.id() => {Page.t(), Page.id()}}, Bedrock.version() | nil} | {:error, :not_found}
  def load_page_block(index_db, target_version) do
    scan_backward_for_version(index_db.file, index_db.file_offset, target_version)
  end

  @spec flush(
          t(),
          new_durable_version :: Bedrock.version(),
          previous_durable_version :: Bedrock.version(),
          collected_pages :: [%{Page.id() => {Page.t(), Page.id()}}]
        ) :: t()
  def flush(index_db, new_durable_version, previous_durable_version, collected_pages) do
    pages_map = merge_collected_pages(collected_pages)
    current_block_empty = map_size(pages_map) == 0
    should_squash = current_block_empty and index_db.last_block_empty and index_db.last_block_offset > 0

    if should_squash do
      write_record(index_db, new_durable_version, index_db.last_block_previous_version, %{}, :overwrite)
    else
      write_record(index_db, new_durable_version, previous_durable_version, pages_map, :append)
    end
  end

  defp merge_collected_pages(collected_pages) do
    Enum.reduce(collected_pages, %{}, fn modified_pages, acc ->
      Map.merge(modified_pages, acc, fn _page_id, new_page, _old_page -> new_page end)
    end)
  end

  defp write_record(index_db, new_version, previous_version, pages_map, mode) do
    record = build_record(new_version, previous_version, pages_map)
    current_block_empty = map_size(pages_map) == 0

    write_offset = if mode == :overwrite, do: index_db.last_block_offset, else: index_db.file_offset
    new_offset = write_offset + :erlang.iolist_size(record)

    :ok = :file.pwrite(index_db.file, write_offset, record)

    # Truncate file when overwriting to remove any stale data
    if mode == :overwrite do
      {:ok, _} = :file.position(index_db.file, new_offset)
      :ok = :file.truncate(index_db.file)
    end

    %{
      index_db
      | file_offset: new_offset,
        durable_version: new_version,
        last_block_empty: current_block_empty,
        last_block_offset: write_offset,
        last_block_previous_version: previous_version
    }
  end

  defp build_record(version, previous_version, pages_map) do
    payload = :erlang.term_to_binary({previous_version, pages_map})
    payload_size = byte_size(payload)
    [<<@magic_number::32, version::binary-size(8), payload_size::32>>, payload, <<payload_size::32>>]
  end

  @spec sync(t()) :: :ok
  def sync(index_db) do
    :file.sync(index_db.file)
    :ok
  catch
    _, _ -> :ok
  end

  @doc """
  Writes a snapshot block to a file descriptor.
  Used during compaction to create a single version block with all current pages.
  The previous_version equals version (self-loop) to terminate the chain.
  """
  @spec write_snapshot_block(:file.fd(), Bedrock.version(), %{Page.id() => {Page.t(), Page.id()}}) :: :ok
  def write_snapshot_block(file_fd, version, pages_map) do
    # Self-referential previous_version terminates the chain
    record = build_record(version, version, pages_map)
    :file.write(file_fd, record)
  end

  @spec info(t(), :n_keys | :utilization | :size_in_bytes | :key_ranges) :: any() | :undefined
  def info(index_db, stat) do
    case stat do
      :size_in_bytes -> index_db.file_offset
      :n_keys -> count_records(index_db.file, index_db.file_offset)
      :utilization -> 0.75
      :key_ranges -> []
      _ -> :undefined
    end
  end

  # Private functions

  defp read_durable_version(_file, file_size) when file_size < @min_record_size do
    Version.zero()
  end

  defp read_durable_version(file, file_size) do
    with {:ok, <<payload_size::32>>} <- :file.pread(file, file_size - @footer_size, @footer_size),
         record_size = @header_size + payload_size + @footer_size,
         true <- record_size <= file_size,
         header_offset = file_size - record_size,
         {:ok, <<@magic_number::32, version::binary-size(8), ^payload_size::32>>} <-
           :file.pread(file, header_offset, @header_size) do
      version
    else
      _ -> Version.zero()
    end
  end

  defp scan_backward_for_version(_file, current_offset, _target_version) when current_offset < @min_record_size do
    {:error, :not_found}
  end

  defp scan_backward_for_version(file, current_offset, target_version) do
    with {:ok, <<payload_size::32>>} <- :file.pread(file, current_offset - @footer_size, @footer_size),
         record_size = @header_size + payload_size + @footer_size,
         record_offset = current_offset - record_size,
         true <- record_offset >= 0,
         {:ok,
          <<@magic_number::32, version::binary-size(8), ^payload_size::32, payload::binary-size(payload_size),
            ^payload_size::32>>} <- :file.pread(file, record_offset, record_size) do
      if version == target_version do
        {previous_version, pages_map} = :erlang.binary_to_term(payload)
        next_version = if previous_version == version, do: nil, else: previous_version
        {:ok, pages_map, next_version}
      else
        scan_backward_for_version(file, record_offset, target_version)
      end
    else
      _ -> {:error, :not_found}
    end
  end

  defp count_records(_file, file_size) when file_size < @min_record_size, do: 0
  defp count_records(file, file_size), do: scan_count_backward(file, file_size, 0)

  defp scan_count_backward(_file, current_offset, count) when current_offset < @min_record_size, do: count

  defp scan_count_backward(file, current_offset, count) do
    with {:ok, <<payload_size::32>>} <- :file.pread(file, current_offset - @footer_size, @footer_size),
         record_size = @header_size + payload_size + @footer_size,
         record_offset = current_offset - record_size,
         true <- record_offset >= 0 do
      scan_count_backward(file, record_offset, count + 1)
    else
      _ -> count
    end
  end
end
