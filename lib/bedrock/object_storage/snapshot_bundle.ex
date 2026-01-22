defmodule Bedrock.ObjectStorage.SnapshotBundle do
  @moduledoc """
  Utilities for working with snapshot bundles.

  A snapshot bundle is a single file containing:
    [data bytes]
    [index record]  <- IndexDatabase format (magic 0x4F4C5644)

  This module provides utilities to split a downloaded bundle into separate
  data and index files for use with the local Olivine storage engine.
  """

  # IndexDatabase constants (must match index_database.ex)
  @magic_number 0x4F4C5644
  @header_size 16
  @footer_size 4
  @min_record_size @header_size + @footer_size

  @doc """
  Create a bundle file from separate data and index files.

  Concatenates data and index into a single bundle file.
  Returns {:ok, bundle_size} on success.
  """
  @spec create(data_path :: Path.t(), idx_path :: Path.t(), bundle_path :: Path.t()) ::
          {:ok, bundle_size :: non_neg_integer()} | {:error, term()}
  def create(data_path, idx_path, bundle_path) do
    with {:ok, data} <- File.read(data_path),
         {:ok, idx} <- File.read(idx_path),
         bundle = [data, idx],
         :ok <- File.write(bundle_path, bundle) do
      {:ok, byte_size(data) + byte_size(idx)}
    end
  end

  @doc """
  Split a bundle file into separate data and index files.

  Reads the bundle, identifies the index record at the end, and writes:
  - data_path: All bytes before the index record
  - idx_path: The complete index record

  Returns {:ok, data_size, idx_size} on success.
  """
  @spec split(bundle_path :: Path.t(), data_path :: Path.t(), idx_path :: Path.t()) ::
          {:ok, data_size :: non_neg_integer(), idx_size :: non_neg_integer()}
          | {:error, term()}
  def split(bundle_path, data_path, idx_path) do
    with {:ok, bundle} <- File.read(bundle_path),
         {:ok, data_end_offset, idx_size} <- find_index_boundary(bundle),
         data = binary_part(bundle, 0, data_end_offset),
         idx = binary_part(bundle, data_end_offset, idx_size),
         :ok <- File.write(data_path, data),
         :ok <- File.write(idx_path, idx) do
      {:ok, data_end_offset, idx_size}
    end
  end

  @doc """
  Split a bundle file in place, avoiding data copies.

  This is more efficient than `split/3` for large bundles:
  1. Reads only the index portion (small) from the end of the bundle
  2. Writes index to a separate file
  3. Truncates the bundle at the data boundary (no copy!)
  4. Renames the truncated bundle to the data path

  The bundle file is consumed (renamed to data_path).
  Returns {:ok, data_size, idx_size} on success.
  """
  @spec split_in_place(bundle_path :: Path.t(), data_path :: Path.t(), idx_path :: Path.t()) ::
          {:ok, data_size :: non_neg_integer(), idx_size :: non_neg_integer()}
          | {:error, term()}
  def split_in_place(bundle_path, data_path, idx_path) do
    with {:ok, %{size: bundle_size}} <- File.stat(bundle_path),
         {:ok, data_end, idx_size} <- find_index_boundary_from_file(bundle_path, bundle_size),
         {:ok, idx} <- read_bytes_at(bundle_path, data_end, idx_size),
         :ok <- File.write(idx_path, idx),
         :ok <- truncate_file(bundle_path, data_end),
         :ok <- File.rename(bundle_path, data_path) do
      {:ok, data_end, idx_size}
    end
  end

  @doc """
  Find the index boundary by reading only the footer and header from a file.

  More efficient than `find_index_boundary/1` for large files since it
  doesn't read the entire bundle into memory.
  """
  @spec find_index_boundary_from_file(Path.t(), non_neg_integer()) ::
          {:ok, data_end_offset :: non_neg_integer(), idx_size :: non_neg_integer()}
          | {:error, :invalid_bundle | :no_index_record | term()}
  def find_index_boundary_from_file(_path, file_size) when file_size < @min_record_size do
    {:error, :invalid_bundle}
  end

  def find_index_boundary_from_file(path, file_size) do
    # Read footer (last 4 bytes) to get payload size
    with {:ok, <<payload_size::32>>} <- read_bytes_at(path, file_size - @footer_size, @footer_size) do
      idx_size = @header_size + payload_size + @footer_size
      data_end_offset = file_size - idx_size

      if data_end_offset >= 0 do
        # Read header to validate magic number
        case read_bytes_at(path, data_end_offset, @header_size) do
          {:ok, <<@magic_number::32, _rest::binary>>} ->
            {:ok, data_end_offset, idx_size}

          {:ok, _} ->
            {:error, :no_index_record}

          error ->
            error
        end
      else
        {:error, :invalid_bundle}
      end
    end
  end

  # Read a specific range of bytes from a file
  defp read_bytes_at(path, offset, size) do
    case :file.open(to_charlist(path), [:read, :raw, :binary]) do
      {:ok, fd} ->
        result = :file.pread(fd, offset, size)
        :file.close(fd)
        result

      error ->
        error
    end
  end

  # Truncate a file at the given offset
  defp truncate_file(path, offset) do
    case :file.open(to_charlist(path), [:read, :write, :raw]) do
      {:ok, fd} ->
        {:ok, _} = :file.position(fd, offset)
        :ok = :file.truncate(fd)
        :file.close(fd)

      error ->
        error
    end
  end

  @doc """
  Find where the index record starts in a bundle.

  Returns {:ok, data_end_offset, idx_size} where:
  - data_end_offset: Byte offset where data ends and index begins
  - idx_size: Size of the index record in bytes
  """
  @spec find_index_boundary(binary()) ::
          {:ok, data_end_offset :: non_neg_integer(), idx_size :: non_neg_integer()}
          | {:error, :invalid_bundle | :no_index_record}
  def find_index_boundary(bundle) when byte_size(bundle) < @min_record_size do
    {:error, :invalid_bundle}
  end

  def find_index_boundary(bundle) do
    bundle_size = byte_size(bundle)

    # Read footer to get payload size
    footer_offset = bundle_size - @footer_size
    <<payload_size::32>> = binary_part(bundle, footer_offset, @footer_size)

    # Calculate index record size and position
    idx_size = @header_size + payload_size + @footer_size
    data_end_offset = bundle_size - idx_size

    # Validate the index record has correct magic number
    if data_end_offset >= 0 do
      <<magic::32, _rest::binary>> = binary_part(bundle, data_end_offset, @header_size)

      if magic == @magic_number do
        {:ok, data_end_offset, idx_size}
      else
        {:error, :no_index_record}
      end
    else
      {:error, :invalid_bundle}
    end
  end
end
