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
