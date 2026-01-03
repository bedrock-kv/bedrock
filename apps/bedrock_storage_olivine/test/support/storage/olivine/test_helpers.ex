defmodule Bedrock.DataPlane.Storage.Olivine.TestHelpers do
  @moduledoc """
  Test helpers for Olivine storage testing.
  """

  alias Bedrock.DataPlane.Storage.Olivine.IndexDatabase
  alias Bedrock.DataPlane.Version

  @doc """
  Load version range metadata only (without loading all pages into memory).
  Returns version ranges with empty page maps for testing purposes.
  """
  @spec load_version_range_metadata(IndexDatabase.t()) :: [{Bedrock.version(), Bedrock.version()}]
  def load_version_range_metadata(index_db) do
    # Scan the append-only file to extract version metadata
    index_db.file
    |> scan_all_version_metadata(index_db.file_offset, [])
    |> Enum.sort_by(fn {version, _last_version} -> version end, &(Version.compare(&1, &2) != :lt))
  end

  # Constants matching IndexDatabase
  @magic_number 0x4F4C5644
  @header_size 16
  @footer_size 4
  @min_record_size @header_size + @footer_size

  defp scan_all_version_metadata(_file, current_offset, acc) when current_offset < @min_record_size,
    do: Enum.reverse(acc)

  defp scan_all_version_metadata(file, current_offset, acc) do
    with {:ok, <<payload_size::32>>} <- :file.pread(file, current_offset - @footer_size, @footer_size),
         record_size = @header_size + payload_size + @footer_size,
         record_offset = current_offset - record_size,
         true <- record_offset >= 0,
         {:ok,
          <<@magic_number::32, version::binary-size(8), ^payload_size::32, payload::binary-size(payload_size),
            ^payload_size::32>>} <- :file.pread(file, record_offset, record_size) do
      {previous_version, _pages_map} = :erlang.binary_to_term(payload)
      scan_all_version_metadata(file, record_offset, [{version, previous_version} | acc])
    else
      _ -> Enum.reverse(acc)
    end
  end
end
