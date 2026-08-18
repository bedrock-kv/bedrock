defmodule Bedrock.DataPlane.Log.Shale.ColdStarting do
  @moduledoc false

  alias Bedrock.DataPlane.Log.Shale.Segment

  @spec reload_segments_at_path(segment_dir :: String.t()) ::
          {:ok, [Segment.t()]}
          | {:error, {:unable_to_list_segments, File.posix()}}
          | {:error, {:unsupported_wal_format | :invalid_wal_format, String.t()}}
  def reload_segments_at_path(segment_dir) do
    segment_dir
    |> File.ls()
    |> case do
      {:ok, files} ->
        files
        |> Enum.filter(&String.starts_with?(&1, Segment.file_prefix()))
        |> Enum.sort(:desc)
        |> Enum.reduce_while({:ok, []}, fn file_name, {:ok, segments} ->
          case Segment.from_path(Path.join(segment_dir, file_name)) do
            {:ok, segment} -> {:cont, {:ok, [segment | segments]}}
            {:error, reason} -> {:halt, {:error, reason}}
          end
        end)
        |> case do
          {:ok, segments} -> {:ok, Enum.sort_by(segments, & &1.min_version, :desc)}
          error -> error
        end

      {:error, posix} ->
        {:error, {:unable_to_list_segments, posix}}
    end
  end
end
