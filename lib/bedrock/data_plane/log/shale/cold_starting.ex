defmodule Bedrock.DataPlane.Log.Shale.ColdStarting do
  @moduledoc false

  alias Bedrock.DataPlane.Log.Shale.Segment
  alias Bedrock.DataPlane.Version

  @spec reload_segments_at_path(segment_dir :: String.t()) ::
          {:ok, [Segment.t()]}
          | {:error, {:unable_to_list_segments, File.posix()}}
  def reload_segments_at_path(segment_dir) do
    segment_dir
    |> File.ls()
    |> case do
      {:ok, files} ->
        files
        |> Enum.filter(&String.starts_with?(&1, Segment.file_prefix()))
        |> Enum.map(fn file_name ->
          path = Path.join(segment_dir, file_name)

          min_version =
            file_name
            |> String.replace_prefix(Segment.file_prefix(), "")
            |> String.replace_suffix(".log", "")
            |> String.to_integer(32)
            |> Version.from_integer()

          {min_version, path}
        end)
        |> Enum.sort_by(&elem(&1, 0), :desc)
        |> Enum.map(fn {min_version, path} ->
          %Segment{min_version: min_version, path: path}
        end)
        |> then(&{:ok, &1})

      {:error, posix} ->
        {:error, {:unable_to_list_segments, posix}}
    end
  end
end
