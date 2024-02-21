defmodule Bedrock.Service.TransactionLogWorker.Limestone.UnusedSegments do
  @moduledoc """
  A simple implementation of a segment manager that keeps track of unused
  segments in a directory. This is useful for managing pre-allocated segments
  that can be used to store transaction log entries.
  """
  alias Bedrock.Service.TransactionLogWorker.Limestone.Segment

  defstruct [:path, :segments, :size, next_id: 0]
  @type t :: %__MODULE__{}

  @unused_file_prefix "preallocated"

  @spec new(path_to_dir :: binary(), size :: non_neg_integer()) :: {:ok, t()} | {:error, atom()}
  def new(path_to_dir, size) do
    with true <- File.dir?(path_to_dir) || {:error, :path_is_not_a_directory},
         segments <- load_from_path(path_to_dir),
         highest_id <- find_highest_id(segments) do
      {:ok,
       %__MODULE__{
         path: path_to_dir,
         segments: segments,
         size: size,
         next_id: highest_id + 1
       }}
    end
  end

  @spec new!(path_to_dir :: binary(), size :: non_neg_integer()) :: t()
  def new!(path_to_dir, size) do
    new(path_to_dir, size)
    |> case do
      {:ok, usf} -> usf
      {:error, reason} -> raise reason
    end
  end

  @spec load_from_path(dir_path :: binary()) :: [Segment.t()]
  defp load_from_path(path) do
    Path.wildcard(Path.join(path, "#{@unused_file_prefix}.*"))
    |> Enum.reduce([], fn
      _, {:error, _} = error ->
        error

      file_name, segments ->
        Segment.from_path(file_name)
        |> case do
          {:ok, segment} -> [segment | segments]
          {:error, _} -> segments
        end
    end)
  end

  @spec check_out(t(), new_name :: String.t()) :: {:ok, Segment.t(), t()} | {:error, atom()}
  def check_out(%{segments: []}, _new_name), do: {:error, :unavailable}

  def check_out(usf, new_name) do
    [first_segment | remaining_segments] = usf.segments

    first_segment
    |> Segment.rename(new_name)
    |> case do
      {:ok, renamed} -> {:ok, renamed, %{usf | segments: remaining_segments}}
      {:error, _reason} = error -> error
    end
  end

  @spec check_in(t(), Segment.t()) :: {:ok, t()} | {:error, atom()}
  def check_in(usf, file) do
    with new_name <- Path.join(usf.path, "#{@unused_file_prefix}.#{usf.next_id}"),
         {:ok, renamed} <- Segment.rename(file, new_name),
         :ok <- Segment.clear(renamed) do
      {:ok, %{usf | segments: [renamed | usf.segments], next_id: usf.next_id + 1}}
    end
  end

  @spec ensure_minimum_available(t(), non_neg_integer()) :: {:ok, t()} | {:error, atom()}
  def ensure_minimum_available(_usf, n) when n < 0, do: raise("n must be >= 0")

  def ensure_minimum_available(%{segments: segments} = usf, n) when n >= length(segments),
    do: {:ok, usf}

  def ensure_minimum_available(usf, 0),
    do: {:ok, usf}

  def ensure_minimum_available(usf, n) do
    with {:ok, segment} <- Segment.allocate(usf.path, usf.size) do
      %{usf | segments: [segment | usf.segments]}
      |> ensure_minimum_available(n - 1)
    end
  end

  defp find_highest_id(segments) do
    segments
    |> Enum.map(&(&1.path |> Path.basename() |> String.split(".")))
    |> Enum.map(fn [@unused_file_prefix, id] -> String.to_integer(id) end)
    |> Enum.max()
  end
end
