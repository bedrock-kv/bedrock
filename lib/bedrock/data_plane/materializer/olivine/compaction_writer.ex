defmodule Bedrock.DataPlane.Materializer.Olivine.CompactionWriter do
  @moduledoc """
  Behaviour for compaction output writers.

  Allows compaction to output to different formats:
  - SplitFile: Two separate files (data + index) for local use
  - Bundle: Single file with data followed by index, for ObjectStorage upload
  """

  @type t :: term()
  @type result :: term()

  @doc """
  Write data bytes to the compacted output.
  """
  @callback write_data(t(), iodata()) :: {:ok, t()} | {:error, term()}

  @doc """
  Write index bytes to the compacted output.
  """
  @callback write_index(t(), iodata()) :: {:ok, t()} | {:error, term()}

  @doc """
  Finish writing and return the result.
  Implementations should sync files to disk before returning.
  """
  @callback finish(t()) :: {:ok, result()} | {:error, term()}

  @doc """
  Dispatch write_data to the implementation module.
  """
  @spec write_data(module(), t(), iodata()) :: {:ok, t()} | {:error, term()}
  def write_data(module, writer, iodata), do: module.write_data(writer, iodata)

  @doc """
  Dispatch write_index to the implementation module.
  """
  @spec write_index(module(), t(), iodata()) :: {:ok, t()} | {:error, term()}
  def write_index(module, writer, iodata), do: module.write_index(writer, iodata)

  @doc """
  Dispatch finish to the implementation module.
  """
  @spec finish(module(), t()) :: {:ok, result()} | {:error, term()}
  def finish(module, writer), do: module.finish(writer)
end
