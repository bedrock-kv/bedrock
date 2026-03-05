defmodule Bedrock.DataPlane.Materializer.Olivine.CompactionWriter.Bundle do
  @moduledoc """
  CompactionWriter implementation that writes to a single bundle file.

  Bundle format:
    [data bytes]
    [index record]  <- existing IndexDatabase format (magic 0x4F4C5644)

  The data_end_offset marks where data ends and index begins.
  This format is suitable for ObjectStorage upload as a single file.
  """

  @behaviour Bedrock.DataPlane.Materializer.Olivine.CompactionWriter

  @type t :: %__MODULE__{
          fd: :file.fd(),
          path: charlist(),
          data_end_offset: non_neg_integer()
        }

  defstruct [:fd, :path, data_end_offset: 0]

  @type result :: %{
          fd: :file.fd(),
          path: charlist(),
          data_end_offset: non_neg_integer(),
          total_size: non_neg_integer()
        }

  @doc """
  Create a new Bundle writer for the given path.
  """
  @spec new(path :: charlist()) :: {:ok, t()} | {:error, term()}
  def new(path) do
    case :file.open(path, [:write, :raw, :binary]) do
      {:ok, fd} ->
        {:ok, %__MODULE__{fd: fd, path: path, data_end_offset: 0}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl true
  @spec write_data(t(), iodata()) :: {:ok, t()} | {:error, term()}
  def write_data(%__MODULE__{} = writer, iodata) do
    case :file.write(writer.fd, iodata) do
      :ok ->
        size = IO.iodata_length(iodata)
        {:ok, %{writer | data_end_offset: writer.data_end_offset + size}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl true
  @spec write_index(t(), iodata()) :: {:ok, t()} | {:error, term()}
  def write_index(%__MODULE__{} = writer, iodata) do
    case :file.write(writer.fd, iodata) do
      :ok -> {:ok, writer}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  @spec finish(t()) :: {:ok, result()} | {:error, term()}
  def finish(%__MODULE__{} = writer) do
    with :ok <- :file.sync(writer.fd),
         {:ok, total_size} <- :file.position(writer.fd, {:cur, 0}) do
      {:ok,
       %{
         fd: writer.fd,
         path: writer.path,
         data_end_offset: writer.data_end_offset,
         total_size: total_size
       }}
    end
  end
end
