defmodule Bedrock.DataPlane.Storage.Olivine.CompactionWriter.SplitFile do
  @moduledoc """
  CompactionWriter implementation that writes to two separate files.

  This is the current behavior used for local storage: data goes to a .data file
  and index goes to a separate .idx file.
  """

  @behaviour Bedrock.DataPlane.Storage.Olivine.CompactionWriter

  @type t :: %__MODULE__{
          data_fd: :file.fd(),
          idx_fd: :file.fd(),
          data_path: charlist(),
          idx_path: charlist(),
          data_offset: non_neg_integer()
        }

  defstruct [:data_fd, :idx_fd, :data_path, :idx_path, data_offset: 0]

  @type result :: %{
          data_fd: :file.fd(),
          idx_fd: :file.fd(),
          data_path: charlist(),
          idx_path: charlist(),
          data_offset: non_neg_integer(),
          idx_offset: non_neg_integer()
        }

  @doc """
  Create a new SplitFile writer for the given paths.
  """
  @spec new(data_path :: charlist(), idx_path :: charlist()) ::
          {:ok, t()} | {:error, term()}
  def new(data_path, idx_path) do
    with {:ok, data_fd} <- :file.open(data_path, [:write, :raw, :binary]),
         {:ok, idx_fd} <- :file.open(idx_path, [:write, :raw, :binary]) do
      {:ok,
       %__MODULE__{
         data_fd: data_fd,
         idx_fd: idx_fd,
         data_path: data_path,
         idx_path: idx_path,
         data_offset: 0
       }}
    end
  end

  @impl true
  @spec write_data(t(), iodata()) :: {:ok, t()} | {:error, term()}
  def write_data(%__MODULE__{} = writer, iodata) do
    case :file.write(writer.data_fd, iodata) do
      :ok ->
        size = IO.iodata_length(iodata)
        {:ok, %{writer | data_offset: writer.data_offset + size}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl true
  @spec write_index(t(), iodata()) :: {:ok, t()} | {:error, term()}
  def write_index(%__MODULE__{} = writer, iodata) do
    case :file.write(writer.idx_fd, iodata) do
      :ok -> {:ok, writer}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  @spec finish(t()) :: {:ok, result()} | {:error, term()}
  def finish(%__MODULE__{} = writer) do
    with :ok <- :file.sync(writer.data_fd),
         :ok <- :file.sync(writer.idx_fd),
         {:ok, idx_offset} <- :file.position(writer.idx_fd, {:cur, 0}) do
      {:ok,
       %{
         data_fd: writer.data_fd,
         idx_fd: writer.idx_fd,
         data_path: writer.data_path,
         idx_path: writer.idx_path,
         data_offset: writer.data_offset,
         idx_offset: idx_offset
       }}
    end
  end
end
