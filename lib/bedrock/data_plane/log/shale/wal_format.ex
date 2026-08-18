defmodule Bedrock.DataPlane.Log.Shale.WalFormat do
  @moduledoc false

  alias Bedrock.DataPlane.Version

  @bed0_magic "BED0"
  @bed1_magic "BED1"
  @eof_version <<0xFFFFFFFFFFFFFFFF::unsigned-big-64>>
  @eof_marker <<@eof_version::binary, 0::unsigned-big-32, 0::unsigned-big-32>>
  @bed0_header_size 4
  @bed1_header_size 12
  @prefix_size @bed1_header_size + byte_size(@eof_marker)

  @enforce_keys [:version, :header_size, :previous_version]
  defstruct [:version, :header_size, :previous_version, :first_version]

  @type t :: %__MODULE__{
          version: :bed0 | :bed1,
          header_size: 4 | 12,
          previous_version: Bedrock.version(),
          first_version: Bedrock.version() | nil
        }

  @type format_error :: :unsupported_wal_format | :invalid_wal_format

  @spec decode(binary()) :: {:ok, t()} | {:error, format_error()}
  def decode(<<@bed1_magic, previous_version::binary-size(8), _::binary>>) do
    {:ok,
     %__MODULE__{
       version: :bed1,
       header_size: @bed1_header_size,
       previous_version: previous_version
     }}
  end

  def decode(<<@bed0_magic, @eof_marker, _::binary>>), do: {:error, :unsupported_wal_format}
  def decode(<<@bed0_magic, @eof_version, _::binary>>), do: {:error, :invalid_wal_format}

  def decode(<<@bed0_magic, first_version::binary-size(8), _size::unsigned-big-32, _::binary-size(4), _::binary>>) do
    {:ok,
     %__MODULE__{
       version: :bed0,
       header_size: @bed0_header_size,
       previous_version: legacy_cursor(first_version),
       first_version: first_version
     }}
  end

  def decode(_), do: {:error, :invalid_wal_format}

  @spec read(Path.t()) :: {:ok, t()} | {:error, format_error() | File.posix()}
  def read(path) do
    with {:ok, fd} <- File.open(path, [:read, :raw, :binary]) do
      result =
        case :file.pread(fd, 0, @prefix_size) do
          {:ok, prefix} -> decode(prefix)
          :eof -> {:error, :invalid_wal_format}
          {:error, reason} -> {:error, reason}
        end

      :ok = File.close(fd)
      result
    end
  end

  @spec previous_version(Path.t()) :: {:ok, Bedrock.version()} | {:error, format_error() | File.posix()}
  def previous_version(path) do
    with {:ok, format} <- read(path), do: {:ok, format.previous_version}
  end

  @spec split(binary()) :: {:ok, t(), binary()} | {:error, format_error()}
  def split(<<@bed1_magic, _previous_version::binary-size(8), entries::binary>> = wal) do
    with {:ok, format} <- decode(wal), do: {:ok, format, entries}
  end

  def split(<<@bed0_magic, entries::binary>> = wal) do
    with {:ok, format} <- decode(wal), do: {:ok, format, entries}
  end

  def split(_), do: {:error, :invalid_wal_format}

  @spec empty_segment(Bedrock.version()) :: binary()
  def empty_segment(<<_::unsigned-big-64>> = previous_version),
    do: <<@bed1_magic, previous_version::binary, @eof_marker>>

  @spec current_header_size() :: 12
  def current_header_size, do: @bed1_header_size

  @spec eof_marker() :: binary()
  def eof_marker, do: @eof_marker

  @spec eof_version?(Bedrock.version()) :: boolean()
  def eof_version?(@eof_version), do: true
  def eof_version?(<<_::unsigned-big-64>>), do: false

  defp legacy_cursor(<<0::unsigned-big-64>>), do: Version.zero()
  defp legacy_cursor(<<version::unsigned-big-64>>), do: Version.from_integer(version - 1)
end
