defmodule Bedrock.DataPlane.Log.Shale.Segment do
  @moduledoc false
  alias Bedrock.DataPlane.Log.Shale.SegmentRecycler
  alias Bedrock.DataPlane.Log.Shale.TransactionStreams
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version

  require Logger

  @type t :: %__MODULE__{
          path: String.t(),
          min_version: Bedrock.version(),
          transactions: nil | [Transaction.encoded()]
        }
  defstruct path: nil,
            min_version: nil,
            transactions: nil

  @wal_prefix "wal_"
  @spec file_prefix() :: String.t()
  def file_prefix, do: @wal_prefix

  @spec encode_file_name(pos_integer()) :: String.t()
  def encode_file_name(n) do
    log_number = n |> Integer.to_string(32) |> String.downcase() |> String.pad_leading(13, "0")
    @wal_prefix <> log_number
  end

  @spec decode_file_name(String.t()) :: pos_integer()
  def decode_file_name(@wal_prefix <> log_number), do: String.to_integer(log_number, 32)

  @spec allocate_from_recycler(SegmentRecycler.server(), String.t(), Bedrock.version()) ::
          {:ok, t()} | {:error, :allocation_failed}
  def allocate_from_recycler(segment_recycler, path, version) do
    path_to_file = Path.join(path, encode_file_name(Version.to_integer(version)))

    case SegmentRecycler.check_out(segment_recycler, path_to_file) do
      :ok ->
        {:ok,
         %__MODULE__{
           min_version: version,
           path: path_to_file
         }}

      _ ->
        {:error, :allocation_failed}
    end
  end

  @spec allocate_from_recycler!(SegmentRecycler.server(), String.t(), Bedrock.version()) :: t()
  def allocate_from_recycler!(segment_recycler, path, version) do
    case allocate_from_recycler(segment_recycler, path, version) do
      {:ok, segment} ->
        segment

      {:error, :allocation_failed} ->
        raise "Failed to allocate segment from recycler for path=#{inspect(path)}, version=#{inspect(version)}"
    end
  end

  @spec return_to_recycler(t(), SegmentRecycler.server()) :: :ok
  def return_to_recycler(segment, segment_recycler), do: SegmentRecycler.check_in(segment_recycler, segment.path)

  @doc """
  Create a new segment from the given file path. We stat the file to get the
  size, ensuring that it exists.
  """
  @spec from_path(path_to_file :: String.t()) :: {:ok, t()} | {:error, :does_not_exist}
  def from_path(path_to_file) do
    if File.exists?(path_to_file) do
      {:ok,
       %__MODULE__{
         path: path_to_file,
         min_version: path_to_file |> Path.basename() |> decode_file_name() |> Version.from_integer()
       }}
    else
      {:error, :does_not_exist}
    end
  end

  @spec ensure_transactions_are_loaded(t()) :: t()
  def ensure_transactions_are_loaded(%{transactions: nil} = segment) do
    segment.path
    |> TransactionStreams.from_file!()
    |> Enum.reverse()
    |> case do
      [{:corrupted, offset} | transactions] ->
        Logger.error("Segment corruption detected",
          segment_path: segment.path,
          corruption_offset: offset,
          min_version: segment.min_version,
          recovered_transactions: length(transactions)
        )

        %{segment | transactions: transactions}

      transactions ->
        %{segment | transactions: transactions}
    end
  end

  def ensure_transactions_are_loaded(segment), do: segment

  @spec transactions(t()) :: [Transaction.encoded()]
  def transactions(segment) do
    segment
    |> ensure_transactions_are_loaded()
    |> Map.get(:transactions, [])
  end

  def oldest_version(%{min_version: min_version}), do: min_version

  def last_version(%{transactions: []}), do: nil
  def last_version(%{transactions: [transaction | _]}), do: Transaction.commit_version!(transaction)
end
