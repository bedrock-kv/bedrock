defmodule Bedrock.Test.Chaos.Journal do
  @moduledoc """
  An append-only record of commits the client saw acked, written outside the
  cluster.

  Each entry is written and fsynced *as its commit is acked*, before the writer
  does anything else. Buffering entries and flushing at the end would lose
  exactly the evidence that matters: what the client was promised by a cluster
  that then wedged, or by a node that was killed a moment later. An entry in the
  journal is therefore a claim the database has to honour.

  One file per writer. Concurrent appends to a shared file can interleave
  mid-entry, and the harness would rather have N small files than one file whose
  damage it has to reason about. A writer's last entry can still be torn if the
  process dies mid-write, so `read_all/1` drops a trailing undecodable line and
  reports the count instead of raising — a torn tail means the ack was never
  fully journaled, which is the safe direction. Damage anywhere earlier is real
  corruption and does raise.

  Entries are base64 of `:erlang.term_to_binary/1`, one per line, so a line is
  self-delimiting and contains no newlines of its own.
  """

  @type t :: %__MODULE__{io: :file.io_device(), path: Path.t()}
  @enforce_keys [:io, :path]
  defstruct [:io, :path]

  @doc """
  Open (or create) the journal file for `writer_id` under `dir`.
  """
  @spec open!(Path.t(), term()) :: t()
  def open!(dir, writer_id) do
    File.mkdir_p!(dir)
    path = Path.join(dir, "#{writer_id}.journal")
    io = File.open!(path, [:append, :raw, :binary])
    %__MODULE__{io: io, path: path}
  end

  @doc """
  Append one entry and fsync it.

  The fsync is the point of the journal. Without it the entry lives in the
  page cache of a machine the next ticket is going to start killing.
  """
  @spec append!(t(), term()) :: :ok
  def append!(%__MODULE__{io: io}, entry) do
    :ok = :file.write(io, [entry |> :erlang.term_to_binary() |> Base.encode64(), ?\n])
    :ok = :file.datasync(io)
    :ok
  end

  @spec close(t()) :: :ok
  def close(%__MODULE__{io: io}), do: :file.close(io)

  @doc """
  Read every entry every writer journalled under `dir`.

  Returns the entries in no particular order across writers (each writer's own
  entries keep their order) along with the number of torn trailing entries
  dropped.
  """
  @spec read_all(Path.t()) :: %{entries: [term()], torn: non_neg_integer()}
  def read_all(dir) do
    dir
    |> Path.join("*.journal")
    |> Path.wildcard()
    |> Enum.map(&read_file!/1)
    |> Enum.reduce(%{entries: [], torn: 0}, fn file, acc ->
      %{entries: file.entries ++ acc.entries, torn: file.torn + acc.torn}
    end)
  end

  defp read_file!(path) do
    lines = path |> File.read!() |> String.split("\n", trim: true)

    case Enum.split(lines, max(length(lines) - 1, 0)) do
      {complete, []} ->
        %{entries: Enum.map(complete, &decode!(&1, path)), torn: 0}

      {complete, [last]} ->
        entries = Enum.map(complete, &decode!(&1, path))

        case decode(last) do
          {:ok, entry} -> %{entries: entries ++ [entry], torn: 0}
          :error -> %{entries: entries, torn: 1}
        end
    end
  end

  defp decode!(line, path) do
    case decode(line) do
      {:ok, entry} -> entry
      :error -> raise "Corrupt journal entry in #{path}: #{inspect(line)}"
    end
  end

  defp decode(line) do
    with {:ok, binary} <- Base.decode64(line) do
      {:ok, :erlang.binary_to_term(binary, [:safe])}
    end
  rescue
    ArgumentError -> :error
  end
end
