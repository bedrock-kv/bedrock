defmodule Bedrock.ControlPlane.Coordinator.DiskRaftLog do
  @moduledoc """
  A disk-based implementation of the Raft log using Erlang's :disk_log.

  This module provides persistent storage for Raft consensus operations,
  ensuring that log entries survive process and node restarts.

  ## Design

  - Uses `:disk_log` with `:internal` format for native Erlang term storage
  - Stores entries as `{{term, sequence}, term, data}` tuples
  - Maintains in-memory index for fast transaction ID lookups
  - Supports configurable sync policies for durability vs performance

  ## File Layout

  Coordinator follows standard Bedrock working directory pattern:

      /data/coordinator/      # Base path from config[:coordinator][:path]
      └── raft/               # Coordinator working directory
          ├── raft_log.LOG    # Main log file
          └── raft_log.IDX    # Index file (managed by disk_log)

  """

  alias Bedrock.Raft
  alias Bedrock.Raft.TransactionID

  @initial_transaction_id TransactionID.new(0, 0)

  @type t :: %__MODULE__{
          log_name: atom(),
          log_file: charlist(),
          entries: [Raft.transaction()],
          index: %{Raft.transaction_id() => non_neg_integer()},
          last_commit: Raft.transaction_id(),
          is_open: boolean()
        }

  defstruct [
    :log_name,
    :log_file,
    entries: [],
    index: %{},
    last_commit: @initial_transaction_id,
    is_open: false
  ]

  @doc """
  Create a new disk-based raft log.

  ## Options

    * `:log_dir` - Directory to store log files (required)
    * `:log_name` - Name for the disk_log (default: :raft_log)
    * `:sync_policy` - :immediate or :periodic (default: :immediate)

  ## Examples

      iex> log = DiskRaftLog.new(log_dir: "/tmp/raft")
      iex> is_struct(log, DiskRaftLog)
      true

  """
  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    log_dir = Keyword.fetch!(opts, :log_dir)
    log_name = Keyword.get(opts, :log_name, :raft_log)

    # Ensure log directory exists
    File.mkdir_p!(log_dir)

    log_file = Path.join(log_dir, "raft_log.LOG") |> String.to_charlist()

    %__MODULE__{
      log_name: log_name,
      log_file: log_file,
      entries: [],
      index: %{},
      last_commit: @initial_transaction_id,
      is_open: false
    }
  end

  @doc """
  Open the disk log for reading and writing.

  This must be called before any other operations.
  """
  @spec open(t()) :: {:ok, t()} | {:error, term()}
  def open(%__MODULE__{} = log) do
    case :disk_log.open([
           {:name, log.log_name},
           {:file, log.log_file},
           {:type, :halt},
           {:format, :internal},
           {:quiet, true}
         ]) do
      {:ok, _} ->
        # Load all entries and build index
        updated_log = load_entries_and_build_index(log)
        {:ok, %{updated_log | is_open: true}}

      {:repaired, _, _, _} ->
        # disk_log repaired a corrupted file - continue normally
        updated_log = load_entries_and_build_index(log)
        {:ok, %{updated_log | is_open: true}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Close the disk log.
  """
  @spec close(t()) :: :ok
  def close(%__MODULE__{log_name: log_name}) do
    :disk_log.close(log_name)
  end

  @doc """
  Append an entry to the log.

  Entry format: `{transaction_id, term, data}`
  where `transaction_id` is `{term, sequence}`.
  """
  @spec append_entry(t(), Raft.transaction_id(), term(), term()) :: {:ok, t()} | {:error, term()}
  def append_entry(%__MODULE__{} = log, transaction_id, term, data) do
    entry = {transaction_id, term, data}

    case :disk_log.log(log.log_name, entry) do
      :ok ->
        # Update index with new entry position
        # For now, we'll use a simple counter - can optimize later
        position = map_size(log.index) + 1
        updated_index = Map.put(log.index, transaction_id, position)

        {:ok, %{log | index: updated_index}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Read all entries from the log.

  Returns entries in the order they were written.
  """
  @spec read_all_entries(t()) :: {:ok, [term()]} | {:error, term()}
  def read_all_entries(%__MODULE__{log_name: log_name}) do
    entries = read_entries_recursive(log_name, :start, [])
    {:ok, entries}
  rescue
    error -> {:error, error}
  end

  @doc """
  Check if the log contains a specific transaction ID.
  """
  @spec has_transaction_id?(t(), Raft.transaction_id()) :: boolean()
  def has_transaction_id?(%__MODULE__{index: index}, transaction_id) do
    Map.has_key?(index, transaction_id)
  end

  @doc """
  Sync the log to disk to ensure durability.
  """
  @spec sync(t()) :: :ok | {:error, term()}
  def sync(%__MODULE__{log_name: log_name}) do
    :disk_log.sync(log_name)
  end

  # Private functions

  defp load_entries_and_build_index(%__MODULE__{} = log) do
    case read_all_entries(log) do
      {:ok, entries} ->
        index =
          entries
          |> Enum.with_index(1)
          |> Enum.map(fn {{transaction_id, _term, _data}, position} ->
            {transaction_id, position}
          end)
          |> Map.new()

        %{log | entries: entries, index: index}

      {:error, _reason} ->
        # If we can't read entries, start with empty entries and index
        %{log | entries: [], index: %{}}
    end
  end

  defp read_entries_recursive(log_name, continuation, acc) do
    case :disk_log.chunk(log_name, continuation) do
      :eof ->
        Enum.reverse(acc)

      {:error, reason} ->
        raise "Error reading from disk_log: #{inspect(reason)}"

      {next_continuation, terms} ->
        read_entries_recursive(log_name, next_continuation, Enum.reverse(terms) ++ acc)
    end
  end
end

# Implement the Bedrock.Raft.Log protocol
defimpl Bedrock.Raft.Log, for: Bedrock.ControlPlane.Coordinator.DiskRaftLog do
  @type t :: Bedrock.ControlPlane.Coordinator.DiskRaftLog.t()

  alias Bedrock.ControlPlane.Coordinator.DiskRaftLog
  alias Bedrock.Raft.TransactionID

  @initial_transaction_id TransactionID.new(0, 0)

  @impl true
  def new_id(_t, term, sequence), do: TransactionID.new(term, sequence)

  @impl true
  def initial_transaction_id(_t), do: @initial_transaction_id

  @impl true
  def append_transactions(t, @initial_transaction_id, transactions) do
    # First transaction(s) - append to empty log
    case append_transactions_to_disk(t, transactions) do
      {:ok, updated_log} -> {:ok, updated_log}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def append_transactions(t, prev_transaction_id, transactions) do
    # Check if previous transaction exists
    if DiskRaftLog.has_transaction_id?(t, prev_transaction_id) do
      case append_transactions_to_disk(t, transactions) do
        {:ok, updated_log} -> {:ok, updated_log}
        {:error, reason} -> {:error, reason}
      end
    else
      {:error, :prev_transaction_not_found}
    end
  end

  @impl true
  def purge_transactions_after(t, transaction_id) do
    # For disk_log, we need to rebuild the log without entries after transaction_id
    case rebuild_log_up_to(t, transaction_id) do
      {:ok, updated_log} ->
        # Update last_commit if it's beyond the purged point
        new_last_commit = min(t.last_commit, transaction_id)
        {:ok, %{updated_log | last_commit: new_last_commit}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl true
  def commit_up_to(_t, @initial_transaction_id), do: :unchanged

  @impl true
  def commit_up_to(t, transaction_id) when transaction_id > t.last_commit do
    {:ok, %{t | last_commit: transaction_id}}
  end

  @impl true
  def commit_up_to(_t, _transaction_id), do: :unchanged

  @impl true
  def newest_transaction_id(t) do
    case t.entries do
      [] ->
        @initial_transaction_id

      entries ->
        {transaction_id, _term, _data} = List.last(entries)
        transaction_id
    end
  end

  @impl true
  def newest_safe_transaction_id(t), do: t.last_commit

  @impl true
  def has_transaction_id?(_t, @initial_transaction_id), do: true
  @impl true
  def has_transaction_id?(t, transaction_id),
    do: DiskRaftLog.has_transaction_id?(t, transaction_id)

  @impl true
  def transactions_to(t, :newest),
    do: transactions_from(t, @initial_transaction_id, newest_transaction_id(t))

  @impl true
  def transactions_to(t, :newest_safe),
    do: transactions_from(t, @initial_transaction_id, newest_safe_transaction_id(t))

  @impl true
  def transactions_to(t, to), do: transactions_from(t, @initial_transaction_id, to)

  @impl true
  def transactions_from(t, from, :newest),
    do: transactions_from(t, from, newest_transaction_id(t))

  @impl true
  def transactions_from(t, from, :newest_safe),
    do: transactions_from(t, from, newest_safe_transaction_id(t))

  @impl true
  def transactions_from(t, @initial_transaction_id, to) do
    # Return all transactions up to 'to'
    t.entries
    |> Enum.take_while(fn {transaction_id, _term, _data} -> transaction_id <= to end)
    |> Enum.map(fn {transaction_id, _term, data} -> {transaction_id, data} end)
  end

  @impl true
  def transactions_from(t, from, to) do
    # Return transactions after 'from' up to 'to'
    t.entries
    |> Enum.drop_while(fn {transaction_id, _term, _data} -> transaction_id <= from end)
    |> Enum.take_while(fn {transaction_id, _term, _data} -> transaction_id <= to end)
    |> Enum.map(fn {transaction_id, _term, data} -> {transaction_id, data} end)
  end

  # Private helper functions

  defp append_transactions_to_disk(t, transactions) do
    # Convert transactions to our internal format and append to disk_log
    numbered_transactions = number_transactions(t, transactions)

    Enum.each(numbered_transactions, fn {transaction_id, term, data} ->
      entry = {transaction_id, term, data}
      :ok = :disk_log.log(t.log_name, entry)
    end)

    # Update in-memory state
    new_entries = t.entries ++ numbered_transactions
    new_index = update_index(t.index, numbered_transactions)

    updated_log = %{t | entries: new_entries, index: new_index}
    {:ok, updated_log}
  catch
    :error, reason -> {:error, reason}
  end

  defp number_transactions(t, transactions) do
    # Assign sequential transaction IDs to new transactions
    next_sequence =
      case newest_transaction_id(t) do
        @initial_transaction_id -> 1
        {_term, sequence} -> sequence + 1
      end

    transactions
    |> Enum.with_index(next_sequence)
    |> Enum.map(fn {{term, data}, sequence} ->
      # Create proper transaction_id in {term, sequence} format
      transaction_id = {term, sequence}
      {transaction_id, term, data}
    end)
  end

  defp update_index(index, new_transactions) do
    new_entries =
      new_transactions
      |> Enum.with_index(map_size(index) + 1)
      |> Enum.map(fn {{transaction_id, _term, _data}, position} ->
        {transaction_id, position}
      end)

    Map.merge(index, Map.new(new_entries))
  end

  defp rebuild_log_up_to(t, transaction_id) do
    # For now, this is a simplified implementation
    # In a production system, we'd want to optimize this
    entries_to_keep =
      t.entries
      |> Enum.take_while(fn {id, _term, _data} -> id <= transaction_id end)

    # Create new log with only the entries we want to keep
    # This is expensive but correct for now
    try do
      # Close current log
      :disk_log.close(t.log_name)

      # Delete and recreate log file
      File.rm(List.to_string(t.log_file))

      # Reopen and write kept entries
      {:ok, _} =
        :disk_log.open([
          {:name, t.log_name},
          {:file, t.log_file},
          {:type, :halt},
          {:format, :internal},
          {:quiet, true}
        ])

      Enum.each(entries_to_keep, fn entry ->
        :disk_log.log(t.log_name, entry)
      end)

      # Rebuild index
      new_index =
        entries_to_keep
        |> Enum.with_index(1)
        |> Enum.map(fn {{id, _term, _data}, position} -> {id, position} end)
        |> Map.new()

      updated_log = %{t | entries: entries_to_keep, index: new_index}
      {:ok, updated_log}
    catch
      :error, reason -> {:error, reason}
    end
  end
end
