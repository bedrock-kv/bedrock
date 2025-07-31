defmodule Bedrock.ControlPlane.Coordinator.DiskRaftLog do
  @moduledoc """
  A DETS-based implementation of the Raft log using transaction chaining.

  This module provides persistent storage for Raft consensus operations,
  ensuring that log entries survive process and node restarts.

  ## Design

  - Uses DETS for key-value storage with transaction chaining
  - Chain links use forward pointers for O(1) truncation
  - No in-memory state - DETS provides all storage
  - Atomic batch operations for consistency

  ## DETS Schema

  - Transaction records: `{transaction_id, data}`
  - Chain links: `{{:chain, transaction_id}, next_transaction_id | nil}`
  - Well-known keys: `{:tail, transaction_id}`, `{:last_commit, transaction_id}`

  ## File Layout

  Coordinator follows standard Bedrock working directory pattern:

      /data/coordinator/      # Base path from config[:coordinator][:path]
      └── raft/               # Coordinator working directory
          └── raft_log.dets   # DETS file

  """

  alias Bedrock.Raft

  @type t :: %__MODULE__{
          table_name: atom(),
          table_file: charlist(),
          is_open: boolean()
        }

  defstruct [
    :table_name,
    :table_file,
    is_open: false
  ]

  @doc """
  Create a new DETS-based raft log.

  ## Options

    * `:log_dir` - Directory to store DETS file (required)
    * `:table_name` - Name for the DETS table (default: :raft_log)

  ## Examples

      iex> log = DiskRaftLog.new(log_dir: "/tmp/raft")
      iex> is_struct(log, DiskRaftLog)
      true

  """
  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    log_dir = Keyword.fetch!(opts, :log_dir)
    table_name = Keyword.get(opts, :table_name, :raft_log)

    # Ensure log directory exists
    File.mkdir_p!(log_dir)

    table_file = Path.join(log_dir, "raft_log.dets") |> String.to_charlist()

    %__MODULE__{
      table_name: table_name,
      table_file: table_file,
      is_open: false
    }
  end

  @doc """
  Open the DETS table for reading and writing.

  This must be called before any other operations.
  """
  @spec open(t()) :: {:ok, t()} | {:error, term()}
  def open(%__MODULE__{} = log) do
    case :dets.open_file(log.table_name, [{:file, log.table_file}]) do
      {:ok, _table_name} ->
        {:ok, %{log | is_open: true}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Close the DETS table.
  """
  @spec close(t()) :: :ok
  def close(%__MODULE__{table_name: table_name}) do
    :dets.close(table_name)
  end

  @doc """
  Helper function to build chain link records.
  """
  @spec build_chain_links(Raft.transaction_id(), [Raft.transaction()]) :: [
          {{:chain, Raft.transaction_id()}, Raft.transaction_id() | nil}
        ]
  def build_chain_links(prev_id, transactions) do
    case transactions do
      [] ->
        []

      [{first_id, _} | rest] ->
        # Link prev_id to first new transaction
        first_link = {{:chain, prev_id}, first_id}

        # Chain the new transactions together
        chain_links =
          transactions
          |> Enum.zip(rest ++ [nil])
          |> Enum.map(fn {{id, _}, next} ->
            next_id =
              case next do
                {next_id, _} -> next_id
                nil -> nil
              end

            {{:chain, id}, next_id}
          end)

        [first_link | chain_links]
    end
  end

  @doc """
  Helper function to walk chain inclusively from current to target.
  """
  @spec walk_chain_inclusive(t(), Raft.transaction_id(), Raft.transaction_id()) :: [
          {Raft.transaction_id(), term()}
        ]
  def walk_chain_inclusive(log, current_id, to_id) when current_id <= to_id do
    case :dets.lookup(log.table_name, current_id) do
      [{^current_id, data}] ->
        [
          {current_id, data}
          | case :dets.lookup(log.table_name, {:chain, current_id}) do
              [{{:chain, ^current_id}, next_id}] when next_id != nil and next_id <= to_id ->
                walk_chain_inclusive(log, next_id, to_id)

              _ ->
                []
            end
        ]

      [] ->
        []
    end
  end

  def walk_chain_inclusive(_log, _current_id, _to_id), do: []

  @doc """
  Sync the DETS table to disk to ensure durability.
  """
  @spec sync(t()) :: :ok | {:error, term()}
  def sync(%__MODULE__{table_name: table_name}) do
    try do
      case :dets.sync(table_name) do
        :ok -> :ok
        {:error, reason} -> {:error, reason}
      end
    rescue
      ArgumentError -> {:error, :table_not_open}
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
  def append_transactions(t, prev_id, transactions) do
    if has_transaction_id?(t, prev_id) do
      # Build all records for atomic insert
      transaction_records = transactions
      chain_links = DiskRaftLog.build_chain_links(prev_id, transactions)

      new_tail_id =
        case List.last(transactions) do
          {id, _} -> id
          nil -> prev_id
        end

      records = transaction_records ++ chain_links ++ [{:tail, new_tail_id}]

      case :dets.insert(t.table_name, records) do
        :ok -> {:ok, t}
        error -> error
      end
    else
      {:error, :prev_transaction_not_found}
    end
  end

  @impl true
  def purge_transactions_after(t, transaction_id) do
    # Get current commit to ensure it doesn't go beyond purge point
    current_commit = newest_safe_transaction_id(t)
    new_commit = min(current_commit, transaction_id)

    records = [
      # Mark as end
      {{:chain, transaction_id}, nil},
      {:tail, transaction_id},
      {:last_commit, new_commit}
    ]

    case :dets.insert(t.table_name, records) do
      :ok -> {:ok, t}
      error -> error
    end
  end

  @impl true
  def commit_up_to(_t, @initial_transaction_id), do: :unchanged

  @impl true
  def commit_up_to(t, transaction_id) do
    current_commit = newest_safe_transaction_id(t)

    if transaction_id > current_commit do
      case :dets.insert(t.table_name, {:last_commit, transaction_id}) do
        :ok -> {:ok, t}
        error -> error
      end
    else
      :unchanged
    end
  end

  @impl true
  def newest_transaction_id(t) do
    case :dets.lookup(t.table_name, :tail) do
      [{:tail, transaction_id}] -> transaction_id
      # Empty log
      [] -> @initial_transaction_id
    end
  end

  @impl true
  def newest_safe_transaction_id(t) do
    case :dets.lookup(t.table_name, :last_commit) do
      [{:last_commit, transaction_id}] -> transaction_id
      # Nothing committed yet
      [] -> @initial_transaction_id
    end
  end

  @impl true
  def has_transaction_id?(_t, @initial_transaction_id), do: true
  @impl true
  def has_transaction_id?(t, transaction_id) do
    case :dets.lookup(t.table_name, transaction_id) do
      [_] -> true
      [] -> false
    end
  end

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
    # Special case: from initial_transaction_id includes all up to 'to'
    case :dets.lookup(t.table_name, {:chain, @initial_transaction_id}) do
      [{{:chain, @initial_transaction_id}, first_real_txn}] when first_real_txn != nil ->
        DiskRaftLog.walk_chain_inclusive(t, first_real_txn, to)

      # Empty chain
      _ ->
        []
    end
  end

  @impl true
  def transactions_from(t, from, to) when from != @initial_transaction_id do
    # Normal case: exclude 'from', include up to 'to'
    case :dets.lookup(t.table_name, from) do
      # from not found
      [] ->
        []

      [_] ->
        # Follow chain starting from NEXT after from
        case :dets.lookup(t.table_name, {:chain, from}) do
          [{{:chain, ^from}, next_id}] when next_id != nil and next_id <= to ->
            DiskRaftLog.walk_chain_inclusive(t, next_id, to)

          _ ->
            []
        end
    end
  end
end
