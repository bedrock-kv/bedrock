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
  - Well-known keys: `{:tail, transaction_id}`, `{:last_commit, transaction_id}`,
    `{:election_state, {election_term, voted_for}}` (with `{:current_term, election_term}`
    read as a legacy fallback for logs written before the vote was persisted)

  ## File Layout

  Coordinator follows standard Bedrock working directory pattern:

      /data/coordinator/      # Base path from config[:coordinator][:path]
      └── raft/               # Coordinator working directory
          └── raft_log.dets   # DETS file

  """

  alias Bedrock.Raft
  alias Bedrock.Raft.TransactionID

  # Type definitions based on Raft types for better specificity
  @type input_transaction :: {term :: Raft.election_term(), data :: term()}
  @type stored_transaction_record :: {Raft.transaction_id(), input_transaction()}
  @type chain_link_record :: {{:chain, Raft.transaction_id()}, Raft.transaction_id() | nil}
  @type metadata_record ::
          {:tail, Raft.transaction_id()}
          | {:last_commit, Raft.transaction_id()}
          | {:election_state, {Raft.election_term(), Raft.peer() | nil}}
          | {:current_term, Raft.election_term()}
  @type dets_record :: stored_transaction_record() | chain_link_record() | metadata_record()
  @type dets_error ::
          {:error, :file_not_found | :permission_denied | :badarg | :table_not_open | term()}
  @type open_result :: {:ok, t()} | dets_error()
  @type dets_operation_result :: {:ok, t()} | dets_error()

  @type t :: %__MODULE__{
          table_name: atom(),
          table_file: String.t(),
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

    table_file = Path.join(log_dir, "raft_log.dets")

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
  @spec open(t()) :: open_result()
  def open(%__MODULE__{} = log) do
    case :dets.open_file(log.table_name, [{:file, String.to_charlist(log.table_file)}]) do
      {:ok, table_name} ->
        {:ok, %{log | table_name: table_name, is_open: true}}

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
  @spec build_chain_links(Raft.transaction_id(), [stored_transaction_record()]) :: [
          chain_link_record()
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
          |> Enum.map(&build_chain_link/1)

        [first_link | chain_links]
    end
  end

  @spec build_chain_link({stored_transaction_record(), stored_transaction_record() | nil}) ::
          chain_link_record()
  defp build_chain_link({{id, _}, next}) do
    next_id =
      case next do
        {next_id, _} -> next_id
        nil -> nil
      end

    {{:chain, id}, next_id}
  end

  @doc """
  Helper function to walk chain inclusively from current to target, returning
  at most `limit` records (`:infinity` for no limit).
  """
  @spec walk_chain_inclusive(
          t(),
          Raft.transaction_id(),
          Raft.transaction_id(),
          non_neg_integer() | :infinity
        ) :: [stored_transaction_record()]
  def walk_chain_inclusive(log, current_id, to_id, limit \\ :infinity)

  def walk_chain_inclusive(_log, _current_id, _to_id, 0), do: []

  def walk_chain_inclusive(log, current_id, to_id, limit) when current_id <= to_id do
    case :dets.lookup(log.table_name, current_id) do
      [{^current_id, data}] ->
        [
          {current_id, data}
          | case :dets.lookup(log.table_name, {:chain, current_id}) do
              [{{:chain, ^current_id}, next_id}] when next_id != nil and next_id <= to_id ->
                walk_chain_inclusive(log, next_id, to_id, decrement_limit(limit))

              _ ->
                []
            end
        ]

      [] ->
        []
    end
  end

  def walk_chain_inclusive(_log, _current_id, _to_id, _limit), do: []

  defp decrement_limit(:infinity), do: :infinity
  defp decrement_limit(limit), do: limit - 1

  @doc """
  Sync the DETS table to disk to ensure durability.
  """
  @spec sync(t()) :: :ok | {:error, :table_not_open | term()}
  def sync(%__MODULE__{table_name: table_name}) do
    case :dets.sync(table_name) do
      :ok -> :ok
      {:error, reason} -> {:error, reason}
    end
  rescue
    ArgumentError -> {:error, :table_not_open}
  end

  # Raft.Log protocol implementation functions

  @initial_transaction_id TransactionID.new(0, 0)

  @doc """
  Create a new log with the given term and sequence number.
  """
  @spec new_id(t(), Raft.election_term(), Raft.index()) :: Raft.tuple_transaction_id()
  def new_id(_t, term, sequence), do: TransactionID.new(term, sequence)

  @doc """
  Get the initial transaction for the log.
  """
  @spec initial_transaction_id(t()) :: Raft.tuple_transaction_id()
  def initial_transaction_id(_t), do: @initial_transaction_id

  @doc """
  Append the given block of transactions to the log.
  """
  @spec append_transactions(t(), Raft.transaction_id(), [input_transaction()]) ::
          dets_operation_result() | {:error, :prev_transaction_not_found}
  def append_transactions(t, prev_id, transactions) do
    if has_transaction_id?(t, prev_id) do
      # Build all records for atomic insert
      transaction_records = transactions
      chain_links = build_chain_links(prev_id, transactions)

      new_tail_id =
        case List.last(transactions) do
          {id, _} -> id
          nil -> prev_id
        end

      records = transaction_records ++ chain_links ++ [{:tail, new_tail_id}]

      with :ok <- :dets.insert(t.table_name, records),
           :ok <- sync(t) do
        {:ok, t}
      end
    else
      {:error, :prev_transaction_not_found}
    end
  end

  @doc """
  Purge the log of all transactions after the given id.

  A purge that would remove committed transactions is rejected, since Raft's
  commit index must never decrease. Purged records are deleted outright rather
  than merely unlinked: the leader reads `has_transaction_id?/2` to reposition
  its send cursor on a follower's hint, so a stale physical record would
  confirm an entry the chain can no longer reach.
  """
  @spec purge_transactions_after(t(), Raft.transaction_id()) ::
          dets_operation_result() | {:error, :would_delete_committed_transactions}
  def purge_transactions_after(t, transaction_id) do
    cond do
      transaction_id < newest_safe_transaction_id(t) ->
        {:error, :would_delete_committed_transactions}

      transaction_id >= newest_transaction_id(t) ->
        {:ok, t}

      true ->
        Enum.each(transaction_ids_after(t, transaction_id), fn id ->
          :ok = :dets.delete(t.table_name, id)
          :ok = :dets.delete(t.table_name, {:chain, id})
        end)

        records = [
          # Mark as end
          {{:chain, transaction_id}, nil},
          {:tail, transaction_id}
        ]

        with :ok <- :dets.insert(t.table_name, records),
             :ok <- sync(t) do
          {:ok, t}
        end
    end
  end

  @spec transaction_ids_after(t(), Raft.transaction_id()) :: [Raft.transaction_id()]
  defp transaction_ids_after(t, transaction_id), do: select_transaction_ids(t, :>, transaction_id)

  # Transaction records are the only ones keyed by an integer pair, so the
  # guards select exactly the transaction ids, skipping chain links (keyed
  # `{:chain, id}`) and metadata (keyed by a bare atom).
  @spec select_transaction_ids(t(), :> | :<, Raft.transaction_id()) :: [Raft.transaction_id()]
  defp select_transaction_ids(t, comparison, transaction_id) do
    :dets.select(t.table_name, [
      {{{:"$1", :"$2"}, :_},
       [
         {:is_integer, :"$1"},
         {:is_integer, :"$2"},
         {comparison, {{:"$1", :"$2"}}, {:const, transaction_id}}
       ], [{{:"$1", :"$2"}}]}
    ])
  end

  @doc """
  Mark all transactions up to and including the given transaction as committed.
  """
  @spec commit_up_to(t(), Raft.transaction_id()) :: dets_operation_result() | :unchanged
  def commit_up_to(_t, @initial_transaction_id), do: :unchanged

  def commit_up_to(t, transaction_id) do
    current_commit = newest_safe_transaction_id(t)

    if transaction_id > current_commit do
      with :ok <- :dets.insert(t.table_name, {:last_commit, transaction_id}),
           :ok <- sync(t) do
        {:ok, t}
      end
    else
      :unchanged
    end
  end

  @doc """
  Get the newest transaction in the log.
  """
  @spec newest_transaction_id(t()) :: Raft.transaction_id()
  def newest_transaction_id(t) do
    case :dets.lookup(t.table_name, :tail) do
      [{:tail, transaction_id}] -> transaction_id
      # Empty log
      [] -> @initial_transaction_id
    end
  end

  @doc """
  Get the newest safe transaction in the log.
  """
  @spec newest_safe_transaction_id(t()) :: Raft.transaction_id()
  def newest_safe_transaction_id(t) do
    case :dets.lookup(t.table_name, :last_commit) do
      [{:last_commit, transaction_id}] -> transaction_id
      # Nothing committed yet
      [] -> @initial_transaction_id
    end
  end

  @doc """
  Does the log contain the given transaction?
  """
  @spec has_transaction_id?(t(), Raft.transaction_id()) :: boolean()
  def has_transaction_id?(_t, @initial_transaction_id), do: true

  def has_transaction_id?(t, transaction_id) do
    case :dets.lookup(t.table_name, transaction_id) do
      [_] -> true
      [] -> false
    end
  end

  @doc """
  Get a list of transactions that have occurred up to the given transaction.
  """
  @spec transactions_to(t(), Raft.transaction_id() | :newest | :newest_safe) :: [
          stored_transaction_record()
        ]
  def transactions_to(t, :newest), do: transactions_from(t, @initial_transaction_id, newest_transaction_id(t))

  def transactions_to(t, :newest_safe), do: transactions_from(t, @initial_transaction_id, newest_safe_transaction_id(t))

  def transactions_to(t, to), do: transactions_from(t, @initial_transaction_id, to)

  @doc """
  Get a list of transactions from the given starting point.
  """
  @spec transactions_from(
          t(),
          Raft.transaction_id(),
          Raft.transaction_id() | :newest | :newest_safe
        ) :: [stored_transaction_record()]
  def transactions_from(t, from, to), do: transactions_from(t, from, to, :infinity)

  @doc """
  Same as `transactions_from/3`, but returns at most `limit` transactions
  (`:infinity` for no limit). The replication hot path fetches one bounded
  batch per AppendEntries request through this function.
  """
  @spec transactions_from(
          t(),
          Raft.transaction_id(),
          Raft.transaction_id() | :newest | :newest_safe,
          non_neg_integer() | :infinity
        ) :: [stored_transaction_record()]
  def transactions_from(t, from, :newest, limit), do: transactions_from(t, from, newest_transaction_id(t), limit)

  def transactions_from(t, from, :newest_safe, limit),
    do: transactions_from(t, from, newest_safe_transaction_id(t), limit)

  def transactions_from(t, @initial_transaction_id, to, limit) do
    # Special case: from initial_transaction_id includes all up to 'to'
    case :dets.lookup(t.table_name, {:chain, @initial_transaction_id}) do
      [{{:chain, @initial_transaction_id}, first_real_txn}] when first_real_txn != nil ->
        walk_chain_inclusive(t, first_real_txn, to, limit)

      # Empty chain
      _ ->
        []
    end
  end

  def transactions_from(t, from, to, limit) when from != @initial_transaction_id do
    # Normal case: exclude 'from', include up to 'to'
    case :dets.lookup(t.table_name, from) do
      # from not found
      [] ->
        []

      [_] ->
        # Follow chain starting from NEXT after from
        case :dets.lookup(t.table_name, {:chain, from}) do
          [{{:chain, ^from}, next_id}] when next_id != nil and next_id <= to ->
            walk_chain_inclusive(t, next_id, to, limit)

          _ ->
            []
        end
    end
  end

  @doc """
  Get the id of the newest transaction in the log that is older than the given
  transaction id, or the initial id when no such transaction exists.

  The protocol asks for O(log n) here; DETS tables are unordered, so this is a
  single-pass select over the table instead. The coordinator's raft log stays
  small and the leader only consults this on rejected AppendEntries responses,
  so the full pass is not a hot path.
  """
  @spec previous_transaction_id(t(), Raft.transaction_id()) :: Raft.transaction_id()
  def previous_transaction_id(t, transaction_id) do
    case select_transaction_ids(t, :<, transaction_id) do
      [] -> @initial_transaction_id
      transaction_ids -> Enum.max(transaction_ids)
    end
  end

  @doc """
  Get the current election term from persistent storage.
  Returns 0 if no term has been persisted yet (initial state).
  """
  @spec current_term(t()) :: Raft.election_term()
  def current_term(t) do
    {term, _voted_for} = election_state(t)
    term
  end

  @doc """
  Get the candidate that received this server's vote in the current term, or
  `nil` if the server has not voted.
  """
  @spec voted_for(t()) :: Raft.peer() | nil
  def voted_for(t) do
    {_term, voted_for} = election_state(t)
    voted_for
  end

  @spec election_state(t()) :: {Raft.election_term(), Raft.peer() | nil}
  defp election_state(t) do
    case :dets.lookup(t.table_name, :election_state) do
      [{:election_state, {term, voted_for}}] ->
        {term, voted_for}

      [] ->
        # Logs written before the vote was persisted carry only the term.
        case :dets.lookup(t.table_name, :current_term) do
          [{:current_term, term}] -> {term, nil}
          [] -> {0, nil}
        end
    end
  end

  @doc """
  Save the current election term to persistent storage.
  This must be called before responding to RPCs to ensure Raft safety.
  Advancing the term also clears any vote from an earlier term; equal or
  lower terms leave the persisted state untouched.
  """
  @spec save_current_term(t(), Raft.election_term()) :: dets_operation_result()
  def save_current_term(t, term) do
    if term > current_term(t) do
      save_election_state(t, term, nil)
    else
      {:ok, t}
    end
  end

  @doc """
  Atomically save the current term and the candidate voted for in that term.

  Advancing to a new term may set any vote (usually `nil`). Within the durable
  term, a vote may be set when none exists or repeated verbatim, but never
  changed or cleared (`{:error, :already_voted}`). Writes below the durable
  term return `{:error, :stale_term}`. Both values live in a single DETS
  record, so they are persisted together.
  """
  @spec save_election_state(t(), Raft.election_term(), Raft.peer() | nil) ::
          dets_operation_result() | {:error, :already_voted | :stale_term}
  def save_election_state(t, term, voted_for) do
    case election_state(t) do
      {current, _} when term > current -> persist_election_state(t, term, voted_for)
      {current, nil} when term == current -> persist_election_state(t, term, voted_for)
      {current, ^voted_for} when term == current -> {:ok, t}
      {current, _} when term == current -> {:error, :already_voted}
      _stale -> {:error, :stale_term}
    end
  end

  @spec persist_election_state(t(), Raft.election_term(), Raft.peer() | nil) ::
          dets_operation_result()
  defp persist_election_state(t, term, voted_for) do
    with :ok <- :dets.insert(t.table_name, {:election_state, {term, voted_for}}),
         :ok <- sync(t) do
      {:ok, t}
    end
  end
end

# Implement the Bedrock.Raft.Log protocol using delegation
defimpl Bedrock.Raft.Log, for: Bedrock.ControlPlane.Coordinator.DiskRaftLog do
  alias Bedrock.ControlPlane.Coordinator.DiskRaftLog

  defdelegate new_id(t, term, sequence), to: DiskRaftLog
  defdelegate initial_transaction_id(t), to: DiskRaftLog
  defdelegate append_transactions(t, prev_id, transactions), to: DiskRaftLog
  defdelegate purge_transactions_after(t, transaction_id), to: DiskRaftLog
  defdelegate commit_up_to(t, transaction_id), to: DiskRaftLog
  defdelegate newest_transaction_id(t), to: DiskRaftLog
  defdelegate newest_safe_transaction_id(t), to: DiskRaftLog
  defdelegate has_transaction_id?(t, transaction_id), to: DiskRaftLog
  defdelegate transactions_to(t, to), to: DiskRaftLog
  defdelegate transactions_from(t, from, to), to: DiskRaftLog
  defdelegate transactions_from(t, from, to, limit), to: DiskRaftLog
  defdelegate previous_transaction_id(t, transaction_id), to: DiskRaftLog
  defdelegate current_term(t), to: DiskRaftLog
  defdelegate save_current_term(t, term), to: DiskRaftLog
  defdelegate voted_for(t), to: DiskRaftLog
  defdelegate save_election_state(t, term, voted_for), to: DiskRaftLog
end
