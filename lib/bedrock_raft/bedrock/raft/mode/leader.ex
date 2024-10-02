defmodule Bedrock.Raft.Mode.Leader do
  @moduledoc """
  The Leader phase in the Raft Consensus Protocol is a crucial stage where a
  node assumes the role of a leader in the Raft cluster. Once a node becomes a
  leader, it is responsible for managing the replicated log and ensuring
  consistency across the cluster.

  Here are the key aspects of the Leader phase:

  - Assumption of Leadership: After a node wins an election during the
    Candidate phase by receiving the majority of votes, it transitions to the
    Leader phase. It assumes authority and starts to manage the cluster's log
    replication.

  - Sending Heartbeats: One of the first actions of the leader is to send out
    heartbeat messages (using AppendEntries RPCs with no log entries) to all
    follower nodes. These heartbeats serve two purposes: they inform followers
    that there is a stable leader, and they prevent new elections from being
    triggered.

  - Log Replication: The leader receives client requests, each containing a
    command to be executed by the replicated state machines. The leader appends
    these commands as new entries in its log and then replicates these entries
    to the follower nodes.

  - Handling Log Consistency: The leader must ensure that the followers' logs
    are consistent with its own. It does this by checking the consistency of
    the logs with each follower. If there's a mismatch, the leader will resend
    missing or conflicting entries to bring the followers' logs into alignment
    with its own.

  - Committing Entries: After successfully replicating a log entry to the
    majority of the followers, the leader marks the entry as committed. This
    means the entry is safely stored and can be applied to the state machines
    of all nodes. The leader also communicates to followers which entries are
    safe to commit.

  - Client Response: Once an entry is committed, the leader returns the result
    of the command execution to the client.

  - Handling Failures: If a follower fails to replicate a log entry (due to a
    crash or network issue), the leader retries log replication until it
    succeeds, ensuring that the follower eventually stores all committed
    entries.

  - Recovering from Disconnects: If the leader gets disconnected from the
    cluster (due to network issues or crashes), a new leader will be elected.
    Upon rejoining, the old leader will revert to a follower and update its log
    according to the new leader's log.

  - Processing Read Requests: The leader can directly handle read-only client
    requests, but it typically first ensures it is still the leader by
    confirming it can communicate with the majority of the cluster.

  - Term Change: If the leader receives a message from another node with a
    higher term, it steps down and reverts to a follower, recognizing the
    authority of the higher term.
  """

  @type t :: %__MODULE__{}
  defstruct ~w[
    nodes
    quorum
    term
    pongs
    newest_transaction_id
    last_consensus_transaction_id
    follower_tracking
    cancel_timer_fn
    log
    interface
  ]a

  alias Bedrock.Raft
  alias Bedrock.Raft.Log
  alias Bedrock.Raft.TransactionID
  alias Bedrock.Raft.Mode.Leader.FollowerTracking

  @doc """
  Create a new leader. We'll send notices to all the nodes, and schedule the
  timer to tick.
  """
  @spec new(
          Raft.election_term(),
          Raft.quorum(),
          [Raft.service()],
          Log.t(),
          interface :: module()
        ) ::
          t()
  def new(term, quorum, nodes, log, interface) do
    %__MODULE__{
      quorum: quorum,
      nodes: nodes,
      term: term,
      pongs: [],
      newest_transaction_id: Log.newest_transaction_id(log),
      last_consensus_transaction_id: Log.newest_safe_transaction_id(log),
      follower_tracking: FollowerTracking.new(nodes),
      log: log,
      interface: interface
    }
    |> reset_pongs()
    |> send_append_entries_to_followers()
    |> set_timer()
  end

  @doc """
  Add a transaction to the log. If the transaction is out of order (the
  transaction is not from this term, or is smaller than the newest transaction),
  we'll return an error.
  """
  @spec add_transaction(t(), Raft.transaction()) ::
          {:ok, t()} | {:error, :out_of_order | :incorrect_term}

  def add_transaction(t, {transaction_id, _data} = transaction) do
    cond do
      transaction_id < t.newest_transaction_id ->
        {:error, :out_of_order}

      TransactionID.term(transaction_id) != t.term ->
        {:error, :incorrect_term}

      true ->
        Log.append_transactions(t.log, t.newest_transaction_id, [transaction])
        {:ok, %{t | newest_transaction_id: transaction_id}}
    end
  end

  @doc """
  A follower has responded to our ping. If we haven't recorded them yet for this
  round, do so now.
  """
  @spec append_entries_ack_received(
          t(),
          Raft.election_term(),
          newest_transaction_id :: Raft.transaction_id(),
          follower :: Raft.service()
        ) ::
          {:ok, t()}
  def append_entries_ack_received(t, term, newest_transaction_id, _from = follower)
      when term == t.term do
    t =
      if follower in t.pongs do
        t
      else
        %{t | pongs: [follower | t.pongs]}
      end

    if Log.has_transaction_id?(t.log, newest_transaction_id) do
      FollowerTracking.update_newest_transaction_id(
        t.follower_tracking,
        follower,
        newest_transaction_id
      )
    end

    {:ok, t}
  end

  @doc """
  A ping has been received. If the term is greater than our term, then we will
  cancel any outstanding timers and signal that a new leader has been elected.
  Otherwise, we'll ignore the ping.
  """
  @spec append_entries_received(
          t(),
          leader_term :: Raft.election_term(),
          prev_transaction_id :: Raft.transaction_id(),
          transactions :: [Raft.transaction()],
          commit_transaction :: Raft.transaction_id(),
          from :: Raft.service()
        ) ::
          :new_leader_elected | {:ok, t()}
  def append_entries_received(
        t,
        leader_term,
        _prev_transaction,
        _transactions,
        _commit_transaction,
        _new_leader
      ) do
    if leader_term > t.term do
      t |> cancel_timer()
      :new_leader_elected
    else
      {:ok, t}
    end
  end

  @doc """
  The timer has ticked. We'll send notices to all the nodes, and start the
  timer again. If we haven't received enough pongs, we'll signal that we've
  failed to reach quorum.
  """
  @spec timer_ticked(t()) :: {:ok, t()} | :quorum_failed
  def timer_ticked(t) when length(t.pongs) < t.quorum,
    do: :quorum_failed

  def timer_ticked(t),
    do:
      {:ok,
       t
       |> reset_pongs()
       |> send_append_entries_to_followers()
       |> cancel_timer()
       |> set_timer()}

  @spec send_append_entries_to_followers(t()) :: t()
  def send_append_entries_to_followers(t) do
    newest_safe_transaction_id = newest_safe_transaction_id(t)

    {:ok, log} =
      Log.commit_up_to(t.log, newest_safe_transaction_id)

    t.nodes
    |> Enum.each(fn follower ->
      prev_transaction_id =
        FollowerTracking.last_sent_transaction_id(t.follower_tracking, follower)
        |> case do
          :unknown -> newest_safe_transaction_id
          transaction_id -> transaction_id
        end

      transactions =
        Log.transactions_from(t.log, prev_transaction_id, :newest)
        |> Enum.take(10)

      if transactions != [] do
        FollowerTracking.update_last_sent_transaction_id(
          t.follower_tracking,
          follower,
          transactions |> List.last() |> elem(0)
        )
      end

      apply(t.interface, :send_event, [
        follower,
        {:append_entries, t.term, prev_transaction_id, transactions, newest_safe_transaction_id}
      ])
    end)

    %{t | log: log}
    |> consensus_reached(newest_safe_transaction_id)
  end

  defp newest_safe_transaction_id(t) do
    t.follower_tracking
    |> FollowerTracking.newest_safe_transaction_id(t.quorum)
    |> case do
      :unknown -> t.newest_transaction_id
      transaction_id -> transaction_id
    end
  end

  @spec cancel_timer(t()) :: t()
  def cancel_timer(t) when is_nil(t.cancel_timer_fn),
    do: t

  def cancel_timer(t) do
    t.cancel_timer_fn.()
    %{t | cancel_timer_fn: nil}
  end

  @spec set_timer(t()) :: t()
  def set_timer(t),
    do: %{t | cancel_timer_fn: apply(t.interface, :timer, [:heartbeat, 50, 50])}

  @spec reset_pongs(t()) :: t()
  def reset_pongs(t),
    do: %{t | pongs: []}

  defp consensus_reached(t, transaction_id)
       when t.last_consensus_transaction_id == transaction_id,
       do: t

  defp consensus_reached(t, transaction_id) do
    :ok = apply(t.interface, :consensus_reached, [t.log, transaction_id])

    %{t | last_consensus_transaction_id: transaction_id}
  end
end
