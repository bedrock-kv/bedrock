defmodule Bedrock.Raft.Mode.Follower do
  @moduledoc """
  The Follower state of the RAFT protocol. This is the state that a node is in
  when it is not the leader, and is waiting for a leader to emerge, but has not
  yet decided to stand as a candidate for election itself.

  These are the key characteristics of this phase:

  - Passivity: Followers are mostly passive. They don't issue requests on
    their own but respond to requests from leaders and candidates.

  - Responding to Leaders: When a follower receives a heartbeat (a signal to
    prevent election timeouts) or data from the current leader, it updates its
    internal state and then responds to the leader’s requests.

  - Election Timeout: Each follower has an election timeout. If a follower
    doesn't hear from a leader before this timeout, it assumes there is no
    current leader and indicates that a transition to the Candidate state is
    necessary to start an election.

  - Log Replication: Followers receive log entries from the leader. They append
    these entries to their log and return acknowledgment to the leader.

  - Committing Entries: Once a log entry is safely replicated, the leader
    informs the followers to commit the entry. The followers then apply the
    entry to their local state machines.

  - Consistency Check: If there's a discrepancy in the log entries between a
    leader and a follower, the follower rejects the new entries. The leader
    then works to bring the follower's log into consistency with its own.
  """

  @type t :: %__MODULE__{}
  defstruct ~w[
      term
      leader
      voted_for
      cancel_timer_fn
      log
      interface
    ]a

  alias Bedrock.Raft
  alias Bedrock.Raft.Log

  @spec new(Raft.election_term(), Log.t(), interface :: Module.t()) :: t()
  def new(term, log, interface) do
    %__MODULE__{
      term: term,
      voted_for: nil,
      leader: :undecided,
      log: log,
      interface: interface
    }
    |> set_timer()
  end

  @doc """
  Our vote has been requested by a candidate. We'll vote for the candidate if
  we haven't yet voted in this election, or:
    - the candidate's election term is greater or equal to our term, and
    - the candidate's last transaction is at least as up-to-date as ours.

  Otherwise, we'll ignore the request.
  """
  @spec vote_requested(
          t(),
          Raft.election_term(),
          candidate :: Raft.service(),
          candidate_last_transaction :: Raft.transaction()
        ) :: {:ok, t()}
  def vote_requested(t, election_term, candidate, candidate_newest_transaction) do
    if (election_term == t.term and is_nil(t.voted_for)) or
         (election_term > t.term and
            candidate_newest_transaction >= Log.newest_transaction(t.log)) do
      {:ok,
       t
       |> reset_timer()
       |> vote_for(candidate, election_term)}
    else
      {:ok, t}
    end
  end

  @doc """
  A ping has been received. If the term is greater than our term, then we will
  cancel any outstanding timers and record that a new leader has been elected.
  Otherwise, we'll ignore the ping.
  """
  @spec append_entries_received(
          t(),
          leader_term :: Raft.election_term(),
          prev_transaction :: Raft.transaction(),
          transactions :: [Raft.transaction()],
          commit_transaction :: Raft.transaction(),
          from :: Raft.service()
        ) ::
          {:ok, t()} | {:ok, t(), :new_leader_elected}
  def append_entries_received(
        t,
        leader_term,
        prev_transaction,
        transactions,
        commit_transaction,
        leader
      ) do
    if leader_term < t.term do
      {:ok, t}
    else
      leadership_changed = leader_term > t.term || (t.leader == :undecided and leader != t.leader)

      t =
        t
        |> reset_timer()
        |> record_leader(leader)
        |> record_term(leader_term)
        |> try_to_append_transactions(prev_transaction, transactions)
        |> commit_up_to(commit_transaction)
        |> send_append_entries_reply()

      if leadership_changed do
        {:ok, t, :new_leader_elected}
      else
        {:ok, t}
      end
    end
  end

  def try_to_append_transactions(t, _prev_transaction, []), do: t

  def try_to_append_transactions(t, prev_transaction, transactions) do
    Log.append_transactions(t.log, prev_transaction, transactions)
    |> case do
      {:ok, log} ->
        %{t | log: log}

      {:error, :prev_transaction_not_found} ->
        raise "prev_transaction_not_found"
    end
  end

  def commit_up_to(t, transaction) do
    Log.commit_up_to(t.log, transaction)
    |> case do
      {:ok, log} ->
        %{t | log: log}
    end
  end

  @spec send_append_entries_reply(t()) :: t()
  defp send_append_entries_reply(t) do
    newest_transaction = Log.newest_transaction(t.log)

    :ok =
      apply(t.interface, :send_event, [
        t.leader,
        {:append_entries_ack, t.term, newest_transaction}
      ])

    t
  end

  @spec vote_for(t(), Raft.service(), Raft.election_term()) :: t()
  defp vote_for(t, candidate, election_term) do
    apply(t.interface, :send_event, [candidate, {:vote, election_term}])
    %{t | voted_for: candidate, term: election_term}
  end

  @spec reset_timer(t()) :: t()
  defp reset_timer(t),
    do: t |> cancel_timer() |> set_timer()

  @spec cancel_timer(t()) :: t()
  def cancel_timer(t) when is_nil(t.cancel_timer_fn), do: t

  def cancel_timer(t) do
    t.cancel_timer_fn.()
    %{t | cancel_timer_fn: nil}
  end

  @spec set_timer(t()) :: t()
  defp set_timer(t),
    do: %{t | cancel_timer_fn: apply(t.interface, :timer, [:election, 150, 300])}

  @spec record_leader(t(), Raft.service()) :: t()
  defp record_leader(t, leader), do: %{t | leader: leader}

  @spec record_term(t(), Raft.election_term()) :: t()
  defp record_term(t, term), do: %{t | term: term}
end
