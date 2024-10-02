defmodule Bedrock.Raft.Mode.Candidate do
  @moduledoc """
  The Candidate state of the RAFT consensus protocol is a critical stage that
  occurs when a node in the Raft cluster transitions from a Follower to a
  Candidate. This phase is integral to the leader election process in Raft.

  Here are the key aspects of the Candidate phase:

  - Initiation of Election: The Candidate phase begins when a follower doesn't
    receive a heartbeat from a leader within a set timeout period. This
    indicates that the current leader is either down or unreachable, prompting
    the follower to become a candidate.

  - Increment Term and Vote for Self: Upon entering the Candidate phase, the
    node increments its current term number (a logical clock) and votes for
    itself. This is the start of attempting to become the new leader.

  - Requesting Votes: The candidate then sends out RequestVote RPCs (Remote
    Procedure Calls) to all other nodes in the cluster. In these requests, the
    candidate includes its term number and the last log entry it has, which
    helps other nodes decide whether to grant their vote to the candidate.

  - Majority Votes: To become a leader, the candidate needs to receive votes
    from a majority (quorum) of the nodes in the cluster. This majority is
    essential for ensuring only one leader is elected per term and to maintain
    consistency.

  - Handling Parallel Elections: If multiple followers become candidates
    simultaneously, parallel elections can occur. This might lead to a split
    vote, where no candidate receives the majority. If this happens, a new
    election term begins, and nodes will retry the election process.

  - Timeout and Retry: If a candidate does not get a majority of votes within a
    certain timeout period, it will restart the election process. This involves
    incrementing its term and sending out a new round of RequestVote RPCs.

  - Transition to Leader: If the candidate receives the majority of votes, it
    transitions to the Leader phase.

  - Response to External Leader Claim: If, during the candidate phase, a node
    receives a legitimate AppendEntries RPC from another node claiming to be
    the leader, and the claimant's term is at least as large as the candidate’s
    current term, the candidate reverts to a follower. This ensures that there
    is only one leader at a time in the cluster.
  """

  @type t :: %__MODULE__{}
  defstruct ~w[
    term
    quorum
    nodes
    votes
    log
    interface
    cancel_timer_fn
  ]a

  alias Bedrock.Raft
  alias Bedrock.Raft.Log

  @spec new(
          Raft.election_term(),
          Raft.quorum(),
          [Raft.service()],
          log :: Log.t(),
          interface :: module()
        ) ::
          t()
  def new(term, quorum, nodes, log, interface) do
    %__MODULE__{
      term: term,
      quorum: quorum,
      nodes: nodes,
      votes: [],
      log: log,
      interface: interface
    }
    |> request_votes_from_all_nodes()
    |> set_timer()
  end

  @doc """
  A vote has been received. If the voting term is equal to our term, then
  we'll record the vote, if we haven't received one from this follower yet.
  If the voting term is different from our term, then we'll ignore the vote.
  If we've received enough votes to win the election for this term, then
  we'll return an indication that we've been elected leader.
  """
  @spec vote_received(t(), Raft.election_term(), follower :: Raft.service()) ::
          :was_elected_leader | {:ok, t()}
  def vote_received(t, voting_term, follower) when voting_term == t.term do
    if follower in t.votes do
      {:ok, t}
    else
      votes = [follower | t.votes]

      if length(votes) >= t.quorum do
        t |> cancel_timer()
        :was_elected_leader
      else
        {:ok, %{t | votes: votes}}
      end
    end
  end

  def vote_received(t, _voting_term, _follower), do: {:ok, t}

  @doc """
  A ping has been received. If the term is greater than or equal to our term,
  then we will cancel any outstanding timers and signal that a new leader has
  been elected. Otherwise, we'll ignore the ping.
  """
  @spec append_entries_received(
          t(),
          leader_term :: Raft.election_term(),
          prev_transaction :: Raft.transaction(),
          transactions :: [Raft.transaction()],
          commit_transaction :: Raft.transaction(),
          from :: Raft.service()
        ) ::
          :new_leader_elected | {:ok, t()}
  def append_entries_received(
        t,
        leader_term,
        _prev_transaction,
        _transactions,
        _commit_transaction,
        _leader
      ) do
    if leader_term >= t.term do
      t |> cancel_timer()
      :new_leader_elected
    else
      {:ok, t}
    end
  end

  @spec set_timer(t()) :: t()
  defp set_timer(t),
    do: %{t | cancel_timer_fn: apply(t.interface, :timer, [:election, 150, 300])}

  @spec cancel_timer(t()) :: t()
  def cancel_timer(t) when is_nil(t.cancel_timer_fn), do: t

  def cancel_timer(t) do
    t.cancel_timer_fn.()
    %{t | cancel_timer_fn: nil}
  end

  @spec request_votes_from_all_nodes(t()) :: t()
  defp request_votes_from_all_nodes(t) do
    newest_transaction_id = Log.newest_transaction_id(t.log)

    t.nodes
    |> Enum.each(
      &apply(t.interface, :send_event, [&1, {:request_vote, t.term, newest_transaction_id}])
    )

    t
  end
end
