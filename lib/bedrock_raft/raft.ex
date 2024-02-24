defmodule Bedrock.Raft do
  @moduledoc """
  The RAFT consensus protocol. This module implements the state machine for the
  protocol. Externalities like timers and message transport are left as details
  to the user of this module, so as not to tie it to any particular set of
  mechanisms and allow it to be used in a variety of contexts.

  This module is intended to be used as a component of a larger application.

  References:
    - https://raft.github.io/
    - https://arorashu.github.io/posts/raft.html
    - https://www.alibabacloud.com/blog/raft-engineering-practices-and-the-cluster-membership-change_597742
    - http://thesecretlivesofdata.com/raft/
  """

  @type election_term :: non_neg_integer()
  @type quorum :: non_neg_integer()
  @type service :: atom() | pid() | {atom(), node()}
  @type leadership :: {leader :: :undecided | service(), election_term()}
  @type binary_transaction_id :: binary()
  @type tuple_transaction_id :: {term :: election_term(), index :: non_neg_integer()}
  @type transaction_id :: binary_transaction_id() | tuple_transaction_id()
  @type transaction :: {transaction_id(), term()}

  alias Bedrock.Raft
  alias Bedrock.Raft.Log
  alias Bedrock.Raft.TransactionID
  alias Bedrock.Raft.Mode.Candidate
  alias Bedrock.Raft.Mode.Follower
  alias Bedrock.Raft.Mode.Leader

  @type t :: %__MODULE__{
          me: service(),
          mode: %Follower{} | %Candidate{} | %Leader{},
          nodes: [service()],
          quorum: non_neg_integer(),
          interface: Module.t()
        }
  defstruct ~w[
      me
      mode
      nodes
      quorum
      interface
    ]a

  @doc """
  Create a new RAFT consensus protocol instance.
  """
  @spec new(
          me :: service(),
          nodes :: [service()],
          log :: Log.t(),
          interface :: Module.t()
        ) :: t()
  def new(me, nodes, log, interface) do
    # Assume that we'll always vote for ourselves, so majority - 1.
    quorum = determine_majority([me | nodes]) - 1

    # Start with the newest term in the log.
    initial_term = Log.newest_safe_transaction_id(log) |> TransactionID.term()

    # If we're the only node, we can be the leader.
    mode =
      if quorum == 0 do
        Leader.new(initial_term, quorum, nodes, log, interface)
      else
        Follower.new(initial_term, log, interface)
      end

    %__MODULE__{
      me: me,
      nodes: nodes,
      quorum: quorum,
      mode: mode,
      interface: interface
    }
    |> notify_change_in_leadership()
  end

  @doc """
  Return the log for this protocol instance.
  """
  @spec log(t()) :: Log.t()
  def log(t), do: t.mode.log

  @doc """
  Return the node that this protocol instance is running on.
  """
  @spec me(t()) :: service()
  def me(%{me: me}), do: me

  @doc """
  Return true if this node is the leader of the cluster.
  """
  @spec am_i_the_leader?(t()) :: boolean()
  def am_i_the_leader?(%{mode: %Leader{}}), do: true
  def am_i_the_leader?(_t), do: false

  @doc """
  Return the current leader of the cluster, if any.
  """
  @spec leader(t()) :: service() | :undecided
  def leader(%{mode: %Follower{}} = t), do: t.mode.leader
  def leader(%{mode: %Leader{}} = t), do: t.me
  def leader(%{mode: %Candidate{}}), do: :undecided

  @doc """
  Return the current term of the cluster.
  """
  @spec term(t()) :: Raft.election_term()
  def term(%{mode: %mode{}} = t)
      when mode in [Follower, Candidate, Leader],
      do: t.mode.term

  @doc """
  Return the curremt leader and term for the cluster.
  """
  @spec leadership(t()) :: Raft.leadership()
  def leadership(t), do: {leader(t), term(t)}

  @doc """
  Return the nodes in the cluster, including ourselves.
  """
  @spec known_nodes(t()) :: [service()]
  def known_nodes(t), do: [t.me | t.nodes]

  @doc """
  Add a transaction to the log. This is the only way to add transactions to the
  log. Transactions are not committed until the leader replicates them to a
  quorum of nodes. Transactions can only be submitted to the leader.
  """
  @spec add_transaction(t(), Raft.transaction()) ::
          {:ok, t()} | {:error, :not_leader}
  def add_transaction(%{mode: %mode{}} = t, transaction)
      when mode in [Leader] do
    mode.add_transaction(t.mode, transaction)
    |> case do
      {:ok, %Leader{} = leader} ->
        {:ok, %{t | mode: leader}}
    end
  end

  def add_transaction(_t, _transaction), do: {:error, :not_leader}

  @doc """
  Handle an event from outside the protocol. Timers, messages, etc. This is
  where the work gets done. Events can come from other nodes or from a timer.
  """
  @spec handle_event(t(), event :: any(), source :: service() | :timer) :: t()
  def handle_event(%{mode: %mode{}} = t, :election, :timer)
      when mode in [Follower, Candidate] do
    mode.cancel_timer(t.mode)

    %{t | mode: Candidate.new(next_term(t), t.quorum, t.nodes, log(t), t.interface)}
    |> notify_change_in_leadership()
  end

  def handle_event(%{mode: %mode{}} = t, :heartbeat, :timer)
      when mode in [Leader] do
    mode.timer_ticked(t.mode)
    |> case do
      :quorum_failed ->
        %{t | mode: Follower.new(next_term(t), log(t), t.interface)}
        |> notify_change_in_leadership()

      {:ok, %Leader{} = leader} ->
        %{t | mode: leader}
    end
  end

  def handle_event(
        %{mode: %mode{}} = t,
        {:request_vote, election_term, newest_transaction},
        _from = candidate
      )
      when mode in [Follower] do
    {:ok, %Follower{} = follower} =
      mode.vote_requested(t.mode, election_term, candidate, newest_transaction)

    %{t | mode: follower}
  end

  def handle_event(
        %{mode: %mode{}} = t,
        {:request_vote, election_term, newest_transaction},
        _from = candidate
      )
      when mode in [Candidate] and election_term > t.mode.term do
    mode.cancel_timer(t.mode)

    {:ok, %Follower{} = follower} =
      Follower.new(election_term, log(t), t.interface)
      |> Follower.vote_requested(election_term, candidate, newest_transaction)

    %{t | mode: follower}
  end

  def handle_event(%{mode: %mode{}} = t, {:vote, election_term}, from)
      when mode in [Candidate] do
    mode.vote_received(t.mode, election_term, from)
    |> case do
      :was_elected_leader ->
        %{t | mode: Leader.new(term(t), t.quorum, t.nodes, log(t), t.interface)}
        |> notify_change_in_leadership()

      {:ok, %Candidate{} = candidate} ->
        %{t | mode: candidate}
    end
  end

  def handle_event(
        %{mode: %mode{}} = t,
        {:append_entries, term, prev_transaction, transactions, commit_transaction},
        _from = leader
      )
      when mode in [Follower] do
    mode.append_entries_received(
      t.mode,
      term,
      prev_transaction,
      transactions,
      commit_transaction,
      leader
    )
    |> case do
      {:ok, %Follower{} = follower, :new_leader_elected} ->
        %{t | mode: follower}
        |> notify_change_in_leadership()

      {:ok, %Follower{} = follower} ->
        %{t | mode: follower}
    end
  end

  def handle_event(
        %{mode: %mode{}} = t,
        {:append_entries, term, prev_transaction, transactions, commit_transaction},
        _from = leader
      )
      when mode in [Candidate, Leader] do
    mode.append_entries_received(
      t.mode,
      term,
      prev_transaction,
      transactions,
      commit_transaction,
      leader
    )
    |> case do
      :new_leader_elected ->
        {:ok, %Follower{} = follower, :new_leader_elected} =
          Follower.new(term, log(t), t.interface)
          |> Follower.append_entries_received(
            term,
            prev_transaction,
            transactions,
            commit_transaction,
            leader
          )

        %{t | mode: follower}
        |> notify_change_in_leadership()

      {:ok, %Leader{} = leader} ->
        %{t | mode: leader}

      {:ok, %Candidate{} = candidate} ->
        %{t | mode: candidate}
    end
  end

  def handle_event(
        %{mode: %mode{}} = t,
        {:append_entries_ack, term, newest_transaction},
        _from = follower
      )
      when mode in [Leader] do
    {:ok, %Leader{} = leader} =
      mode.append_entries_ack_received(t.mode, term, newest_transaction, follower)

    %{t | mode: leader}
  end

  def handle_event(t, event, from) do
    apply(t.interface, :ignored_event, [event, from])
    t
  end

  @spec next_term(t()) :: Raft.election_term()
  defp next_term(t), do: term(t) + 1

  @spec determine_majority([service()]) :: non_neg_integer()
  defp determine_majority(nodes), do: 1 + (nodes |> length() |> div(2))

  @spec notify_change_in_leadership(t()) :: t()
  defp notify_change_in_leadership(t) do
    apply(t.interface, :leadership_changed, [t |> leadership()])
    t
  end
end
