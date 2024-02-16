defmodule Bedrock.Raft.Mode.Leader.FollowerTracking do
  @moduledoc """
  The FollowerTracking module is responsible for keeping track of the state of
  followers. This includes the last transaction that was sent to a follower, the
  newest transaction that the follower has acknowledged, and the newest
  transaction that a quorum of followers has acknowledged. This information is
  used to determine the commit index.
  """

  @type t :: :ets.table()

  alias Bedrock.Raft

  @spec new(any()) :: t()
  def new(followers) do
    t = :ets.new(:follower_tracking, [:set])
    :ets.insert(t, followers |> Enum.map(&{&1, :unknown, :unknown}))
    t
  end

  @spec last_sent_transaction(t(), Raft.service()) :: Raft.transaction() | :unknown
  def last_sent_transaction(t, follower) do
    :ets.lookup(t, follower)
    |> case do
      [{^follower, last_sent_transaction, _newest_transaction}] -> last_sent_transaction
      [] -> nil
    end
  end

  @spec newest_transaction(t(), Raft.service()) :: Raft.transaction() | :unknown
  def newest_transaction(t, follower) do
    :ets.lookup(t, follower)
    |> case do
      [{^follower, _last_sent_transaction, newest_transaction}] -> newest_transaction
      [] -> nil
    end
  end

  @doc """
  Find the highest commit that a majority of followers have acknowledged. We
  can do this by sorting the list of last_transaction_acked and taking the
  quorum-th-from-last element.
  """
  @spec newest_safe_transaction(t(), quorum :: non_neg_integer()) :: Raft.transaction() | :unknown
  def newest_safe_transaction(t, quorum) do
    :ets.select(t, [{{:_, :_, :"$3"}, [], [:"$3"]}])
    |> Enum.sort()
    |> Enum.at(quorum) || :unknown
  end

  @spec update_last_sent_transaction(t(), Raft.service(), Raft.transaction()) :: t()
  def update_last_sent_transaction(t, follower, last_transaction_sent) do
    :ets.update_element(t, follower, {2, last_transaction_sent})
    t
  end

  @spec update_newest_transaction(t(), Raft.service(), Raft.transaction()) :: t()
  def update_newest_transaction(t, follower, newest_transaction) do
    :ets.update_element(t, follower, [{2, newest_transaction}, {3, newest_transaction}])
    t
  end
end
