defmodule Bedrock.Raft.Mode.Leader.FollowerTracking do
  @moduledoc """
  The FollowerTracking module is responsible for keeping track of the state of
  followers. This includes the last transaction id that was sent to a follower,
  the newest transaction id that the follower has acknowledged, and the newest
  transaction id that a quorum of followers has acknowledged. This information
  is used to determine the commit index.
  """

  @type t :: :ets.table()

  alias Bedrock.Raft

  @spec new(any()) :: t()
  def new(followers) do
    t = :ets.new(:follower_tracking, [:set])
    :ets.insert(t, followers |> Enum.map(&{&1, :unknown, :unknown}))
    t
  end

  @spec last_sent_transaction_id(t(), Raft.service()) :: Raft.transaction_id() | :unknown
  def last_sent_transaction_id(t, follower) do
    :ets.lookup(t, follower)
    |> case do
      [{^follower, last_sent_transaction_id, _newest_transaction_id}] -> last_sent_transaction_id
      [] -> nil
    end
  end

  @spec newest_transaction_id(t(), Raft.service()) :: Raft.transaction_id() | :unknown
  def newest_transaction_id(t, follower) do
    :ets.lookup(t, follower)
    |> case do
      [{^follower, _last_sent_transaction_id, newest_transaction_id}] -> newest_transaction_id
      [] -> nil
    end
  end

  @doc """
  Find the highest commit that a majority of followers have acknowledged. We
  can do this by sorting the list of last_transaction_id_acked and taking the
  quorum-th-from-last element -- every follower above this index has already
  acknowledged *at-least* up to this transaction.
  """
  @spec newest_safe_transaction_id(t(), quorum :: non_neg_integer()) ::
          Raft.transaction_id() | :unknown
  def newest_safe_transaction_id(t, quorum) do
    :ets.select(t, [{{:_, :_, :"$3"}, [], [:"$3"]}])
    |> Enum.sort()
    |> Enum.at(quorum, :unknown)
  end

  @spec update_last_sent_transaction_id(t(), Raft.service(), Raft.transaction_id()) :: t()
  def update_last_sent_transaction_id(t, follower, last_transaction_id_sent) do
    :ets.update_element(t, follower, {2, last_transaction_id_sent})
    t
  end

  @spec update_newest_transaction_id(t(), Raft.service(), Raft.transaction_id()) :: t()
  def update_newest_transaction_id(t, follower, newest_transaction_id) do
    :ets.update_element(t, follower, [{2, newest_transaction_id}, {3, newest_transaction_id}])
    t
  end
end
