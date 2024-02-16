defmodule Bedrock.Raft.Interface do
  @type cancel_timer_fn :: (-> :ok)

  @doc """
  An unhandled event has occurred.
  """
  @callback ignored_event(event :: any(), from :: Raft.service() | :timer) :: :ok

  @doc """
  Set the leader of the Raft cluster.
  """
  @callback leadership_changed(Raft.leadership()) :: :ok

  @doc """
  Set a timer for a Raft node. It's expected that the protocol will be notified
  when the timer expires.
  """
  @callback timer(
              name :: atom(),
              min_millis :: non_neg_integer(),
              max_millis :: non_neg_integer()
            ) ::
              cancel_timer_fn()

  @doc """
  Send an event to a Raft node.
  """
  @callback send_event(to :: Raft.service(), event :: any()) :: :ok
end
