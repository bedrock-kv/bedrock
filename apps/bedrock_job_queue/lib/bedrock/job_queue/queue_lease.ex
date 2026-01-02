defmodule Bedrock.JobQueue.QueueLease do
  @moduledoc """
  A lease on a queue for exclusive dequeuing.

  Per QuiCK paper: Two-tier leasing prevents thundering herd by first
  acquiring a queue lease, then item leases within that queue. Only one
  consumer can hold a queue lease at a time.
  """

  alias Bedrock.JobQueue.Expirable

  @type t :: %__MODULE__{
          id: binary(),
          queue_id: String.t(),
          holder: binary(),
          obtained_at: non_neg_integer(),
          expires_at: non_neg_integer()
        }

  defstruct [:id, :queue_id, :holder, :obtained_at, :expires_at]

  @default_duration_ms 5_000

  @doc """
  Creates a new queue lease.
  """
  @spec new(String.t(), binary(), pos_integer()) :: t()
  def new(queue_id, holder, duration_ms \\ @default_duration_ms) do
    now = System.system_time(:millisecond)

    %__MODULE__{
      id: :crypto.strong_rand_bytes(16),
      queue_id: queue_id,
      holder: holder,
      obtained_at: now,
      expires_at: now + duration_ms
    }
  end

  @doc """
  Returns true if the queue lease has expired.
  """
  defdelegate expired?(lease), to: Expirable

  @doc """
  Returns the remaining time on the lease in milliseconds.
  """
  defdelegate remaining_ms(lease), to: Expirable
end
