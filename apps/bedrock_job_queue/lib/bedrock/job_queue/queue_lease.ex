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

  ## Options

  - `:duration_ms` - Lease duration in milliseconds (default: 5_000)
  - `:now` - Current time in milliseconds (default: System.system_time(:millisecond))
  """
  @spec new(String.t(), binary(), keyword()) :: t()
  def new(queue_id, holder, opts \\ [])

  def new(queue_id, holder, opts) do
    now = Keyword.get(opts, :now, System.system_time(:millisecond))
    duration = Keyword.get(opts, :duration_ms, @default_duration_ms)

    %__MODULE__{
      id: :crypto.strong_rand_bytes(16),
      queue_id: queue_id,
      holder: holder,
      obtained_at: now,
      expires_at: now + duration
    }
  end

  @doc """
  Returns true if the queue lease has expired.

  ## Options

  - `:now` - Current time in milliseconds (default: System.system_time(:millisecond))
  """
  @dialyzer {:nowarn_function, expired?: 2}
  @spec expired?(t(), keyword()) :: boolean()
  def expired?(lease, opts \\ []), do: Expirable.expired?(lease, opts)

  @doc """
  Returns the remaining time on the lease in milliseconds.

  ## Options

  - `:now` - Current time in milliseconds (default: System.system_time(:millisecond))
  """
  @dialyzer {:nowarn_function, remaining_ms: 2}
  @spec remaining_ms(t(), keyword()) :: non_neg_integer()
  def remaining_ms(lease, opts \\ []), do: Expirable.remaining_ms(lease, opts)
end
