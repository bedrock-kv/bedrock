defmodule Bedrock.JobQueue.QueueLease do
  @moduledoc """
  A lease on a queue for exclusive dequeuing.

  Per [QuiCK paper](https://www.foundationdb.org/files/QuiCK.pdf): Two-tier
  leasing prevents thundering herd by first acquiring a queue lease, then
  item leases within that queue. Only one consumer can hold a queue lease
  at a time.

  ## Default Duration

  Queue leases default to 5 seconds (vs 30 seconds for item leases). This is
  intentionally short because queue leases only protect the dequeue operation,
  not job execution. A consumer holds the queue lease just long enough to:

  1. Peek for visible items
  2. Obtain item leases on those items
  3. Release the queue lease

  Item leases are held much longer to cover actual job execution time.
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
      id: derive_id(queue_id, holder, now),
      queue_id: queue_id,
      holder: holder,
      obtained_at: now,
      expires_at: now + duration
    }
  end

  @doc """
  Derives a deterministic queue lease ID from inputs.

  This makes queue lease IDs predictable for testing while still being unique
  per queue/holder/time combination.
  """
  @spec derive_id(String.t(), binary(), non_neg_integer()) :: binary()
  def derive_id(queue_id, holder, now) do
    :sha256
    |> :crypto.hash([queue_id, to_string(holder), <<now::64>>])
    |> binary_part(0, 16)
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
