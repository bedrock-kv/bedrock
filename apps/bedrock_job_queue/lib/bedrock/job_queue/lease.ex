defmodule Bedrock.JobQueue.Lease do
  @moduledoc """
  A lease on a job item.

  Per QuiCK paper: Leasing works by updating vesting_time to make items "invisible"
  rather than removing them from the queue. If a worker fails, the lease expires
  and the item becomes visible again automatically.
  """

  alias Bedrock.JobQueue.Expirable
  alias Bedrock.JobQueue.Item

  @type t :: %__MODULE__{
          id: binary(),
          item_id: binary(),
          queue_id: String.t(),
          holder: binary(),
          obtained_at: non_neg_integer(),
          expires_at: non_neg_integer(),
          item_key: tuple() | nil
        }

  defstruct [:id, :item_id, :queue_id, :holder, :obtained_at, :expires_at, :item_key]

  @default_lease_duration_ms 30_000

  @doc """
  Creates a new lease for an item.

  ## Options

  - `:duration_ms` - Lease duration in milliseconds (default: 30_000)
  - `:now` - Current time in milliseconds (default: System.system_time(:millisecond))
  """
  @spec new(Item.t(), binary(), keyword()) :: t()
  def new(item, holder, opts \\ [])

  def new(%Item{} = item, holder, opts) do
    now = Keyword.get(opts, :now, System.system_time(:millisecond))
    duration = Keyword.get(opts, :duration_ms, @default_lease_duration_ms)
    expires_at = now + duration

    # Store the NEW item key (with updated vesting_time) for O(1) lookup on complete/requeue
    # After leasing, item's vesting_time becomes expires_at
    item_key = {item.priority, expires_at, item.id}

    %__MODULE__{
      id: :crypto.strong_rand_bytes(16),
      item_id: item.id,
      queue_id: item.queue_id,
      holder: holder,
      obtained_at: now,
      expires_at: expires_at,
      item_key: item_key
    }
  end

  @doc """
  Returns true if the lease has expired.

  ## Options

  - `:now` - Current time in milliseconds (default: System.system_time(:millisecond))
  """
  @dialyzer {:nowarn_function, expired?: 2}
  @spec expired?(t(), keyword()) :: boolean()
  def expired?(lease, opts \\ []), do: Expirable.expired?(lease, opts)

  @doc """
  Returns the remaining time on the lease in milliseconds.
  Returns 0 if expired.

  ## Options

  - `:now` - Current time in milliseconds (default: System.system_time(:millisecond))
  """
  @dialyzer {:nowarn_function, remaining_ms: 2}
  @spec remaining_ms(t(), keyword()) :: non_neg_integer()
  def remaining_ms(lease, opts \\ []), do: Expirable.remaining_ms(lease, opts)
end
