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
  """
  @spec new(Item.t(), binary()) :: t()
  @spec new(Item.t(), binary(), pos_integer() | keyword()) :: t()
  def new(item, holder, duration_or_opts \\ [])

  def new(%Item{} = item, holder, duration_ms) when is_integer(duration_ms) do
    now = System.system_time(:millisecond)
    expires_at = now + duration_ms

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

  def new(%Item{} = item, holder, opts) when is_list(opts) do
    duration = Keyword.get(opts, :duration_ms, @default_lease_duration_ms)
    new(item, holder, duration)
  end

  @doc """
  Returns true if the lease has expired.
  """
  defdelegate expired?(lease), to: Expirable

  @doc """
  Returns the remaining time on the lease in milliseconds.
  Returns 0 if expired.
  """
  defdelegate remaining_ms(lease), to: Expirable
end
