defmodule Bedrock.JobQueue.Item do
  @moduledoc """
  A job item in the queue.

  ## Fields

  - `id` - Unique job identifier (UUID binary)
  - `topic` - Job type/topic (Phoenix PubSub-style, e.g., "user:created")
  - `priority` - Integer priority (lower = higher priority)
  - `vesting_time` - When the job becomes visible (milliseconds since epoch)
  - `lease_id` - Current lease holder (nil if available)
  - `lease_expires_at` - When the lease expires
  - `error_count` - Number of failed attempts
  - `max_retries` - Maximum retry attempts
  - `payload` - Job-specific data (binary, typically JSON)
  - `queue_id` - The queue/tenant this job belongs to
  """

  alias Bedrock.JobQueue.Payload

  @type t :: %__MODULE__{
          id: binary(),
          topic: String.t(),
          priority: non_neg_integer(),
          vesting_time: non_neg_integer(),
          lease_id: binary() | nil,
          lease_expires_at: non_neg_integer() | nil,
          error_count: non_neg_integer(),
          max_retries: non_neg_integer(),
          payload: binary(),
          queue_id: String.t()
        }

  defstruct [
    :id,
    :topic,
    :priority,
    :vesting_time,
    :lease_id,
    :lease_expires_at,
    :error_count,
    :max_retries,
    :payload,
    :queue_id
  ]

  @default_priority 100
  @default_max_retries 3

  @doc """
  Creates a new job item with defaults.

  ## Options

  - `:id` - Custom job ID (default: random 16-byte binary)
  - `:priority` - Integer priority, lower = higher priority (default: 100)
  - `:vesting_time` - When the job becomes visible in ms since epoch (default: now)
  - `:max_retries` - Maximum retry attempts before dead-lettering (default: 3)
  - `:now` - Current time in ms, used for vesting_time default (default: System.system_time(:millisecond))

  ## Priority Ordering

  Jobs are processed in priority order where **lower values = higher priority**.
  For example, priority 0 is processed before priority 100. Use non-negative
  integers only; negative priorities are not supported.
  """
  @spec new(String.t(), String.t(), term(), keyword()) :: t()
  def new(queue_id, topic, payload, opts \\ []) do
    now = Keyword.get(opts, :now, System.system_time(:millisecond))

    %__MODULE__{
      id: Keyword.get(opts, :id, generate_id()),
      topic: topic,
      priority: Keyword.get(opts, :priority, @default_priority),
      vesting_time: Keyword.get(opts, :vesting_time, now),
      lease_id: nil,
      lease_expires_at: nil,
      error_count: 0,
      max_retries: Keyword.get(opts, :max_retries, @default_max_retries),
      payload: Payload.encode(payload),
      queue_id: queue_id
    }
  end

  @doc """
  Returns true if the job is currently visible (vesting_time has passed and not leased).
  """
  @spec visible?(t()) :: boolean()
  @spec visible?(t(), non_neg_integer()) :: boolean()
  def visible?(item, now \\ System.system_time(:millisecond))

  def visible?(%__MODULE__{vesting_time: vt, lease_id: nil}, now) do
    now >= vt
  end

  def visible?(%__MODULE__{}, _now), do: false

  @doc """
  Returns true if the job is currently leased.

  ## Options

  - `:now` - Current time in milliseconds (default: System.system_time(:millisecond))
  """
  @spec leased?(t(), keyword()) :: boolean()
  def leased?(item, opts \\ [])

  def leased?(%__MODULE__{lease_id: nil}, _opts), do: false

  def leased?(%__MODULE__{lease_expires_at: exp}, opts) when not is_nil(exp) do
    now = Keyword.get(opts, :now, System.system_time(:millisecond))
    now < exp
  end

  def leased?(_, _opts), do: false

  @doc """
  Returns true if retries are exhausted.
  """
  @spec exhausted?(t()) :: boolean()
  def exhausted?(%__MODULE__{error_count: ec, max_retries: mr}), do: ec >= mr

  @doc """
  Builds the storage key tuple for this item.

  Keys are `{priority, vesting_time, id}` which sorts items by priority first,
  then by vesting time, then by unique id.
  """
  @spec key(t()) :: {non_neg_integer(), non_neg_integer(), binary()}
  def key(%__MODULE__{priority: p, vesting_time: vt, id: id}), do: {p, vt, id}

  defp generate_id, do: :crypto.strong_rand_bytes(16)
end
