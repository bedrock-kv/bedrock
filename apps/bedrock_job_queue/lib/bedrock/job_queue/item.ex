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
  """
  @spec new(String.t(), String.t(), term(), keyword()) :: t()
  def new(queue_id, topic, payload, opts \\ []) do
    now = System.system_time(:millisecond)

    %__MODULE__{
      id: Keyword.get(opts, :id, generate_id()),
      topic: topic,
      priority: Keyword.get(opts, :priority, @default_priority),
      vesting_time: Keyword.get(opts, :vesting_time, now),
      lease_id: nil,
      lease_expires_at: nil,
      error_count: 0,
      max_retries: Keyword.get(opts, :max_retries, @default_max_retries),
      payload: encode_payload(payload),
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
  """
  @spec leased?(t()) :: boolean()
  def leased?(%__MODULE__{lease_id: nil}), do: false

  def leased?(%__MODULE__{lease_expires_at: exp}) when not is_nil(exp) do
    System.system_time(:millisecond) < exp
  end

  def leased?(_), do: false

  @doc """
  Returns true if retries are exhausted.
  """
  @spec exhausted?(t()) :: boolean()
  def exhausted?(%__MODULE__{error_count: ec, max_retries: mr}), do: ec >= mr

  defp generate_id, do: :crypto.strong_rand_bytes(16)

  defp encode_payload(payload) when is_binary(payload), do: payload
  defp encode_payload(payload), do: Jason.encode!(payload)
end
