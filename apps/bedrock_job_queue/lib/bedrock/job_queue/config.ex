defmodule Bedrock.JobQueue.Config do
  @moduledoc """
  Configuration for the job queue system.
  """

  @type t :: %__MODULE__{
          repo: module(),
          concurrency: pos_integer(),
          batch_size: pos_integer(),
          scan_interval_ms: pos_integer(),
          max_time_per_queue_ms: pos_integer(),
          item_lease_duration_ms: pos_integer(),
          queue_lease_duration_ms: pos_integer(),
          max_retries: pos_integer(),
          backoff_fn: (non_neg_integer() -> non_neg_integer()),
          telemetry_prefix: [atom()]
        }

  defstruct [
    :repo,
    concurrency: System.schedulers_online(),
    batch_size: 10,
    scan_interval_ms: 100,
    max_time_per_queue_ms: 50,
    item_lease_duration_ms: 30_000,
    queue_lease_duration_ms: 60_000,
    max_retries: 3,
    backoff_fn: &__MODULE__.default_backoff/1,
    telemetry_prefix: [:bedrock, :job_queue]
  ]

  @doc """
  Creates a new configuration from options.
  """
  @spec new(keyword()) :: t()
  def new(opts) do
    struct!(__MODULE__, opts)
  end

  @doc """
  Default exponential backoff function.

  Returns milliseconds to wait before retry based on attempt number.
  """
  @spec default_backoff(non_neg_integer()) :: non_neg_integer()
  def default_backoff(attempt) when attempt >= 0 do
    # Exponential backoff: 1s, 2s, 4s, 8s, 16s, ...
    # With jitter: add random 0-500ms
    base = 2 |> :math.pow(attempt) |> trunc() |> Kernel.*(1000)
    jitter = :rand.uniform(500)
    base + jitter
  end
end
