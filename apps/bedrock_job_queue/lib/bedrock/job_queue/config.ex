defmodule Bedrock.JobQueue.Config do
  @moduledoc """
  Configuration and defaults for the job queue system.

  This module provides configuration struct and utility functions used by the
  job queue consumer components. The main entry point for configuration is through
  `use Bedrock.JobQueue` in your application's JobQueue module.

  ## Fields

  - `:repo` - The Bedrock Repo module for database operations
  - `:concurrency` - Maximum concurrent job workers (default: `System.schedulers_online()`)
  - `:batch_size` - Number of items to dequeue per batch (default: 10)

  ## Backoff

  The `default_backoff/1` function provides exponential backoff with jitter for
  failed job retries. Custom backoff functions can be passed via `:backoff_fn`
  when starting the consumer.
  """

  @type t :: %__MODULE__{
          repo: module(),
          concurrency: pos_integer(),
          batch_size: pos_integer()
        }

  defstruct [
    :repo,
    concurrency: System.schedulers_online(),
    batch_size: 10
  ]

  @doc """
  Creates a new configuration from options.

  ## Options

  - `:repo` - Required. The Bedrock Repo module
  - `:concurrency` - Max concurrent workers (default: `System.schedulers_online()`)
  - `:batch_size` - Items per dequeue batch (default: 10)
  """
  @spec new(keyword()) :: t()
  def new(opts), do: struct!(__MODULE__, opts)

  @doc """
  Default exponential backoff function.

  Returns milliseconds to wait before retry based on attempt number (0-indexed).

  ## Formula

  `delay = 2^attempt * 1000 + random(0..500)`

  This produces delays of approximately: 1s, 2s, 4s, 8s, 16s, 32s, etc.
  with up to 500ms of random jitter to prevent thundering herd.

  ## Examples

      iex> delay = Config.default_backoff(0)
      iex> delay >= 1000 and delay <= 1500
      true

      iex> delay = Config.default_backoff(3)
      iex> delay >= 8000 and delay <= 8500
      true
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
