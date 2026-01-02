defmodule Bedrock.JobQueue.Config do
  @moduledoc """
  Configuration for the job queue system.
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
