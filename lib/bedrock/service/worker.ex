defmodule Bedrock.Service.Worker do
  @moduledoc """
  A worker is a GenServer that is started and stopped by a service director.
  It is expected to provide a set of facts about itself when requested along
  with other services (as befits the type of worker.)
  """

  use Bedrock.Internal.GenServerApi

  @type ref :: GenServer.server()
  @type id :: Bedrock.service_id()
  @type fact_name :: :supported_info | :kind | :id | :health | :otp_name | :pid
  @type fact_value :: [fact_name()] | :log | :storage | binary() | health() | atom() | pid()
  @type timeout_in_ms :: Bedrock.timeout_in_ms()
  @type health :: {:ok, pid()} | :stopped | {:error, :timeout | :unavailable}
  @type otp_name :: atom()

  @spec random_id() :: binary()
  def random_id, do: :crypto.strong_rand_bytes(5) |> Base.encode32(case: :lower)

  @spec info(
          worker_ref :: ref(),
          requested_facts :: [fact_name() | atom()],
          opts :: [timeout_in_ms: timeout_in_ms()]
        ) ::
          {:ok, facts :: %{fact_name() => fact_value()}} | {:error, :unavailable}
  def info(worker, fact_names, opts \\ []),
    do: call(worker, {:info, fact_names}, opts[:timeout_in_ms] || :infinity)

  @spec lock_for_recovery(
          worker_ref :: ref(),
          recovery_epoch :: Bedrock.epoch(),
          opts :: [timeout_in_ms: timeout_in_ms()]
        ) ::
          {:ok, worker_pid :: pid(), recovery_info :: keyword()}
          | {:error, :newer_epoch_exists}
          | {:error, :timeout}
  def lock_for_recovery(worker, epoch, opts \\ []),
    do: call(worker, {:lock_for_recovery, epoch}, opts[:timeout_in_ms] || :infinity)
end
