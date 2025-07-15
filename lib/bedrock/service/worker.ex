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
  @type timeout_in_ms :: Bedrock.timeout_in_ms()
  @type health :: {:ok, pid()} | :stopped | {:error, term()}
  @type otp_name :: atom()

  @spec random_id() :: binary()
  def random_id, do: :crypto.strong_rand_bytes(5) |> Base.encode32(case: :lower)

  @spec info(worker :: ref(), [fact_name() | atom()], opts :: [timeout_in_ms: timeout_in_ms()]) ::
          {:ok, %{fact_name() => any()}} | {:error, :unavailable}
  @spec info(GenServer.server(), term(), opts :: [timeout_in_ms: timeout_in_ms()]) :: term()
  def info(worker, fact_names, opts \\ []),
    do: call(worker, {:info, fact_names}, opts[:timeout_in_ms] || :infinity)

  @spec lock_for_recovery(
          worker :: ref(),
          epoch :: Bedrock.epoch(),
          opts :: [timeout_in_ms: timeout_in_ms()]
        ) ::
          {:ok, pid(), recovery_info :: map()} | {:error, :newer_epoch_exists}
  @spec lock_for_recovery(GenServer.server(), integer(), opts :: [timeout_in_ms: timeout_in_ms()]) ::
          {:ok, term()} | {:error, term()}
  def lock_for_recovery(worker, epoch, opts \\ []),
    do: call(worker, {:lock_for_recovery, epoch}, opts[:timeout_in_ms] || :infinity)
end
