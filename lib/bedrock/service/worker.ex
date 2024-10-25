defmodule Bedrock.Service.Worker do
  @moduledoc """
  A worker is a GenServer that is started and stopped by a service controller.
  It is expected to provide a set of facts about itself when requested along
  with other services (as befits the type of worker.)
  """

  use Bedrock.Internal.GenServerApi

  @type ref :: GenServer.server()
  @type id :: Bedrock.service_id()
  @type fact_name :: :supported_info | :kind | :id | :health | :otp_name | :pid
  @type timeout_in_ms :: Bedrock.timeout_in_ms()
  @type health :: :ok | :starting | {:error, term()}
  @type otp_name :: atom()

  @spec info(worker :: ref(), [fact_name()]) :: {:ok, map()} | {:error, :unavailable}
  @spec info(worker :: ref(), [fact_name()], opts :: keyword()) ::
          {:ok, map()} | {:error, :unavailable}
  def info(worker, fact_names, opts \\ []),
    do: call(worker, {:info, fact_names}, opts[:timeout] || :infinity)

  @spec lock_for_recovery(worker :: ref(), epoch :: Bedrock.epoch(), opts :: keyword()) ::
          {:ok, pid(), recovery_info :: map()} | {:error, :newer_epoch_exists}
  def lock_for_recovery(worker, epoch, opts \\ []),
    do: call(worker, {:lock_for_recovery, epoch}, opts[:timeout] || :infinity)
end
