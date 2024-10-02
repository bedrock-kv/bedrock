defmodule Bedrock.Service.Worker do
  @moduledoc """
  A worker is a GenServer that is started and stopped by a service controller.
  It is expected to provide a set of facts about itself when requested along
  with other services (as befits the type of worker.)
  """

  @type ref :: GenServer.server()
  @type id :: String.t()
  @type fact_name :: :supported_info | :kind | :id | :health | :otp_name | :pid
  @type timeout_in_ms :: Bedrock.timeout_in_ms()
  @type health :: :ok | :starting | {:error, term()}
  @type otp_name :: atom()

  @spec info(worker :: ref(), [fact_name()]) :: {:ok, keyword()} | {:error, :unavailable}
  @spec info(worker :: ref(), [fact_name()], timeout_in_ms()) ::
          {:ok, keyword()} | {:error, :unavailable}
  def info(worker, fact_names, timeout \\ 5_000) do
    GenServer.call(worker, {:info, fact_names}, timeout)
  catch
    :exit, {:noproc, {GenServer, :call, _}} -> {:error, :unavailable}
  end
end
