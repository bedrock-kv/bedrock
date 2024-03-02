defmodule Bedrock.Service.Worker do
  @moduledoc """
  A worker is a process that is managed by a controller. It is a GenServer that
  is started and stopped by the controller. It is expected to provide a set of
  facts about itself when requested.
  """

  @type t :: pid() | atom() | {atom(), Node.t()}
  @type fact_name :: :supported_info | :kind | :id | :health | :otp_name
  @type timeout_in_ms :: Bedrock.timeout_in_ms()

  @spec info(t(), [fact_name()]) :: {:ok, keyword()} | {:error, :unavailable}
  @spec info(t(), [fact_name()], timeout_in_ms()) :: {:ok, keyword()} | {:error, :unavailable}
  def info(worker, fact_names, timeout \\ 5_000) do
    GenServer.call(worker, {:info, fact_names}, timeout)
  catch
    :exit, {:noproc, {GenServer, :call, _}} -> {:error, :unavailable}
  end
end
