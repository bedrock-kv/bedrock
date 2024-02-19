defmodule Bedrock.Worker do
  @callback one_time_initialization(path :: String.t()) :: :ok | {:error, term()}

  @type t :: pid() | atom() | {atom(), Node.t()}

  defmacro __using__(_) do
    quote do
      @behaviour Bedrock.Worker
      alias Bedrock.WorkerController

      @impl Bedrock.Worker
      def one_time_initialization(_path), do: :ok

      defoverridable one_time_initialization: 1
    end
  end

  @spec info(
          t(),
          fact_names :: [:supported_info | :id | :otp_name | :health],
          timeout :: :infinity | non_neg_integer()
        ) ::
          {:ok, keyword()} | {:error, :unavailable}
  @spec info(atom() | pid() | {atom(), atom()}, [:health | :id | :otp_name | :supported_info]) ::
          {:error, :unavailable} | {:ok, keyword()}
  def info(engine, fact_names, timeout \\ 5_000) do
    GenServer.call(engine, {:info, fact_names}, timeout)
  catch
    :exit, {:noproc, {GenServer, :call, _}} -> {:error, :unavailable}
  end
end
