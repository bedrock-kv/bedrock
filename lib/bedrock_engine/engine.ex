defmodule Bedrock.Engine do
  @callback one_time_initialization(path :: String.t()) :: :ok | {:error, term()}

  @type t :: pid() | atom() | {atom(), Node.t()}

  defmacro __using__(_) do
    quote do
      @behaviour Bedrock.Engine
      alias Bedrock.Engine.Controller

      @impl Bedrock.Engine
      def one_time_initialization(_path), do: :ok

      defoverridable one_time_initialization: 1
    end
  end

  @spec info(
          t(),
          fact_names :: [:supported_info | :id | :otp_name | :health],
          timeout :: :infinity | non_neg_integer()
        ) ::
          {:ok, keyword()} | {:error, term()}
  def info(engine, fact_names, timeout \\ 5_000) do
    GenServer.call(engine, {:info, fact_names}, timeout)
  catch
    :exit, {:noproc, {GenServer, :call, _}} ->
      {:error, :engine_does_not_exist}
  end
end
