defmodule Bedrock.Internal.GenServerApi do
  defmacro __using__(opts) do
    module = Keyword.get(opts, :for, Module.concat([__CALLER__.module, Server]))

    quote do
      @doc false
      @spec child_spec(opts :: Keyword.t()) :: Supervisor.child_spec()
      defdelegate child_spec(opts), to: unquote(module)

      import Bedrock.Internal.GenServerApi,
        only: [
          cast: 2,
          call: 3,
          broadcast: 3
        ]
    end
  end

  @spec broadcast([GenServer.name()], otp_name :: GenServer.name(), message :: any()) :: :ok
  def broadcast(nodes, otp_name, message) do
    GenServer.abcast(nodes, otp_name, message)
    :ok
  end

  @spec cast(GenServer.name(), message :: any()) :: :ok
  def cast(server, message), do: GenServer.cast(server, message)

  @spec call(GenServer.name(), message :: any(), timeout()) ::
          :ok | {:ok, any()} | {:error, :unavailable}
  def call(server, message, timeout) do
    GenServer.call(server, message, timeout)
  catch
    :exit, _ -> {:error, :unavailable}
  end
end
