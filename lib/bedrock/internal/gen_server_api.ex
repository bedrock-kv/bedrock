defmodule Bedrock.Internal.GenServerApi do
  defmacro __using__(opts) do
    module = Keyword.get(opts, :for)

    quote do
      unquote do
        if module do
          quote do
            @doc false
            @spec child_spec(opts :: Keyword.t()) :: Supervisor.child_spec()
            defdelegate child_spec(opts), to: unquote(module)
          end
        end
      end

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

  @spec call(GenServer.name(), message :: any(), timeout() | :infinity) :: term()
  def call(server, message, timeout) do
    try do
      GenServer.call(server, message, timeout)
    catch
      :exit, {:noproc, _} -> {:error, :unavailable}
      :exit, {{:nodedown, _}, _} -> {:error, :unavailable}
      :exit, {:timeout, _} -> {:error, :timeout}
    end
  end
end
