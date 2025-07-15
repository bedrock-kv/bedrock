defmodule Bedrock.Internal.GenServer.Calls do
  @spec broadcast([GenServer.name()], otp_name :: GenServer.name(), message :: term()) :: :ok
  def broadcast(nodes, otp_name, message) do
    GenServer.abcast(nodes, otp_name, message)
    :ok
  end

  @spec cast(GenServer.server(), message :: term()) :: :ok
  def cast(server, message), do: GenServer.cast(server, message)

  @spec call(GenServer.server(), message :: term(), timeout()) :: term()
  def call(server, message, timeout) do
    GenServer.call(server, message, normalize_timeout(timeout))
  rescue
    _ -> {:error, :unknown}
  catch
    :exit, {:noproc, _} -> {:error, :unavailable}
    :exit, {{:nodedown, _}, _} -> {:error, :unavailable}
    :exit, {:timeout, _} -> {:error, :timeout}
  end

  @spec normalize_timeout(timeout()) :: timeout()
  defp normalize_timeout(:infinity), do: :infinity
  defp normalize_timeout(timeout) when is_integer(timeout) and timeout >= 0, do: timeout
  defp normalize_timeout(_), do: 5000
end
