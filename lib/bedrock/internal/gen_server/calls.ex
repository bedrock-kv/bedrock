defmodule Bedrock.Internal.GenServer.Calls do
  @moduledoc """
  Utilities for GenServer call and cast operations.
  """
  @spec broadcast([node()], otp_name :: atom(), message :: term()) :: :ok
  def broadcast(nodes, otp_name, message) do
    GenServer.abcast(nodes, otp_name, message)
    :ok
  end

  @spec cast(GenServer.server(), message :: term()) :: :ok
  def cast(server, message), do: GenServer.cast(server, message)

  @spec call(GenServer.server(), message :: term(), timeout()) ::
          term()
          | {:error, :unavailable}
          | {:error, :timeout}
          | {:error, :unknown}
  def call(server, message, timeout) do
    GenServer.call(server, message, normalize_timeout(timeout))
  rescue
    _ -> {:error, :unknown}
  catch
    :exit, {:noproc, _} -> {:error, :unavailable}
    :exit, {{:nodedown, _}, _} -> {:error, :unavailable}
    :exit, {:timeout, _} -> {:error, :timeout}
  end

  @spec call!(GenServer.server(), message :: term(), timeout()) :: term()
  def call!(server, message, timeout) do
    GenServer.call(server, message, normalize_timeout(timeout))
  rescue
    error -> raise "GenServer call failed: #{inspect(error)}"
  catch
    :exit, {:noproc, _} -> raise "GenServer call failed: #{inspect(:unavailable)}"
    :exit, {{:nodedown, _}, _} -> raise "GenServer call failed: #{inspect(:unavailable)}"
    :exit, {:timeout, _} -> raise "GenServer call failed: #{inspect(:timeout)}"
  end

  @spec normalize_timeout(timeout()) :: timeout()
  defp normalize_timeout(:infinity), do: :infinity
  defp normalize_timeout(timeout) when is_integer(timeout) and timeout >= 0, do: timeout
  defp normalize_timeout(_), do: 5000
end
