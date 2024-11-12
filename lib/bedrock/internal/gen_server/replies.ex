defmodule Bedrock.Internal.GenServer.Replies do
  @spec reply(t, result) :: {:reply, result, t} when t: term(), result: term()
  def reply(t, result), do: {:reply, result, t}

  @spec reply(t, result, continue: continue) :: {:reply, result, t, {:continue, continue}}
        when t: term(), result: term(), continue: term()
  def reply(t, result, continue: action), do: {:reply, result, t, {:continue, action}}

  @spec noreply(
          t,
          opts :: [
            continue: continue,
            timeout: timeout()
          ]
        ) ::
          {:noreply, t}
          | {:noreply, t, {:continue, continue} | timeout()}
        when t: term(), continue: term()
  def noreply(t, opts \\ [])
  def noreply(t, continue: continue), do: {:noreply, t, {:continue, continue}}
  def noreply(t, timeout: ms), do: {:noreply, t, ms}
  def noreply(t, []), do: {:noreply, t}
  def noreply(_, opts), do: raise("Invalid options: #{inspect(opts)}")

  @spec stop(t, reason) :: {:stop, reason, t} when t: term(), reason: term()
  def stop(t, reason), do: {:stop, reason, t}
end
