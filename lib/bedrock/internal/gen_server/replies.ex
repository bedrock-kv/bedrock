defmodule Bedrock.Internal.GenServer.Replies do
  @spec reply(state, result) :: {:reply, result, state} when state: term(), result: term()
  def reply(t, result), do: {:reply, result, t}

  @spec reply(state, result, [{:continue, continue}]) ::
          {:reply, result, state, {:continue, continue}}
        when state: term(), result: term(), continue: term()
  def reply(t, result, continue: action), do: {:reply, result, t, {:continue, action}}

  @spec noreply(
          state,
          opts :: [
            continue: continue,
            timeout: timeout()
          ]
        ) ::
          {:noreply, state}
          | {:noreply, state, {:continue, continue} | timeout()}
        when state: term(), continue: term()
  def noreply(t, opts \\ [])
  def noreply(t, continue: continue), do: {:noreply, t, {:continue, continue}}
  def noreply(t, timeout: ms), do: {:noreply, t, ms}
  def noreply(t, []), do: {:noreply, t}
  def noreply(_, opts), do: raise("Invalid options: #{inspect(opts)}")

  @spec stop(state, reason) :: {:stop, reason, state} when state: term(), reason: term()
  def stop(t, reason), do: {:stop, reason, t}
end
