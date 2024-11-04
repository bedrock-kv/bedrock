defmodule Bedrock.Internal.GenServer.Replies do
  @spec reply(t, result) :: {:reply, result, t} when t: term(), result: term()
  def reply(t, result), do: {:reply, result, t}

  @spec reply(t, result, continue: continue) :: {:reply, result, t, {:continue, continue}}
        when t: term(), result: term(), continue: term()
  def reply(t, result, continue: action), do: {:reply, result, t, {:continue, action}}

  @spec noreply(t) :: {:noreply, t} when t: term()
  def noreply(t), do: {:noreply, t}

  @spec noreply(t, continue: continue) :: {:noreply, t, {:continue, continue}}
        when t: term(), continue: term()
  def noreply(t, continue: continue), do: {:noreply, t, {:continue, continue}}
end
