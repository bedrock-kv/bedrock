defmodule Bedrock.DataPlane.Resolver.Server do
  alias Bedrock.DataPlane.Resolver.State

  import Bedrock.DataPlane.Resolver.Recovery, only: [recover_from: 4]
  import Bedrock.DataPlane.Resolver.ConflictResolution, only: [commit: 3]

  use GenServer

  def child_spec(_opts) do
    %{
      id: __MODULE__,
      start:
        {GenServer, :start_link,
         [
           __MODULE__,
           {}
         ]},
      restart: :temporary
    }
  end

  @impl true
  def init({}) do
    %State{}
    |> then(&{:ok, &1})
  end

  @impl true
  def handle_call({:recover_from, source_log, first_version, last_version}, _from, t) do
    case recover_from(t, source_log, first_version, last_version) do
      {:ok, t} -> t |> reply(:ok)
      {:error, reason} -> t |> reply({:error, reason})
    end
  end

  # When transactions come in order, we can resolve them immediately. Once we're
  # done, we check if there are any transactions waiting for this version to be
  # resolved, and if so, we resolve them as well. We reply to this caller before
  # we do to avoid blocking them.
  def handle_call({:resolve_transactions, {last_version, next_version}, transactions}, _from, t)
      when last_version == t.last_version do
    {tree, aborted} = commit(t.tree, transactions, next_version)
    t = %{t | tree: tree, last_version: next_version}

    if Map.has_key?(t.waiting, next_version) do
      t |> reply({:ok, aborted}, continue: {:resolve_next, next_version})
    else
      t |> reply({:ok, aborted})
    end
  end

  # When transactions come in a little out of order, we need to wait for the
  # previous transaction to be resolved before we can resolve the next one.
  def handle_call({:resolve_transactions, {last_version, next_version}, transactions}, from, t) do
    %{t | waiting: Map.put(t.waiting, last_version, {next_version, transactions, from})}
    |> noreply()
  end

  @impl true
  def handle_info({:resolve_next, next_version}, t) do
    {{next_version, transactions, from}, waiting} = Map.pop(t.waiting, next_version)

    {tree, aborted} = commit(t.tree, transactions, next_version)
    t = %{t | tree: tree, last_version: next_version, waiting: waiting}

    GenServer.reply(from, {:ok, aborted})

    if Map.has_key?(t.waiting, next_version) do
      t |> noreply(continue: {:resolve_next, next_version})
    else
      t |> noreply()
    end
  end

  defp reply(%State{} = t, result), do: {:reply, result, t}
  defp reply(%State{} = t, result, continue: action), do: {:reply, result, t, {:continue, action}}

  defp noreply(%State{} = t), do: {:noreply, t}
  defp noreply(%State{} = t, continue: action), do: {:noreply, t, {:continue, action}}
end
