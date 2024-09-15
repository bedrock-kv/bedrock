defmodule Bedrock.Service.StorageWorker.Basalt.WaitingList do
  @moduledoc """
  A waiting list is a list of processes that are waiting for a specific version
  to be committed. It is used to implement MVCC in the Basalt storage engine.
  """
  use GenServer

  defstruct ~w[version waiting]a

  def wait_for_version(waiting_list, version, timeout) do
    GenServer.call(waiting_list, {:wait_for_version, version}, timeout)
  catch
    :exit, {:timeout, _} ->
      {:error, :timeout}
  end

  def notify_version_committed(waiting_list, version),
    do: GenServer.cast(waiting_list, {:version_committed, version})

  def start_link(version),
    do: GenServer.start_link(__MODULE__, version)

  def init(version), do: {:ok, %{version: version, waiting: []}}

  def handle_call(
        {:wait_for_version, version},
        from,
        %{version: current_version} = state
      )
      when version > current_version do
    {:noreply, %{state | waiting: [{from, version} | state.waiting]}}
  end

  def handle_call({:wait_for_version, _version}, _from, state),
    do: {:reply, :ok, state}

  def handle_cast({:version_committed, version}, %{waiting: []} = state),
    do: {:noreply, %{state | version: version}}

  def handle_cast({:version_committed, committed_version}, state) do
    new_wait_list =
      state.wait_list
      |> Enum.reduce([], fn
        {version, _from} = entry, acc when version > committed_version ->
          [entry | acc]

        {_version, from}, acc ->
          GenServer.reply(from, :ok)
          acc
      end)

    {:noreply, %{state | wait_list: new_wait_list}}
  end
end
