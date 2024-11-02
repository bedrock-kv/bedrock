defmodule Bedrock.DataPlane.Sequencer.Server do
  alias Bedrock.DataPlane.Sequencer.State

  import Bedrock.DataPlane.Sequencer.ControllerFeedback,
    only: [
      accept_invitation: 1,
      decline_invitation: 2
    ]

  use GenServer

  def child_spec(opts) do
    controller = opts[:controller] || raise "Missing :controller option"
    epoch = opts[:epoch] || raise "Missing :epoch option"

    last_committed_version =
      opts[:last_committed_version] || raise "Missing :last_committed_version option"

    %{
      id: __MODULE__,
      start:
        {GenServer, :start_link,
         [
           __MODULE__,
           {controller, epoch, last_committed_version}
         ]},
      restart: :temporary
    }
  end

  @impl true
  def init({controller, epoch, last_committed_version}) do
    %State{
      controller: controller,
      epoch: epoch,
      last_committed_version: last_committed_version
    }
    |> then(&{:ok, &1})
  end

  @impl true
  def handle_cast({:recruitment_invitation, controller, new_epoch, last_committed_version}, t)
      when new_epoch > t.epoch do
    %{
      t
      | controller: controller,
        epoch: new_epoch,
        last_committed_version: last_committed_version
    }
    |> accept_invitation()
    |> noreply()
  end

  def handle_cast({:recruitment_invitation, controller, _, _, _}, t),
    do: t |> decline_invitation(controller) |> noreply()

  @impl GenServer
  def handle_call(:next_read_version, _from, t),
    do: t |> reply({:ok, t.last_committed_version})

  def handle_call(:next_commit_version, _from, t) do
    next_version = 1 + t.last_committed_version

    %{t | last_committed_version: next_version}
    |> reply({:ok, t.last_committed_version, next_version})
  end

  @impl GenServer
  def handle_info(:die, _t), do: raise("die!")

  defp noreply(t), do: {:noreply, t}
  defp reply(t, result), do: {:reply, result, t}
end
