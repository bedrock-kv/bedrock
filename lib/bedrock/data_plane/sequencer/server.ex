defmodule Bedrock.DataPlane.Sequencer.Server do
  alias Bedrock.DataPlane.Sequencer.State

  import Bedrock.DataPlane.Sequencer.ControllerFeedback,
    only: [
      accept_invitation: 1,
      decline_invitation: 2
    ]

  use GenServer

  def child_spec(opts) do
    cluster = opts[:cluster] || raise "Missing :cluster option"
    controller = opts[:controller] || raise "Missing :controller option"
    epoch = opts[:epoch] || raise "Missing :epoch option"
    started_at = opts[:started_at] || raise "Missing :started_at option"
    otp_name = opts[:otp_name] || raise "Missing :otp_name option"

    last_committed_version =
      opts[:last_committed_version] || raise "Missing :last_committed_version option"

    %{
      id: __MODULE__,
      start:
        {GenServer, :start_link,
         [
           __MODULE__,
           {cluster, controller, epoch, started_at, last_committed_version},
           [name: otp_name]
         ]},
      restart: :temporary
    }
  end

  @impl true
  def init({cluster, controller, epoch, last_committed_version}) do
    %State{
      cluster: cluster,
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
