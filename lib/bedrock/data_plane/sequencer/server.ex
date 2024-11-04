defmodule Bedrock.DataPlane.Sequencer.Server do
  alias Bedrock.DataPlane.Sequencer.State

  import Bedrock.DataPlane.Sequencer.DirectorFeedback,
    only: [
      accept_invitation: 1,
      decline_invitation: 2
    ]

  use GenServer
  import Bedrock.Internal.GenServer.Replies

  def child_spec(opts) do
    director = opts[:director] || raise "Missing :director option"
    epoch = opts[:epoch] || raise "Missing :epoch option"

    last_committed_version =
      opts[:last_committed_version] || raise "Missing :last_committed_version option"

    %{
      id: __MODULE__,
      start:
        {GenServer, :start_link,
         [
           __MODULE__,
           {director, epoch, last_committed_version}
         ]},
      restart: :temporary
    }
  end

  @impl true
  def init({director, epoch, last_committed_version}) do
    %State{
      director: director,
      epoch: epoch,
      last_committed_version: last_committed_version
    }
    |> then(&{:ok, &1})
  end

  @impl true
  def handle_cast({:recruitment_invitation, director, new_epoch, last_committed_version}, t)
      when new_epoch > t.epoch do
    %{
      t
      | director: director,
        epoch: new_epoch,
        last_committed_version: last_committed_version
    }
    |> accept_invitation()
    |> noreply()
  end

  def handle_cast({:recruitment_invitation, director, _, _, _}, t),
    do: t |> decline_invitation(director) |> noreply()

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
end
