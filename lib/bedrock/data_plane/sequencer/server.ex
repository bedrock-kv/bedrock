defmodule Bedrock.DataPlane.Sequencer.Server do
  alias Bedrock.DataPlane.Sequencer.State

  use GenServer

  import Bedrock.Internal.GenServer.Replies

  @doc false
  def child_spec(opts) do
    director = opts[:director] || raise "Missing :director option"
    epoch = opts[:epoch] || raise "Missing :epoch option"
    otp_name = opts[:otp_name] || raise "Missing :name option"

    last_committed_version =
      opts[:last_committed_version] || raise "Missing :last_committed_version option"

    %{
      id: __MODULE__,
      start:
        {GenServer, :start_link,
         [
           __MODULE__,
           {director, epoch, last_committed_version},
           [name: otp_name]
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
  def handle_call(:next_read_version, _from, t),
    do: t |> reply({:ok, t.last_committed_version})

  def handle_call(:next_commit_version, _from, t) do
    next_version = 1 + t.last_committed_version

    %{t | last_committed_version: next_version}
    |> reply({:ok, t.last_committed_version, next_version})
  end
end
