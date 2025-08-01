defmodule Bedrock.DataPlane.Sequencer.Server do
  @moduledoc """
  GenServer implementation for the Sequencer version authority.

  Manages Lamport clock state for version assignment, tracking the last committed
  version and next commit version. Implements the core Lamport clock semantics
  where each commit version assignment updates the internal clock state to maintain
  proper causality relationships for distributed MVCC conflict detection.
  """

  alias Bedrock.DataPlane.Sequencer.State

  use GenServer

  import Bedrock.Internal.GenServer.Replies

  @doc false
  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    director = opts[:director] || raise "Missing :director option"
    epoch = opts[:epoch] || raise "Missing :epoch option"
    otp_name = opts[:otp_name] || raise "Missing :name option"

    known_committed_version =
      opts[:last_committed_version] || raise "Missing :last_committed_version option"

    %{
      id: __MODULE__,
      start:
        {GenServer, :start_link,
         [
           __MODULE__,
           {director, epoch, known_committed_version},
           [name: otp_name]
         ]},
      restart: :temporary
    }
  end

  @impl true
  @spec init({pid(), Bedrock.epoch(), Bedrock.version()}) :: {:ok, State.t()}
  def init({director, epoch, known_committed_version}) do
    %State{
      director: director,
      epoch: epoch,
      next_commit_version: known_committed_version + 1,
      last_commit_version: known_committed_version,
      known_committed_version: known_committed_version
    }
    |> then(&{:ok, &1})
  end

  @impl true
  @spec handle_call(:next_read_version | :next_commit_version, GenServer.from(), State.t()) ::
          {:reply, {:ok, Bedrock.version()} | {:ok, Bedrock.version(), Bedrock.version()},
           State.t()}
  def handle_call(:next_read_version, _from, t),
    do: t |> reply({:ok, t.known_committed_version})

  @impl true
  def handle_call(:next_commit_version, _from, t) do
    commit_version = t.next_commit_version

    %{t | next_commit_version: commit_version + 1, last_commit_version: commit_version}
    |> reply({:ok, t.last_commit_version, commit_version})
  end

  @impl true
  @spec handle_cast({:report_successful_commit, Bedrock.version()}, State.t()) ::
          {:noreply, State.t()}
  def handle_cast({:report_successful_commit, commit_version}, t) do
    updated_known_committed_version = max(t.known_committed_version, commit_version)

    %{t | known_committed_version: updated_known_committed_version}
    |> noreply([])
  end
end
