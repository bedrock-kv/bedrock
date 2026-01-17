defmodule Bedrock.DataPlane.Sequencer.Server do
  @moduledoc """
  GenServer implementation for the Sequencer version authority.

  Manages Lamport clock state for version assignment, tracking the last committed
  version and next commit version. Implements the core Lamport clock semantics
  where each commit version assignment updates the internal clock state to maintain
  proper causality relationships for distributed MVCC conflict detection.
  """

  use GenServer

  import Bedrock.DataPlane.Sequencer.Telemetry,
    only: [
      emit_next_read_version: 1,
      emit_next_commit_version: 3,
      emit_successful_commit: 2
    ]

  import Bedrock.Internal.GenServer.Replies

  alias Bedrock.DataPlane.Sequencer.State
  alias Bedrock.DataPlane.Version

  @doc false
  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    cluster = opts[:cluster] || raise "Missing :cluster option"
    director = opts[:director] || raise "Missing :director option"
    epoch = opts[:epoch] || raise "Missing :epoch option"
    otp_name = opts[:otp_name] || raise "Missing :name option"

    known_committed_version =
      opts[:last_committed_version] || raise "Missing :last_committed_version option"

    %{
      id: {__MODULE__, cluster, epoch},
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
    # Monitor the Director - if it dies, this sequencer should terminate
    Process.monitor(director)

    epoch_start_monotonic_us = System.monotonic_time(:microsecond)
    epoch_baseline_version_int = Version.to_integer(known_committed_version)

    then(
      %State{
        director: director,
        epoch: epoch,
        next_commit_version_int: epoch_baseline_version_int + 1,
        last_commit_version_int: epoch_baseline_version_int,
        known_committed_version_int: epoch_baseline_version_int,
        epoch_baseline_version_int: epoch_baseline_version_int,
        epoch_start_monotonic_us: epoch_start_monotonic_us
      },
      &{:ok, &1}
    )
  end

  @impl true
  @spec handle_call(
          {:next_read_version, Bedrock.epoch()}
          | {:next_commit_version, Bedrock.epoch()}
          | {:report_successful_commit, Bedrock.epoch(), Bedrock.version()},
          GenServer.from(),
          State.t()
        ) ::
          {:reply,
           {:ok, Bedrock.version()} | {:ok, Bedrock.version(), Bedrock.version()} | :ok | {:error, :wrong_epoch},
           State.t()}
  def handle_call({:next_read_version, epoch}, _from, %{epoch: epoch} = t) do
    read_version = Version.from_integer(t.known_committed_version_int)
    emit_next_read_version(read_version)
    reply(t, {:ok, read_version})
  end

  def handle_call({:next_read_version, _wrong_epoch}, _from, t) do
    reply(t, {:error, :wrong_epoch})
  end

  def handle_call({:next_commit_version, epoch}, _from, %{epoch: epoch} = t) do
    current_monotonic_us = System.monotonic_time(:microsecond)
    elapsed_us = current_monotonic_us - t.epoch_start_monotonic_us
    proposed_version_int = t.epoch_baseline_version_int + elapsed_us
    commit_version_int = max(proposed_version_int, t.next_commit_version_int)
    next_commit_version_int = commit_version_int + 1

    updated_state = %{
      t
      | next_commit_version_int: next_commit_version_int,
        last_commit_version_int: commit_version_int
    }

    last_commit_version = Version.from_integer(t.last_commit_version_int)
    commit_version = Version.from_integer(commit_version_int)

    emit_next_commit_version(last_commit_version, commit_version, elapsed_us)
    reply(updated_state, {:ok, last_commit_version, commit_version})
  end

  def handle_call({:next_commit_version, _wrong_epoch}, _from, t) do
    reply(t, {:error, :wrong_epoch})
  end

  def handle_call({:report_successful_commit, epoch, commit_version}, _from, %{epoch: epoch} = t) do
    commit_version_int = Version.to_integer(commit_version)
    updated_known_committed_int = max(t.known_committed_version_int, commit_version_int)

    known_committed_version = Version.from_integer(updated_known_committed_int)
    emit_successful_commit(commit_version, known_committed_version)

    reply(%{t | known_committed_version_int: updated_known_committed_int}, :ok)
  end

  def handle_call({:report_successful_commit, _wrong_epoch, _commit_version}, _from, t) do
    reply(t, {:error, :wrong_epoch})
  end

  @impl true
  @spec handle_info({:DOWN, reference(), :process, pid(), term()}, State.t()) :: {:stop, :normal, State.t()}
  def handle_info({:DOWN, _ref, :process, director_pid, _reason}, %{director: director_pid} = t) do
    # Director has died - this sequencer should terminate gracefully
    {:stop, :normal, t}
  end

  def handle_info(_msg, t) do
    {:noreply, t}
  end
end
