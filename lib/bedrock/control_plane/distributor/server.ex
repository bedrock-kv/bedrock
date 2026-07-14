defmodule Bedrock.ControlPlane.Distributor.Server do
  @moduledoc """
  GenServer implementation of the Distributor singleton.

  Holds the epoch it was recruited under, a reference to the recruiting
  director, and a snapshot of the shard layout. Monitors the director and
  stops (`:normal`) when the director exits or when a newer epoch is
  signaled - the next director recruits a fresh distributor.

  This is the Phase A skeleton: coverage tracking, recruitment, and
  placeholder supervision arrive in later tickets and will all pass
  through the epoch guard implemented here.
  """

  use GenServer

  import Bedrock.ControlPlane.Distributor.Telemetry,
    only: [
      emit_distributor_started: 3,
      emit_distributor_stopped: 3
    ]

  import Bedrock.Internal.GenServer.Replies

  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.ControlPlane.Distributor.State

  require Logger

  @doc false
  @spec child_spec(
          opts :: [
            cluster: module(),
            epoch: Bedrock.epoch(),
            director: pid(),
            shard_layout: TransactionSystemLayout.shard_layout(),
            otp_name: atom()
          ]
        ) :: Supervisor.child_spec()
  def child_spec(opts) do
    cluster = opts[:cluster] || raise "Missing :cluster option"
    epoch = opts[:epoch] || raise "Missing :epoch option"
    director = opts[:director] || raise "Missing :director option"
    otp_name = opts[:otp_name] || raise "Missing :otp_name option"
    shard_layout = opts[:shard_layout] || %{}

    %{
      id: {__MODULE__, cluster, epoch},
      start:
        {GenServer, :start_link,
         [
           __MODULE__,
           {cluster, epoch, director, shard_layout},
           [name: otp_name]
         ]},
      restart: :temporary
    }
  end

  @impl true
  @spec init({module(), Bedrock.epoch(), pid(), TransactionSystemLayout.shard_layout()}) ::
          {:ok, State.t()}
  def init({cluster, epoch, director, shard_layout}) do
    # Monitor the Director - if it dies, this distributor should terminate
    # and let the next director recruit a fresh one.
    Process.monitor(director)

    emit_distributor_started(cluster, epoch, director)

    {:ok,
     %State{
       cluster: cluster,
       epoch: epoch,
       director: director,
       shard_layout: shard_layout
     }}
  end

  @impl true
  def handle_call({:check_epoch, epoch}, _from, %State{} = t) do
    case State.check_epoch(t, epoch) do
      :ok ->
        reply(t, :ok)

      {:error, :epoch_superseded} = error ->
        Logger.info("Distributor for epoch #{t.epoch} superseded by epoch #{epoch}; stopping")
        {:stop, :normal, error, t}

      {:error, :newer_epoch_exists} = error ->
        reply(t, error)
    end
  end

  @impl true
  def handle_cast({:epoch_changed, new_epoch}, %State{} = t) do
    case State.check_epoch(t, new_epoch) do
      {:error, :epoch_superseded} ->
        Logger.info("Distributor for epoch #{t.epoch} superseded by epoch #{new_epoch}; stopping")
        stop(t, :normal)

      _same_or_older ->
        noreply(t)
    end
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, director, reason}, %State{director: director} = t) do
    Logger.info("Distributor for epoch #{t.epoch} stopping: director exited (#{inspect(reason)})")

    stop(t, :normal)
  end

  def handle_info(message, %State{} = t) do
    Logger.debug("Distributor ignoring unexpected message: #{inspect(message)}")
    noreply(t)
  end

  @impl true
  def terminate(reason, %State{} = t) do
    emit_distributor_stopped(t.cluster, t.epoch, reason)
    :ok
  end
end
