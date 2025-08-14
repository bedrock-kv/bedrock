defmodule Bedrock.DataPlane.Log.Shale.Server do
  @moduledoc false
  alias Bedrock.Cluster
  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.EncodedTransaction
  alias Bedrock.DataPlane.Log.Shale.Segment
  alias Bedrock.DataPlane.Log.Shale.SegmentRecycler
  alias Bedrock.DataPlane.Log.Shale.State
  alias Bedrock.DataPlane.Version

  import Bedrock.DataPlane.Log.Shale.ColdStarting, only: [reload_segments_at_path: 1]
  import Bedrock.DataPlane.Log.Shale.Facts, only: [info: 2]
  import Bedrock.DataPlane.Log.Shale.Locking, only: [lock_for_recovery: 3]
  import Bedrock.DataPlane.Log.Shale.Recovery, only: [recover_from: 4]
  import Bedrock.DataPlane.Log.Shale.Pushing, only: [push: 4]
  import Bedrock.DataPlane.Log.Shale.Pulling, only: [pull: 3]

  import Bedrock.DataPlane.Log.Shale.LongPulls,
    only: [
      process_expired_deadlines_for_waiting_pullers: 2,
      try_to_add_to_waiting_pullers: 5,
      determine_timeout_for_next_puller_deadline: 2,
      notify_waiting_pullers: 3
    ]

  import Bedrock.DataPlane.Log.Telemetry

  use GenServer
  import Bedrock.Internal.GenServer.Replies

  @doc false
  @spec child_spec(
          opts :: [
            cluster: Cluster.t(),
            otp_name: atom(),
            id: Log.id(),
            foreman: pid(),
            path: Path.t()
          ]
        ) :: Supervisor.child_spec()
  def child_spec(opts) do
    cluster = opts[:cluster] || raise "Missing :cluster option"
    otp_name = opts[:otp_name] || raise "Missing :otp_name option"
    id = Keyword.fetch!(opts, :id) || raise "Missing :id option"
    foreman = Keyword.fetch!(opts, :foreman)
    path = Keyword.fetch!(opts, :path)

    %{
      id: {__MODULE__, id},
      start:
        {GenServer, :start_link,
         [
           __MODULE__,
           {
             cluster,
             otp_name,
             id,
             foreman,
             path
           },
           [name: otp_name]
         ]}
    }
  end

  @impl true
  @spec init({module(), atom(), Bedrock.DataPlane.Log.id(), pid(), Path.t()}) ::
          {:ok, Bedrock.DataPlane.Log.Shale.State.t(), {:continue, :initialization}}
  def init({cluster, otp_name, id, foreman, path}) do
    {:ok,
     %State{
       path: path,
       cluster: cluster,
       mode: :locked,
       id: id,
       otp_name: otp_name,
       foreman: foreman,
       oldest_version: Version.zero(),
       last_version: Version.zero()
     }, {:continue, :initialization}}
  end

  @impl true
  @spec handle_continue(
          :initialization
          | {:notify_waiting_pullers, Bedrock.version(), EncodedTransaction.t()}
          | :check_for_expired_pullers
          | :wait_for_next_puller_deadline,
          State.t()
        ) ::
          {:noreply, State.t()} | {:noreply, State.t(), timeout()}
  def handle_continue(:initialization, t) do
    trace_metadata(%{cluster: t.cluster, id: t.id, otp_name: t.otp_name})
    trace_started()

    {:ok, pid} =
      SegmentRecycler.start_link(
        path: t.path,
        min_available: 2,
        max_available: 3,
        segment_size: 1 * 1024 * 1024
      )

    {oldest_version, last_version, active_segment, segments} =
      t.path
      |> reload_segments_at_path
      |> case do
        {:error, :unable_to_list_segments} ->
          {Version.zero(), Version.zero(), nil, []}

        {:ok, []} ->
          {Version.zero(), Version.zero(), nil, []}

        {:ok, [active_segment | segments]} ->
          active_segment = Segment.ensure_transactions_are_loaded(active_segment)
          last_version = Segment.last_version(active_segment)

          oldest_version =
            [active_segment.min_version | Enum.map(segments, & &1.min_version)]
            |> Enum.min()

          {oldest_version, last_version, active_segment, segments}
      end

    t
    |> Map.put(:oldest_version, oldest_version)
    |> Map.put(:last_version, last_version)
    |> Map.put(:active_segment, active_segment)
    |> Map.put(:segments, segments)
    |> Map.put(:segment_recycler, pid)
    |> noreply()
  end

  @impl true
  def handle_continue({:notify_waiting_pullers, version, transaction}, t) do
    t
    |> Map.update!(
      :waiting_pullers,
      &notify_waiting_pullers(&1, version, transaction)
    )
    |> noreply(continue: :check_for_expired_pullers)
  end

  @impl true
  def handle_continue(:check_for_expired_pullers, t) do
    t
    |> Map.update!(
      :waiting_pullers,
      &process_expired_deadlines_for_waiting_pullers(&1, monotonic_now())
    )
    |> noreply(continue: :wait_for_next_puller_deadline)
  end

  @impl true
  def handle_continue(:wait_for_next_puller_deadline, t) do
    t
    |> Map.get(:waiting_pullers)
    |> determine_timeout_for_next_puller_deadline(monotonic_now())
    |> case do
      nil -> t |> noreply()
      timeout -> {:noreply, t, timeout}
    end
  end

  @impl true
  @spec handle_info(:timeout, State.t()) ::
          {:noreply, State.t(), {:continue, :check_for_expired_pullers}}
  def handle_info(:timeout, t), do: t |> noreply(continue: :check_for_expired_pullers)

  @impl true
  @spec handle_call(
          {:info, [atom()]}
          | {:lock_for_recovery, Bedrock.epoch()}
          | {:recover_from, pid(), Bedrock.version(), Bedrock.version()}
          | {:push, binary(), Bedrock.version()}
          | {:pull, Bedrock.version(), keyword()}
          | :ping,
          GenServer.from(),
          State.t()
        ) ::
          {:reply, term(), State.t()} | {:noreply, State.t(), {:continue, atom()}}
  def handle_call({:info, fact_names}, _, t),
    do: info(t, fact_names) |> then(&(t |> reply(&1)))

  @impl true
  def handle_call({:lock_for_recovery, epoch}, {director, _}, t) do
    trace_lock_for_recovery(epoch)

    with {:ok, t} <- lock_for_recovery(t, epoch, director),
         {:ok, info} <- info(t, Log.recovery_info()) do
      t |> reply({:ok, self(), info})
    else
      error -> t |> reply(error)
    end
  end

  @impl true
  def handle_call({:recover_from, source_log, first_version, last_version}, {_director, _}, t) do
    trace_recover_from(source_log, first_version, last_version)

    case recover_from(t, source_log, first_version, last_version) do
      {:ok, t} -> t |> reply({:ok, self()})
      {:error, reason} -> t |> reply({:error, {:failed_to_recover, reason}})
    end
  end

  @impl true
  def handle_call({:push, transaction_bytes, expected_version}, from, %State{} = t) do
    with {:ok, transaction} <- EncodedTransaction.validate(transaction_bytes),
         {:ok, t} <- push(t, expected_version, transaction, ack_fn(from)) do
      t |> noreply(continue: {:notify_waiting_pullers, expected_version, transaction})
    else
      {:wait, t} -> t |> noreply(continue: :check_for_expired_pullers)
      {:error, _reason} = error -> t |> reply(error, continue: :check_for_expired_pullers)
    end
  end

  @impl true
  def handle_call({:pull, from_version, opts}, from, t) do
    trace_pull_transactions(from_version, opts)

    case pull(t, from_version, opts) do
      {:ok, t, transactions} ->
        t |> reply({:ok, transactions})

      {:waiting_for, from_version} ->
        try_to_add_to_waiting_pullers(
          t.waiting_pullers,
          monotonic_now(),
          reply_to_fn(from),
          from_version,
          opts
        )
        |> case do
          {:error, _reason} = error ->
            t |> reply(error, continue: :check_for_expired_pullers)

          {:ok, waiting_pullers} ->
            t
            |> Map.put(:waiting_pullers, waiting_pullers)
            |> noreply(continue: :check_for_expired_pullers)
        end

      {:error, _reason} = error ->
        t |> reply(error)
    end
  end

  @impl true
  def handle_call(:ping, _from, t), do: t |> reply(:pong)

  @spec check_running(term()) :: {:error, :unavailable}
  def check_running(_t), do: {:error, :unavailable}

  @spec ack_fn(GenServer.from()) :: (:ok | {:error, term()} -> :ok)
  def ack_fn(from), do: fn result -> GenServer.reply(from, result) end

  @spec reply_to_fn(GenServer.from()) :: (term() -> :ok)
  def reply_to_fn(from), do: &GenServer.reply(from, &1)

  @spec monotonic_now() :: integer()
  def monotonic_now, do: :erlang.monotonic_time(:millisecond)
end
