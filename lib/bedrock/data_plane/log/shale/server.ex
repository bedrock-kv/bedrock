defmodule Bedrock.DataPlane.Log.Shale.Server do
  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.Log.Shale.State

  import Bedrock.DataPlane.Log.Shale.Facts, only: [info: 2]
  import Bedrock.DataPlane.Log.Shale.Locking, only: [lock_for_recovery: 3]
  import Bedrock.DataPlane.Log.Shale.Recovery, only: [recover_from: 4]
  import Bedrock.DataPlane.Log.Shale.Pushing, only: [push: 3]
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
  @spec child_spec(opts :: keyword() | []) :: Supervisor.child_spec()
  def child_spec(opts) do
    cluster = opts[:cluster] || raise "Missing :cluster option"
    otp_name = opts[:otp_name] || raise "Missing :otp_name option"
    id = Keyword.fetch!(opts, :id) || raise "Missing :id option"
    foreman = Keyword.fetch!(opts, :foreman)

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
             foreman
           },
           [name: otp_name]
         ]}
    }
  end

  @impl true
  def init({cluster, otp_name, id, foreman}) do
    log = :ets.new(:log, [:protected, :ordered_set])
    true = :ets.insert_new(log, Log.initial_transaction())

    {:ok,
     %State{
       cluster: cluster,
       mode: :locked,
       id: id,
       otp_name: otp_name,
       foreman: foreman,
       log: log,
       oldest_version: 0,
       last_version: 0
     }, {:continue, :initialization}}
  end

  @impl true
  def handle_continue(:initialization, t) do
    trace_log_started(t.cluster, t.id, t.otp_name)

    t |> noreply()
  end

  def handle_continue({:notify_waiting_pullers, version, transaction}, t) do
    t
    |> Map.update!(
      :waiting_pullers,
      &notify_waiting_pullers(&1, version, transaction)
    )
    |> noreply(continue: :check_for_expired_pullers)
  end

  def handle_continue(:check_for_expired_pullers, t) do
    t
    |> Map.update!(
      :waiting_pullers,
      &process_expired_deadlines_for_waiting_pullers(&1, monotonic_now())
    )
    |> noreply(continue: :wait_for_next_puller_deadline)
  end

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
  def handle_info(:timeout, t), do: t |> noreply(continue: :check_for_expired_pullers)

  @impl true
  def handle_call({:info, fact_names}, _, t),
    do: info(t, fact_names) |> then(&(t |> reply(&1)))

  def handle_call({:lock_for_recovery, epoch}, {director, _}, t) do
    trace_log_lock_for_recovery(t.cluster, t.id, epoch)

    with :ok <- check_mode_for_locking(t, director),
         {:ok, t} <- lock_for_recovery(t, epoch, director),
         {:ok, info} <- info(t, Log.recovery_info()) do
      t |> reply({:ok, self(), info})
    else
      error -> t |> reply(error)
    end
  end

  def handle_call({:recover_from, source_log, first_version, last_version}, {_director, _}, t) do
    trace_log_recover_from(t.cluster, t.id, source_log, first_version, last_version)

    case recover_from(t, source_log, first_version, last_version) do
      {:ok, t} -> t |> reply(:ok)
      {:error, reason} -> t |> reply({:error, {:failed_to_recover, reason}})
    end
  end

  def handle_call({:push, transaction, expected_version}, from, t) do
    trace_log_push_transaction(t.cluster, t.id, transaction, expected_version)

    case push(t, expected_version, {transaction, ack_fn(from)}) do
      {:waiting, t} ->
        t |> noreply(continue: :check_for_expired_pullers)

      {:ok, t} ->
        t |> reply(:ok, continue: {:notify_waiting_pullers, expected_version, transaction})

      {:error, _reason} = error ->
        t |> reply(error)
    end
  end

  def handle_call({:pull, from_version, opts}, from, t) do
    trace_log_pull_transactions(t.cluster, t.id, from_version, opts)

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

  def check_mode_for_locking(t, director) do
    case {t.mode, t.director} do
      {:locked, ^director} -> :ok
      {:locked, nil} -> :ok
      {:locked, _} -> {:error, :locked}
      _ -> :ok
    end
  end

  def check_running(_t), do: {:error, :unavailable}

  @spec ack_fn(GenServer.from()) :: (-> :ok)
  def ack_fn(from), do: fn -> GenServer.reply(from, :ok) end

  @spec reply_to_fn(GenServer.from()) :: (any() -> :ok)
  def reply_to_fn(from), do: &GenServer.reply(from, &1)

  def monotonic_now, do: :erlang.monotonic_time(:millisecond)
end
