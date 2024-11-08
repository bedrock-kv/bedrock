defmodule Bedrock.DataPlane.Log.Shale.Server do
  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.Log.Shale.State

  import Bedrock.DataPlane.Log.Shale.Facts, only: [info: 2]
  import Bedrock.DataPlane.Log.Shale.Locking, only: [lock_for_recovery: 3]
  import Bedrock.DataPlane.Log.Shale.Recovery, only: [recover_from: 4]
  import Bedrock.DataPlane.Log.Shale.Pushing, only: [push: 3]
  import Bedrock.DataPlane.Log.Shale.Pulling, only: [pull: 3]

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
    :ets.insert(log, [Log.initial_transaction()])

    {:ok,
     %State{
       cluster: cluster,
       mode: :starting,
       id: id,
       otp_name: otp_name,
       foreman: foreman,
       log: log,
       oldest_version: :start,
       last_version: 0
     }, {:continue, :initialization}}
  end

  @impl true
  def handle_continue(:initialization, t) do
    trace_log_started(t.cluster, t.id, t.otp_name)

    t |> noreply()
  end

  def handle_continue({:check_waiting_pullers, expected_version}, t) do
    {pullers, remaining_waiting_pullers} = Map.pop(t.waiting_pullers, expected_version)

    if pullers == nil do
      t
    else
      pullers
      |> Enum.reduce(
        t,
        fn {_, from, opts}, t ->
          pull(t, expected_version, opts)
          |> case do
            {:ok, t, entries} ->
              GenServer.reply(from, {:ok, entries})
              t

            {:waiting_for, _} ->
              # This should never happen, as we should have already checked
              # for this condition when adding the puller to the waiting list
              raise "Unexpected waiting_for condition"

            {:error, reason} ->
              GenServer.reply(from, {:error, reason})
              t
          end
        end
      )
      |> Map.put(:waiting_pullers, remaining_waiting_pullers)
    end
    |> noreply(continue: :check_for_expired_pullers)
  end

  def handle_continue(:check_for_expired_pullers, t) do
    Map.update!(
      t,
      :waiting_pullers,
      &process_expired_deadlines_for_waiting_pullers(
        &1,
        :erlang.monotonic_time(:millisecond)
      )
    )
    |> noreply(continue: :wait_for_next_puller_deadline)
  end

  def handle_continue(:wait_for_next_puller_deadline, t) do
    now = :erlang.monotonic_time(:millisecond)

    t
    |> Map.get(:waiting_pullers)
    |> determine_timeout_for_next_puller_deadline(now)
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

    with {:ok, t} <- lock_for_recovery(t, epoch, director),
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
      {:waiting, t} -> t |> noreply(continue: :check_for_expired_pullers)
      {:ok, t} -> t |> reply(:ok, continue: {:check_waiting_pullers, expected_version})
      {:error, _reason} = error -> t |> reply(error)
    end
  end

  def handle_call({:pull, from_version, opts}, from, t) do
    trace_log_pull_transactions(t.cluster, t.id, from_version, opts)

    case pull(t, from_version, opts) do
      {:ok, t, transactions} ->
        t |> reply({:ok, transactions})

      {:waiting_for, from_version} ->
        {timeout_in_ms, opts} = opts |> Keyword.pop(:willing_to_wait_in_ms)

        if timeout_in_ms == nil do
          # Not willing to wait timeout, so we reply with an error
          t |> reply({:error, :version_too_new}, continue: :check_for_expired_pullers)
        else
          deadline = :erlang.monotonic_time(:millisecond) + normalize_timeout_to_ms(timeout_in_ms)

          t
          |> Map.update!(:waiting_pullers, fn waiting_pullers ->
            puller = {deadline, from, opts}

            waiting_pullers
            |> Map.update(from_version, [puller], &[puller | &1])
          end)
          |> noreply(continue: :check_for_expired_pullers)
        end

      {:error, _reason} = error ->
        t |> reply(error)
    end
  end

  def normalize_timeout_to_ms(n), do: n |> to_timeout() |> max(10) |> min(10_000)

  @spec process_expired_deadlines_for_waiting_pullers(map(), integer()) :: map()
  def process_expired_deadlines_for_waiting_pullers(waiting_pullers, now) do
    waiting_pullers
    |> Enum.map(fn {version, pullers} ->
      pullers
      |> Enum.reduce([], fn
        {deadline, from, _opts}, acc when deadline <= now ->
          GenServer.reply(from, {:ok, []})
          acc

        puller, acc ->
          [puller | acc]
      end)
      |> case do
        [] -> nil
        pullers -> {version, pullers}
      end
    end)
    |> Enum.reject(&is_nil/1)
    |> Map.new()
  end

  @spec determine_timeout_for_next_puller_deadline(map(), integer()) ::
          pos_integer() | nil
  def determine_timeout_for_next_puller_deadline(waiting_pullers, now) do
    waiting_pullers
    |> Map.values()
    |> Enum.map(&Enum.min/1)
    |> Enum.min(fn -> nil end)
    |> case do
      nil ->
        nil

      {next_deadline, _, _} ->
        max(1, next_deadline - now)
    end
  end

  @spec ack_fn(GenServer.from()) :: (-> :ok)
  def ack_fn(from), do: fn -> GenServer.reply(from, :ok) end
end
