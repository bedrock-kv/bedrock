defmodule Bedrock.DataPlane.Resolver.Server do
  @moduledoc """
  GenServer implementation for the Resolver conflict detection engine.

  Manages resolver state including version ordering through waiting queues.
  Handles out-of-order transaction resolution by queuing later versions until
  earlier ones complete.

  Starts in running mode and is immediately ready to process transaction
  resolution requests.
  """
  use GenServer

  import Bedrock.DataPlane.Resolver.ConflictResolution, only: [resolve: 3, remove_old_transactions: 2]

  import Bedrock.DataPlane.Resolver.Telemetry,
    only: [
      emit_received: 4,
      emit_processing: 3,
      emit_completed: 5,
      emit_reply_sent: 4,
      emit_waiting_list: 4,
      emit_waiting_list_inserted: 5,
      emit_waiting_resolved: 4,
      emit_validation_error: 2,
      emit_waiting_list_validation_error: 2
    ]

  import Bedrock.Internal.GenServer.Replies

  alias Bedrock.DataPlane.Resolver.State
  alias Bedrock.DataPlane.Resolver.Tree
  alias Bedrock.DataPlane.Resolver.Validation
  alias Bedrock.DataPlane.Version
  alias Bedrock.Internal.Time
  alias Bedrock.Internal.WaitingList

  @type reply_fn :: (result :: {:ok, [non_neg_integer()]} | {:error, any()} -> :ok)

  @default_waiting_timeout_ms 30_000

  @spec child_spec(
          opts :: [
            lock_token: Bedrock.lock_token(),
            key_range: Bedrock.key_range(),
            epoch: Bedrock.epoch(),
            last_version: Bedrock.version(),
            director: pid(),
            cluster: module(),
            sweep_interval_ms: pos_integer(),
            version_retention_ms: pos_integer()
          ]
        ) :: Supervisor.child_spec()
  def child_spec(opts) do
    lock_token = opts[:lock_token] || raise "Missing :lock_token option"
    key_range = opts[:key_range] || raise "Missing :key_range option"
    epoch = opts[:epoch] || raise "Missing :epoch option"
    last_version = opts[:last_version] || Version.zero()
    director = opts[:director] || raise "Missing :director option"
    cluster = opts[:cluster] || raise "Missing :cluster option"
    sweep_interval_ms = opts[:sweep_interval_ms] || 1_000
    version_retention_ms = opts[:version_retention_ms] || 6_000

    %{
      id: {__MODULE__, cluster, key_range, epoch},
      start:
        {GenServer, :start_link,
         [
           __MODULE__,
           {lock_token, last_version, epoch, director, sweep_interval_ms, version_retention_ms}
         ]},
      restart: :temporary
    }
  end

  @impl true
  def init({lock_token, last_version, epoch, director, sweep_interval_ms, version_retention_ms}) do
    # Monitor the Director - if it dies, this resolver should terminate
    Process.monitor(director)

    then(
      %State{
        lock_token: lock_token,
        tree: %Tree{},
        oldest_version: last_version,
        last_version: last_version,
        waiting: %{},
        mode: :running,
        epoch: epoch,
        director: director,
        sweep_interval_ms: sweep_interval_ms,
        version_retention_ms: version_retention_ms,
        last_sweep_time: Time.monotonic_now_in_ms()
      },
      &{:ok, &1}
    )
  end

  @impl true
  def terminate(_reason, _state) do
    :ok
  end

  @impl true
  def handle_call({:resolve_transactions, epoch, {_last_version, _next_version}, _transactions}, _from, t)
      when epoch != t.epoch do
    reply(t, {:error, {:epoch_mismatch, expected: t.epoch, received: epoch}})
  end

  @impl true
  def handle_call({:resolve_transactions, epoch, {last_version, next_version}, transactions}, from, t)
      when t.mode == :running and epoch == t.epoch and last_version == t.last_version do
    emit_received(length(transactions), last_version, next_version, t.last_version)

    transactions
    |> Validation.check_transactions()
    |> case do
      :ok ->
        noreply(t, continue: {:process_ready, {next_version, transactions, reply_fn(from)}})

      {:error, reason} ->
        emit_validation_error(length(transactions), reason)
        reply(t, {:error, reason}, continue: :next_timeout)
    end
  end

  @impl true
  def handle_call({:resolve_transactions, epoch, {last_version, next_version}, transactions}, from, t)
      when t.mode == :running and epoch == t.epoch and is_binary(last_version) and last_version > t.last_version do
    emit_waiting_list(length(transactions), last_version, next_version, t.last_version)

    transactions
    |> Validation.check_transactions()
    |> case do
      :ok ->
        data = {next_version, transactions}

        {new_waiting, _timeout} =
          WaitingList.insert(
            t.waiting,
            last_version,
            data,
            reply_fn(from),
            @default_waiting_timeout_ms
          )

        emit_waiting_list_inserted(
          length(transactions),
          map_size(new_waiting),
          last_version,
          next_version,
          t.last_version
        )

        noreply(%{t | waiting: new_waiting}, continue: :next_timeout)

      {:error, reason} ->
        emit_waiting_list_validation_error(length(transactions), reason)
        reply(t, {:error, reason}, continue: :next_timeout)
    end
  end

  @impl true
  def handle_call({:resolve_transactions, epoch, {last_version, _next_version}, transactions}, _from, t)
      when t.mode == :running and epoch == t.epoch and is_binary(last_version) and last_version < t.last_version do
    aborted_indices = Enum.to_list(0..(length(transactions) - 1))
    reply(t, {:ok, aborted_indices})
  end

  @impl true
  def handle_info(:timeout, t) do
    {new_waiting, expired_entries} = WaitingList.expire(t.waiting)

    Enum.each(expired_entries, fn {_deadline, reply_fn, _data} ->
      reply_fn.({:error, :waiting_timeout})
    end)

    noreply(%{t | waiting: new_waiting}, continue: :next_timeout)
  end

  def handle_info({:DOWN, _ref, :process, director_pid, _reason}, %{director: director_pid} = t) do
    # Director has died - this resolver should terminate gracefully
    {:stop, :normal, t}
  end

  def handle_info(_msg, t) do
    {:noreply, t}
  end

  @impl true
  def handle_continue({:process_ready, {next_version, transactions, reply_fn}}, t) do
    emit_processing(length(transactions), t.last_version, next_version)

    {new_waiting, _} = WaitingList.remove(t.waiting, t.last_version)

    {tree, aborted} = resolve(t.tree, transactions, next_version)
    t = %{t | tree: tree, last_version: next_version, waiting: new_waiting}

    emit_completed(
      length(transactions),
      length(aborted),
      t.last_version,
      next_version,
      t.last_version
    )

    reply_fn.({:ok, aborted})

    emit_reply_sent(length(transactions), length(aborted), t.last_version, next_version)

    case WaitingList.find(t.waiting, next_version) do
      nil ->
        noreply(t, continue: :next_timeout)

      {_deadline, reply_fn, {waiting_next_version, transactions}} ->
        emit_waiting_resolved(length(transactions), 0, waiting_next_version, t.last_version)
        noreply(t, continue: {:process_ready, {waiting_next_version, transactions, reply_fn}})
    end
  end

  @impl true
  def handle_continue(:next_timeout, t) do
    timeout = WaitingList.next_timeout(t.waiting)
    time_since_last_sweep = Time.elapsed_monotonic_in_ms(t.last_sweep_time)
    should_sweep = timeout == :infinity || time_since_last_sweep >= t.sweep_interval_ms

    if should_sweep do
      retention_microseconds = t.version_retention_ms * 1000
      current_version_int = Version.to_integer(t.last_version)

      updated_state =
        if current_version_int >= retention_microseconds do
          new_tree = remove_old_transactions(t.tree, Version.subtract(t.last_version, retention_microseconds))
          %{t | tree: new_tree, last_sweep_time: Time.monotonic_now_in_ms()}
        else
          %{t | last_sweep_time: Time.monotonic_now_in_ms()}
        end

      noreply(updated_state, timeout: min(timeout, t.sweep_interval_ms))
    else
      time_until_next_sweep = t.sweep_interval_ms - time_since_last_sweep
      noreply(t, timeout: min(timeout, time_until_next_sweep))
    end
  end

  @spec reply_fn(GenServer.from()) :: reply_fn()
  defp reply_fn(from), do: &GenServer.reply(from, &1)
end
