defmodule Bedrock.DataPlane.Storage.Olivine.Server do
  @moduledoc false
  use GenServer

  import Bedrock.Internal.GenServer.Replies

  alias Bedrock.DataPlane.Storage
  alias Bedrock.DataPlane.Storage.Olivine.Logic
  alias Bedrock.DataPlane.Storage.Olivine.State
  alias Bedrock.DataPlane.Storage.Telemetry
  alias Bedrock.Service.Foreman

  # Process transactions up to this size (in bytes) per window advancement
  # 2MB
  @max_batch_size_bytes 2 * 1024 * 1024

  @spec child_spec(opts :: keyword()) :: map()
  def child_spec(opts) do
    otp_name = opts[:otp_name] || raise "Missing :otp_name option"
    foreman = opts[:foreman] || raise "Missing :foreman option"
    id = opts[:id] || raise "Missing :id option"
    path = opts[:path] || raise "Missing :path option"

    %{
      id: {__MODULE__, id},
      start:
        {GenServer, :start_link,
         [
           __MODULE__,
           {otp_name, foreman, id, path},
           [name: otp_name]
         ]}
    }
  end

  @impl true
  def init(args), do: {:ok, args, {:continue, :finish_startup}}

  @impl true

  def handle_call({:get, key, version, opts}, from, %State{} = t) do
    start_time = System.monotonic_time(:microsecond)
    Telemetry.trace_read_request_start(t.otp_name, :get, key)
    fetch_opts = Keyword.put(opts, :reply_fn, reply_fn_for(from))

    case Logic.get(t, key, version, fetch_opts) do
      {:ok, task_pid} ->
        Telemetry.trace_read_task_spawned(t.otp_name, :get, key)

        t
        |> State.add_active_task(task_pid)
        |> noreply_with_transaction_processing()

      {:error, :version_too_new} ->
        if wait_ms = opts[:wait_ms] do
          Telemetry.trace_read_request_waitlisted(t.otp_name, :get, key)

          t
          |> Logic.add_to_waitlist({key, version}, version, reply_fn_for(from), wait_ms)
          |> noreply_with_transaction_processing()
        else
          duration = System.monotonic_time(:microsecond) - start_time
          Telemetry.trace_read_request_complete(t.otp_name, :get, key, duration)
          reply(t, {:error, :version_too_new})
        end

      {:error, _reason} = error ->
        duration = System.monotonic_time(:microsecond) - start_time
        Telemetry.trace_read_request_complete(t.otp_name, :get, key, duration)
        reply(t, error)
    end
  end

  def handle_call({:get_range, start_key, end_key, version, opts}, from, %State{} = t) do
    start_time = System.monotonic_time(:microsecond)
    Telemetry.trace_read_request_start(t.otp_name, :get_range, {start_key, end_key})
    fetch_opts = Keyword.put(opts, :reply_fn, reply_fn_for(from))

    case Logic.get_range(t, start_key, end_key, version, fetch_opts) do
      {:ok, task_pid} ->
        Telemetry.trace_read_task_spawned(t.otp_name, :get_range, {start_key, end_key})

        t
        |> State.add_active_task(task_pid)
        |> noreply_with_transaction_processing()

      {:error, :version_too_new} ->
        if wait_ms = opts[:wait_ms] do
          Telemetry.trace_read_request_waitlisted(t.otp_name, :get_range, {start_key, end_key})

          t
          |> Logic.add_to_waitlist({start_key, end_key, version}, version, reply_fn_for(from), wait_ms)
          |> noreply_with_transaction_processing()
        else
          duration = System.monotonic_time(:microsecond) - start_time
          Telemetry.trace_read_request_complete(t.otp_name, :get_range, {start_key, end_key}, duration)
          reply(t, {:error, :version_too_new})
        end

      {:error, _reason} = error ->
        duration = System.monotonic_time(:microsecond) - start_time
        Telemetry.trace_read_request_complete(t.otp_name, :get_range, {start_key, end_key}, duration)
        reply(t, error)
    end
  end

  @impl true
  def handle_call({:info, fact_names}, _from, %State{} = t), do: t |> Logic.info(fact_names) |> then(&reply(t, &1))

  @impl true
  def handle_call({:lock_for_recovery, epoch}, {director, _}, t) do
    with {:ok, t} <- Logic.lock_for_recovery(t, director, epoch),
         {:ok, info} <- Logic.info(t, Storage.recovery_info()) do
      reply(t, {:ok, self(), info})
    else
      error -> reply(t, error)
    end
  end

  @impl true
  def handle_call({:unlock_after_recovery, durable_version, transaction_system_layout}, {_director, _}, t) do
    case Logic.unlock_after_recovery(t, durable_version, transaction_system_layout) do
      {:ok, updated_state} -> reply(updated_state, :ok)
    end
  end

  @impl true
  def handle_call(_, _from, t), do: reply(t, {:error, :not_ready})

  @impl true
  def handle_continue(:finish_startup, {otp_name, foreman, id, path}) do
    Telemetry.trace_startup_start(otp_name)

    case Logic.startup(otp_name, foreman, id, path) do
      {:ok, state} ->
        Telemetry.trace_startup_complete(otp_name)
        noreply(state, continue: :report_health_to_foreman)

      {:error, reason} ->
        Telemetry.trace_startup_failed(otp_name, reason)
        stop(:no_state, reason)
    end
  end

  @impl true
  def handle_continue(:report_health_to_foreman, %State{} = t) do
    :ok = Foreman.report_health(t.foreman, t.id, {:ok, self()})
    noreply_with_transaction_processing(t)
  end

  defp maybe_advance_window_and_notify(state, version) do
    {:ok, final_state} = Logic.advance_window_with_size_control(state)
    Logic.notify_waiting_fetches(final_state, version)
  end

  # Helper to resume transaction processing if queue has work
  defp noreply_with_transaction_processing(state) do
    if State.queue_empty?(state) do
      noreply(state)
    else
      Telemetry.trace_transaction_timeout_scheduled(state.otp_name)
      noreply(state, timeout: 0)
    end
  end

  @impl true
  def handle_info({:apply_transactions, _encoded_transactions}, %State{mode: :locked} = t) do
    # Discard transactions when locked
    noreply(t)
  end

  @impl true
  def handle_info({:apply_transactions, encoded_transactions}, %State{} = t) do
    # Queue the transactions and start processing
    updated_state = State.queue_transactions(t, encoded_transactions)
    queue_size = State.queue_size(updated_state)
    Telemetry.trace_transactions_queued(t.otp_name, length(encoded_transactions), queue_size)
    Telemetry.trace_transaction_timeout_scheduled(t.otp_name)
    noreply(updated_state, timeout: 0)
  end

  @impl true
  def handle_info(:timeout, %State{} = t) do
    case State.take_transaction_batch_by_size(t, @max_batch_size_bytes) do
      {[], nil, updated_state} ->
        # No more transactions to process
        noreply(updated_state)

      {batch, _batch_last_version, updated_state} ->
        # Process this batch
        batch_size = length(batch)
        batch_size_bytes = Enum.sum(Enum.map(batch, &byte_size/1))
        start_time = System.monotonic_time(:microsecond)
        Telemetry.trace_batch_processing_start(t.otp_name, batch_size, batch_size_bytes)

        {:ok, state_with_txns, version} = Logic.apply_transaction_batch(updated_state, batch)

        # Use new size-controlled window advancement
        final_state = maybe_advance_window_and_notify(state_with_txns, version)

        duration = System.monotonic_time(:microsecond) - start_time
        Telemetry.trace_batch_processing_complete(t.otp_name, batch_size, duration, batch_size_bytes)
        noreply_with_transaction_processing(final_state)
    end
  end

  @impl true
  def handle_info({:transactions_applied, version}, %State{} = t) do
    t
    |> Logic.notify_waiting_fetches(version)
    |> noreply_with_transaction_processing()
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, _reason}, %State{} = t) do
    Telemetry.trace_read_task_complete(t.otp_name, pid)

    t
    |> State.remove_active_task(pid)
    |> noreply_with_transaction_processing()
  end

  @impl true
  def handle_info(_msg, state) do
    {:noreply, state}
  end

  @impl true
  def terminate(reason, %State{} = t) do
    Telemetry.trace_shutdown_start(t.otp_name, reason)
    active_tasks = State.get_active_tasks(t)

    if MapSet.size(active_tasks) > 0 do
      wait_for_tasks(active_tasks, 5_000, t.otp_name)
    end

    Logic.shutdown(t)
    Telemetry.trace_shutdown_complete(t.otp_name)
    :ok
  end

  @impl true
  def terminate(_reason, _state), do: :ok

  defp wait_for_tasks(tasks, timeout, otp_name) do
    Telemetry.trace_shutdown_waiting(otp_name, MapSet.size(tasks))
    do_wait_for_tasks(tasks, timeout)
  end

  defp do_wait_for_tasks(tasks, timeout) do
    cond do
      MapSet.size(tasks) == 0 ->
        :ok

      timeout <= 0 ->
        Telemetry.trace_shutdown_timeout(MapSet.size(tasks))
        :timeout

      true ->
        start_time = System.monotonic_time(:millisecond)

        receive do
          {:DOWN, _ref, :process, pid, _reason} ->
            tasks = MapSet.delete(tasks, pid)
            elapsed = System.monotonic_time(:millisecond) - start_time
            do_wait_for_tasks(tasks, timeout - elapsed)
        after
          timeout ->
            Telemetry.trace_shutdown_timeout(MapSet.size(tasks))
            :timeout
        end
    end
  end

  defp reply_fn_for(from), do: fn result -> GenServer.reply(from, result) end
end
