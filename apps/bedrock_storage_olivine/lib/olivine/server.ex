defmodule Bedrock.DataPlane.Storage.Olivine.Server do
  @moduledoc false
  use GenServer

  import Bedrock.Internal.GenServer.Replies

  alias Bedrock.DataPlane.Storage
  alias Bedrock.DataPlane.Storage.Olivine.DataDatabase
  alias Bedrock.DataPlane.Storage.Olivine.Index
  alias Bedrock.DataPlane.Storage.Olivine.Index.Page
  alias Bedrock.DataPlane.Storage.Olivine.IndexDatabase
  alias Bedrock.DataPlane.Storage.Olivine.IndexManager
  alias Bedrock.DataPlane.Storage.Olivine.IntakeQueue
  alias Bedrock.DataPlane.Storage.Olivine.Logic
  alias Bedrock.DataPlane.Storage.Olivine.Reading
  alias Bedrock.DataPlane.Storage.Olivine.State
  alias Bedrock.DataPlane.Storage.Telemetry
  alias Bedrock.Service.Foreman

  # Transaction count limits for adaptive batching
  # Small batches for responsiveness during normal operation
  @continuation_batch_count 5
  # Larger batches during lulls when no reads are waiting
  @timeout_batch_count 50

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
    # Set operation context metadata for this request
    Telemetry.trace_metadata(%{operation: :get, key: key})

    fetch_opts = Keyword.put(opts, :reply_fn, reply_fn_for(from))
    context = Reading.ReadingContext.new(t.index_manager, t.database)

    {updated_manager, result} =
      Reading.handle_get(
        t.read_request_manager,
        context,
        key,
        version,
        Keyword.put_new(fetch_opts, :wait_ms, 1_000)
      )

    updated_state = %{t | read_request_manager: updated_manager}

    case result do
      :ok -> noreply(updated_state, continue: :maybe_process_transactions)
      {:error, _reason} = error -> reply(updated_state, error)
    end
  end

  def handle_call({:get_range, start_key, end_key, version, opts}, from, %State{} = t) do
    # Set operation context metadata for this request
    Telemetry.trace_metadata(%{operation: :get_range, key: {start_key, end_key}})

    fetch_opts = Keyword.put(opts, :reply_fn, reply_fn_for(from))
    context = Reading.ReadingContext.new(t.index_manager, t.database)

    {updated_manager, result} =
      Reading.handle_get_range(
        t.read_request_manager,
        context,
        start_key,
        end_key,
        version,
        Keyword.put_new(fetch_opts, :wait_ms, 1_000)
      )

    updated_state = %{t | read_request_manager: updated_manager}

    case result do
      :ok -> noreply(updated_state, continue: :maybe_process_transactions)
      {:error, _reason} = error -> reply(updated_state, error)
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
    {:ok, updated_state} = Logic.unlock_after_recovery(t, durable_version, transaction_system_layout)
    reply(updated_state, :ok)
  end

  @impl true
  def handle_call(:compact, _from, %State{compaction_task: task} = t) when not is_nil(task) do
    # Compaction already in progress
    reply(t, {:error, :compaction_in_progress})
  end

  @impl true
  def handle_call(:compact, _from, %State{} = t) do
    {:ok, task} = Logic.start_compaction(t)
    updated_state = %{t | compaction_task: task, allow_window_advancement: false}
    reply(updated_state, :ok)
  end

  @impl true
  def handle_call(_, _from, t), do: reply(t, {:error, :not_ready})

  @impl true
  def handle_continue(:finish_startup, {otp_name, foreman, id, path}) do
    # Set persistent telemetry metadata for this server
    Telemetry.trace_metadata(%{otp_name: otp_name, storage_id: id})

    Telemetry.trace_startup_start()

    case Logic.startup(otp_name, foreman, id, path) do
      {:ok, state} ->
        Telemetry.trace_startup_complete()
        noreply(state, continue: :report_health_to_foreman)

      {:error, reason} ->
        Telemetry.trace_startup_failed(reason)
        stop(:no_state, reason)
    end
  end

  @impl true
  def handle_continue(:report_health_to_foreman, %State{} = t) do
    :ok = Foreman.report_health(t.foreman, t.id, {:ok, self()})
    noreply(t, continue: :process_transactions)
  end

  @impl true
  def handle_continue(:process_transactions, %State{} = t) do
    case IntakeQueue.take_batch_by_count(t.intake_queue, @continuation_batch_count) do
      {[], nil, updated_intake_queue} ->
        # Queue empty, just wait for new transactions or timeout
        updated_state = %{t | intake_queue: updated_intake_queue}
        noreply(updated_state)

      {batch, _batch_last_version, updated_intake_queue} ->
        updated_state = %{t | intake_queue: updated_intake_queue}
        # Process small batch for responsiveness
        {:ok, state_with_txns, version} = Logic.apply_transactions(updated_state, batch)
        final_state = notify_waiting_fetches(state_with_txns, version)

        # Check for more transactions to process
        noreply(final_state, continue: :maybe_process_transactions)
    end
  end

  @impl true
  def handle_continue(:maybe_process_transactions, %State{} = t) do
    if IntakeQueue.empty?(t.intake_queue) do
      noreply(t, timeout: 0)
    else
      noreply(t, continue: :process_transactions)
    end
  end

  @impl true
  def handle_continue(:advance_window, %State{} = t) do
    if t.allow_window_advancement do
      {:ok, state_after_window} = Logic.advance_window(t)
      noreply(state_after_window)
    else
      # Compaction in progress - skip window advancement
      noreply(t)
    end
  end

  defp notify_waiting_fetches(state, version) do
    context = Reading.ReadingContext.new(state.index_manager, state.database)
    updated_manager = Reading.notify_waiting_fetches(state.read_request_manager, context, version)
    %{state | read_request_manager: updated_manager}
  end

  @impl true
  # Discard transactions when locked
  def handle_info({:apply_transactions, _encoded_transactions}, %State{mode: :locked} = t), do: noreply(t)

  @impl true
  def handle_info({:apply_transactions, encoded_transactions}, %State{} = t) do
    # Queue the transactions and start processing
    updated_intake_queue = IntakeQueue.add_transactions(t.intake_queue, encoded_transactions)
    updated_state = %{t | intake_queue: updated_intake_queue}
    queue_size = IntakeQueue.size(updated_intake_queue)
    Telemetry.trace_transactions_queued(length(encoded_transactions), queue_size)
    Telemetry.trace_transaction_timeout_scheduled()
    noreply(updated_state, continue: :process_transactions)
  end

  @impl true
  def handle_info(:timeout, %State{} = t) do
    # First, process a larger batch of transactions for throughput
    case IntakeQueue.take_batch_by_count(t.intake_queue, @timeout_batch_count) do
      {[], nil, updated_intake_queue} ->
        # No transactions to process, advance window during this lull
        updated_state = %{t | intake_queue: updated_intake_queue}
        noreply(updated_state, continue: :advance_window)

      {batch, _batch_last_version, updated_intake_queue} ->
        updated_state = %{t | intake_queue: updated_intake_queue}
        # Process larger batch for throughput
        {:ok, state_with_txns, version} = Logic.apply_transactions(updated_state, batch)
        state_after_txns = notify_waiting_fetches(state_with_txns, version)

        # Now advance window after processing transactions
        {:ok, final_state} = Logic.advance_window(state_after_txns)
        noreply(final_state, continue: :maybe_process_transactions)
    end
  end

  @impl true
  def handle_info({:transactions_applied, version}, %State{} = t) do
    t
    |> notify_waiting_fetches(version)
    |> noreply()
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, _reason}, %State{} = t) do
    updated_manager = Reading.remove_active_task(t.read_request_manager, pid)
    updated_state = %{t | read_request_manager: updated_manager}
    noreply(updated_state)
  end

  @impl true
  def handle_info(
        {:compaction_ready, compact_data_fd, compact_idx_fd, compact_data_path, compact_idx_path, new_data_offset,
         index_offset, compacted_pages, durable_version, duration, data_size_before, index_size_before},
        %State{} = t
      ) do
    # Atomic cutover to compacted files
    # Get file paths from old database
    alias Bedrock.DataPlane.Storage.Olivine.Telemetry, as: OlivineTelemetry

    {data_db, index_db} = t.database
    data_path = data_db.file_name
    idx_path = index_db.file_name

    # Note: We don't explicitly close the old files - on Unix, we can rename open files,
    # and they'll be closed automatically when no longer referenced. Attempting to close
    # them can fail with :not_on_controlling_process due to file descriptor ownership.

    # Rename files atomically

    # Create .old backup names
    old_data_path = data_path ++ ~c".old"
    old_idx_path = idx_path ++ ~c".old"

    :ok = :file.rename(data_path, old_data_path)
    :ok = :file.rename(idx_path, old_idx_path)
    :ok = :file.rename(compact_data_path, data_path)
    :ok = :file.rename(compact_idx_path, idx_path)

    # Clean up .old backup files after successful rename
    :ok = :file.delete(old_data_path)
    :ok = :file.delete(old_idx_path)

    # Build new database structures from compacted files
    # File name is now the original path (we renamed compact to replace it)
    new_data_db = %DataDatabase{
      file: compact_data_fd,
      file_offset: new_data_offset,
      file_name: data_path,
      window_size_in_microseconds: 5_000_000,
      buffer: :ets.new(:buffer, [:ordered_set, :protected, {:read_concurrency, true}])
    }

    new_index_db = %IndexDatabase{
      file: compact_idx_fd,
      file_offset: index_offset,
      file_name: idx_path,
      durable_version: durable_version,
      last_block_empty: false,
      last_block_offset: 0,
      last_block_previous_version: nil
    }

    new_database = {new_data_db, new_index_db}

    # Build index structures from in-memory compacted pages
    new_tree = Index.Tree.from_page_map(compacted_pages)
    {min_key, max_key} = calculate_key_bounds_from_pages(compacted_pages)

    # Get max_keys_per_page from the durable version's index
    {^durable_version, {durable_index, _modified}} =
      Enum.find(t.index_manager.versions, fn {v, _} -> v == durable_version end)

    new_index = %Index{
      tree: new_tree,
      page_map: compacted_pages,
      min_key: min_key,
      max_key: max_key,
      max_keys_per_page: durable_index.max_keys_per_page,
      target_keys_per_page: durable_index.target_keys_per_page
    }

    new_index_manager = %IndexManager{
      versions: [{durable_version, {new_index, %{}}}],
      current_version: durable_version,
      window_size_in_microseconds: 5_000_000,
      id_allocator: t.index_manager.id_allocator,
      output_queue: :queue.new(),
      last_version_ended_at_offset: 0,
      window_lag_time_μs: 5_000_000,
      n_keys: IndexManager.info(t.index_manager, :n_keys)
    }

    # Reset state for replay
    new_state = %{
      t
      | database: new_database,
        index_manager: new_index_manager,
        intake_queue: IntakeQueue.new(),
        compaction_task: nil,
        allow_window_advancement: true
    }

    # Emit completion telemetry
    values_compacted = Enum.sum(Enum.map(compacted_pages, fn {_, {page, _}} -> Page.key_count(page) end))

    OlivineTelemetry.trace_compaction_complete(durable_version,
      duration_μs: duration,
      data_size_before: data_size_before,
      data_size_after: new_data_offset,
      index_size_before: index_size_before,
      index_size_after: index_offset,
      values_compacted: values_compacted
    )

    # Resume normal operation
    # The puller will automatically fetch transactions from durable_version + 1
    # and rebuild the buffer through normal transaction processing
    noreply(new_state)
  end

  @impl true
  def handle_info({:compaction_failed, reason}, %State{} = t) do
    # Log error and resume normal operation
    require Logger

    Logger.error("Compaction failed: #{inspect(reason)}")

    # Clean up any partial .compact files
    {data_db, index_db} = t.database

    try do
      :file.delete(data_db.file_name ++ ~c".compact")
      :file.delete(index_db.file_name ++ ~c".compact")
    catch
      _, _ -> :ok
    end

    # Resume normal operation
    updated_state = %{t | compaction_task: nil, allow_window_advancement: true}
    noreply(updated_state)
  end

  @impl true
  def handle_info(_msg, state), do: {:noreply, state}

  @impl true
  def terminate(reason, %State{} = t) do
    Telemetry.trace_shutdown_start(reason)
    Reading.shutdown(t.read_request_manager)
    Logic.shutdown(t)
    Telemetry.trace_shutdown_complete()
    :ok
  end

  @impl true
  def terminate(_reason, _state), do: :ok

  defp reply_fn_for(from), do: fn result -> GenServer.reply(from, result) end

  # Calculate min/max key bounds from page_map
  defp calculate_key_bounds_from_pages(page_map) when map_size(page_map) == 0, do: {<<0xFF, 0xFF>>, <<>>}

  defp calculate_key_bounds_from_pages(page_map) do
    min_key =
      page_map
      |> Enum.map(fn {_id, {page, _next}} -> Page.left_key(page) end)
      |> Enum.reject(&is_nil/1)
      |> case do
        [] -> <<0xFF, 0xFF>>
        keys -> Enum.min(keys)
      end

    max_key =
      page_map
      |> Enum.map(fn {_id, {page, _next}} -> Page.right_key(page) end)
      |> Enum.reject(&is_nil/1)
      |> case do
        [] -> <<>>
        keys -> Enum.max(keys)
      end

    {min_key, max_key}
  end
end
