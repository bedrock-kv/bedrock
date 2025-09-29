defmodule Bedrock.DataPlane.Storage.Olivine.Logic do
  @moduledoc false

  import Bedrock.DataPlane.Storage.Olivine.State,
    only: [update_mode: 2, update_director_and_epoch: 3, reset_puller: 1, put_puller: 2]

  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.ControlPlane.Director
  alias Bedrock.DataPlane.Storage
  alias Bedrock.DataPlane.Storage.Olivine.Database
  alias Bedrock.DataPlane.Storage.Olivine.IndexManager
  alias Bedrock.DataPlane.Storage.Olivine.Pulling
  alias Bedrock.DataPlane.Storage.Olivine.State
  alias Bedrock.DataPlane.Storage.Olivine.Telemetry, as: OlivineTelemetry
  alias Bedrock.DataPlane.Storage.Telemetry
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.DataPlane.Version
  alias Bedrock.Service.Worker

  @spec startup(otp_name :: atom(), foreman :: pid(), id :: Worker.id(), Path.t()) ::
          {:ok, State.t()} | {:error, File.posix()} | {:error, term()}
  @spec startup(otp_name :: atom(), foreman :: pid(), id :: Worker.id(), Path.t(), db_opts :: keyword()) ::
          {:ok, State.t()} | {:error, File.posix()} | {:error, term()}
  def startup(otp_name, foreman, id, path, db_opts \\ []) do
    with :ok <- ensure_directory_exists(path),
         {:ok, database} <- Database.open(:"#{otp_name}_db", Path.join(path, "dets"), db_opts),
         {:ok, index_manager} <- IndexManager.recover_from_database(database) do
      {:ok,
       %State{
         path: path,
         otp_name: otp_name,
         id: id,
         foreman: foreman,
         database: database,
         index_manager: index_manager
       }}
    end
  end

  @spec ensure_directory_exists(Path.t()) :: :ok | {:error, File.posix()}
  defp ensure_directory_exists(path), do: File.mkdir_p(path)

  @spec shutdown(State.t()) :: :ok
  def shutdown(%State{} = t) do
    stop_pulling(t)
    :ok = Database.close(t.database)
  end

  @spec lock_for_recovery(State.t(), Director.ref(), Bedrock.epoch()) ::
          {:ok, State.t()} | {:error, :newer_epoch_exists | String.t()}
  def lock_for_recovery(t, _, epoch) when not is_nil(t.epoch) and epoch < t.epoch, do: {:error, :newer_epoch_exists}

  def lock_for_recovery(t, director, epoch) do
    t
    |> update_mode(:locked)
    |> update_director_and_epoch(director, epoch)
    |> stop_pulling()
    |> then(&{:ok, &1})
  end

  @spec stop_pulling(State.t()) :: State.t()
  def stop_pulling(%{pull_task: nil} = t), do: t

  def stop_pulling(%{pull_task: puller} = t) do
    Pulling.stop(puller)
    reset_puller(t)
  end

  @spec unlock_after_recovery(State.t(), Bedrock.version(), TransactionSystemLayout.t()) ::
          {:ok, State.t()}
  def unlock_after_recovery(t, durable_version, %{logs: logs, services: services}) do
    t = stop_pulling(t)
    main_process_pid = self()

    apply_and_notify_fn = fn transactions ->
      send(main_process_pid, {:apply_transactions, transactions})
      last_transaction = List.last(transactions)
      Transaction.commit_version!(last_transaction)
    end

    database = t.database

    puller =
      Pulling.start_pulling(
        durable_version,
        t.id,
        logs,
        services,
        apply_and_notify_fn,
        fn -> Database.load_current_durable_version(database) end
      )

    t
    |> update_mode(:running)
    |> put_puller(puller)
    |> then(&{:ok, &1})
  end

  @spec info(State.t(), Storage.fact_name() | [Storage.fact_name()]) ::
          {:ok, term() | %{Storage.fact_name() => term()}} | {:error, :unsupported_info}
  def info(%State{} = t, fact_name) when is_atom(fact_name), do: {:ok, gather_info(fact_name, t)}

  def info(%State{} = t, fact_names) when is_list(fact_names) do
    {:ok,
     fact_names
     |> Enum.reduce([], fn
       fact_name, acc -> [{fact_name, gather_info(fact_name, t)} | acc]
     end)
     |> Map.new()}
  end

  defp supported_info, do: ~w[
      durable_version
      oldest_durable_version
      id
      pid
      path
      key_ranges
      kind
      n_keys
      otp_name
      size_in_bytes
      supported_info
      utilization
    ]a

  defp gather_info(:oldest_durable_version, t), do: Database.durable_version(t.database)
  defp gather_info(:durable_version, t), do: Database.durable_version(t.database)
  defp gather_info(:id, t), do: t.id
  defp gather_info(:key_ranges, t), do: IndexManager.info(t.index_manager, :key_ranges)
  defp gather_info(:kind, _t), do: :storage
  defp gather_info(:n_keys, t), do: IndexManager.info(t.index_manager, :n_keys)
  defp gather_info(:otp_name, t), do: t.otp_name
  defp gather_info(:path, t), do: t.path
  defp gather_info(:pid, _t), do: self()
  defp gather_info(:size_in_bytes, t), do: IndexManager.info(t.index_manager, :size_in_bytes)
  defp gather_info(:supported_info, _t), do: supported_info()
  defp gather_info(:utilization, t), do: IndexManager.info(t.index_manager, :utilization)
  defp gather_info(_unsupported, _t), do: {:error, :unsupported_info}

  # Window advancement constants

  defp max_eviction_size, do: 10 * 1024 * 1024

  @doc """
  Performs window advancement by delegating policy decisions to IndexManager and handling persistence.
  IndexManager determines what to evict based on buffer tracking and hot set management.
  Logic handles database persistence and telemetry for the eviction.
  """
  @spec advance_window(State.t()) :: {:ok, State.t()} | {:error, term()}
  def advance_window(%State{} = state) do
    %{start_time: System.monotonic_time(:microsecond)}
    |> index_manager_advance_window(state)
    |> database_advance_durable_version()
    |> emit_telemetry()
    |> then(&{:ok, &1.state})
  end

  defp index_manager_advance_window(pipeline, %State{} = state) do
    case IndexManager.advance_window(state.index_manager, max_eviction_size()) do
      {:no_eviction, updated_index_manager} ->
        Map.merge(pipeline, %{
          result: :no_eviction,
          state: %{state | index_manager: updated_index_manager},
          current_version: state.index_manager.current_version
        })

      {:evict, batch, updated_index_manager} ->
        {new_durable_version, data_size_in_bytes, _} = List.last(batch)
        window_edge = calculate_window_edge_for_telemetry(batch, state.window_lag_time_μs)

        Map.merge(pipeline, %{
          result: :evicted,
          state: %{state | index_manager: updated_index_manager},
          current_version: new_durable_version,
          batch: batch,
          new_durable_version: new_durable_version,
          data_size_in_bytes: data_size_in_bytes,
          window_edge: window_edge,
          evicted_count: length(batch),
          lag_time_μs: calculate_lag_time_μs(window_edge, new_durable_version)
        })
    end
  end

  defp database_advance_durable_version(%{result: :no_eviction} = pipeline), do: pipeline

  defp database_advance_durable_version(%{result: :evicted} = pipeline) do
    {:ok, updated_database, db_pipeline} =
      Database.advance_durable_version(
        pipeline.state.database,
        pipeline.new_durable_version,
        pipeline.data_size_in_bytes
      )

    Map.merge(pipeline, %{
      state: %{pipeline.state | database: updated_database},
      tx_size_bytes: db_pipeline.tx_size_bytes,
      tx_count: db_pipeline.tx_count,
      # Database-specific telemetry
      db_build_time_μs: db_pipeline.build_time_μs,
      db_insert_time_μs: db_pipeline.insert_time_μs,
      db_write_time_μs: db_pipeline.write_time_μs,
      db_sync_time_μs: db_pipeline.sync_time_μs,
      db_cleanup_time_μs: db_pipeline.cleanup_time_μs,
      durable_version_duration_μs: db_pipeline.total_duration_μs
    })
  end

  defp emit_telemetry(pipeline) do
    duration = System.monotonic_time(:microsecond) - pipeline.start_time

    case pipeline.result do
      :no_eviction ->
        pipeline

      :evicted ->
        OlivineTelemetry.trace_window_advanced(pipeline.result, pipeline.current_version,
          duration_μs: duration,
          evicted_count: pipeline.evicted_count,
          lag_time_μs: pipeline.lag_time_μs,
          #
          window_target_version: pipeline.window_edge,
          data_size_in_bytes: pipeline.data_size_in_bytes,
          durable_version_duration_μs: pipeline.durable_version_duration_μs,
          tx_size_bytes: pipeline.tx_size_bytes,
          tx_count: pipeline.tx_count,
          #
          db_build_time_μs: pipeline.db_build_time_μs,
          db_insert_time_μs: pipeline.db_insert_time_μs,
          db_write_time_μs: pipeline.db_write_time_μs,
          db_sync_time_μs: pipeline.db_sync_time_μs,
          db_cleanup_time_μs: pipeline.db_cleanup_time_μs
        )

        pipeline
    end
  end

  # Helper to calculate window edge for telemetry purposes
  # We need to reconstruct this from the batch since IndexManager now handles the calculation internally
  defp calculate_window_edge_for_telemetry(batch, window_lag_time_μs) do
    {newest_version_in_batch, _, _} = List.last(batch)
    Version.subtract(newest_version_in_batch, window_lag_time_μs)
  rescue
    ArgumentError ->
      # Underflow - return zero version
      Version.zero()
  end

  defp calculate_lag_time_μs(window_edge_version, eviction_version) do
    max(0, Version.distance(window_edge_version, eviction_version))
  rescue
    _ -> 0
  end

  @doc """
  Apply a batch of transactions to the storage state.
  This is used for incremental processing to avoid large DETS writes.
  Buffer tracking is now handled directly by IndexManager.apply_transactions.
  """
  @spec apply_transaction_batch(State.t(), [binary()]) :: {:ok, State.t(), Bedrock.version()}
  def apply_transaction_batch(%State{} = t, encoded_transactions) do
    batch_size = length(encoded_transactions)
    batch_size_bytes = Enum.sum(Enum.map(encoded_transactions, &byte_size/1))
    start_time = System.monotonic_time(:microsecond)

    # Set batch processing context metadata
    Telemetry.trace_metadata(%{batch_size: batch_size, batch_size_bytes: batch_size_bytes})
    Telemetry.trace_transaction_processing_start(batch_size, batch_size_bytes)

    {updated_index_manager, updated_database} =
      IndexManager.apply_transactions(t.index_manager, encoded_transactions, t.database)

    version = updated_index_manager.current_version

    duration = System.monotonic_time(:microsecond) - start_time
    Telemetry.trace_transaction_processing_complete(batch_size, duration, batch_size_bytes)

    # Update state with both updated index manager and database
    updated_state = %{t | index_manager: updated_index_manager, database: updated_database}
    {:ok, updated_state, version}
  end
end
