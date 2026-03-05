defmodule Bedrock.DataPlane.Materializer do
  @moduledoc """
  Materializer service for the data plane.

  Materializer nodes persist key-value data and handle range-based queries.
  """

  # Removed: import Bedrock.Internal.GenServer.Calls

  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.DataPlane.Materializer.Telemetry
  alias Bedrock.KeySelector
  alias Bedrock.Service.Worker

  @type ref :: Worker.ref()
  @type id :: Worker.id()
  @type key_range :: Bedrock.key_range()
  @type fact_name ::
          Worker.fact_name()
          | :key_ranges
          | :durable_version
          | :n_objects
          | :path
          | :size_in_bytes
          | :utilization

  @type recovery_info :: %{
          kind: :materializer,
          durable_version: Bedrock.version(),
          oldest_durable_version: Bedrock.version()
        }

  @spec recovery_info :: [fact_name()]
  def recovery_info, do: [:kind, :durable_version, :oldest_durable_version]

  @doc """
  Returns the value for the given key/version, or resolved key-value for KeySelector/version.
  """
  def get(storage, key_or_selector, version, opts \\ [])

  @spec get(
          storage_ref :: ref(),
          key :: Bedrock.key(),
          version :: Bedrock.version(),
          opts :: [timeout: timeout()]
        ) ::
          {:ok, value :: Bedrock.value() | nil}
          | {:error, :version_too_old | :version_too_new}
          | {:failure, :timeout | :unavailable, ref()}
  def get(storage, key, version, opts) when is_binary(key) do
    timeout = opts[:timeout] || :infinity
    start_time = System.monotonic_time()

    try do
      result = GenServer.call(storage, {:get, key, version, opts}, timeout)

      Telemetry.emit_materializer_operation(
        :get_success,
        %{duration: System.monotonic_time() - start_time},
        %{materializer_id: storage, key: key, version: version, result: elem(result, 0)}
      )

      result
    catch
      :exit, {:timeout, _} ->
        Telemetry.emit_materializer_operation(
          :get_timeout,
          %{duration: System.monotonic_time() - start_time, timeout_ms: timeout},
          %{materializer_id: storage, key: key, version: version}
        )

        {:failure, :timeout, storage}

      :exit, reason ->
        Telemetry.emit_materializer_operation(
          :get_unavailable,
          %{duration: System.monotonic_time() - start_time},
          %{materializer_id: storage, key: key, version: version, exit_reason: reason}
        )

        {:failure, :unavailable, storage}
    end
  end

  @spec get(
          storage_ref :: ref(),
          key_selector :: KeySelector.t(),
          version :: Bedrock.version(),
          opts :: [timeout: timeout()]
        ) ::
          {:ok, {resolved_key :: Bedrock.key(), value :: Bedrock.value()} | nil}
          | {:error, :version_too_old | :version_too_new}
          | {:failure, :timeout | :unavailable, ref()}
  def get(storage, %KeySelector{} = key_selector, version, opts) do
    timeout = opts[:timeout] || :infinity
    start_time = System.monotonic_time()

    try do
      result = GenServer.call(storage, {:get, key_selector, version, opts}, timeout)

      Telemetry.emit_materializer_operation(
        :get_success,
        %{duration: System.monotonic_time() - start_time},
        %{materializer_id: storage, key_selector: key_selector.key, version: version, result: elem(result, 0)}
      )

      result
    catch
      :exit, {:timeout, _} ->
        Telemetry.emit_materializer_operation(
          :get_timeout,
          %{duration: System.monotonic_time() - start_time, timeout_ms: timeout},
          %{materializer_id: storage, key_selector: key_selector.key, version: version}
        )

        {:failure, :timeout, storage}

      :exit, reason ->
        Telemetry.emit_materializer_operation(
          :get_unavailable,
          %{duration: System.monotonic_time() - start_time},
          %{materializer_id: storage, key_selector: key_selector.key, version: version, exit_reason: reason}
        )

        {:failure, :unavailable, storage}
    end
  end

  @doc """
  Returns key-value pairs for keys in the given range at the specified version.

  Range is [start_key, end_key) - includes start_key, excludes end_key.
  Supports both binary keys and KeySelectors for range boundaries.
  Only supported by Olivine materializer engine; Basalt returns {:error, :unsupported}.
  """
  def get_range(storage, start_key_or_selector, end_key_or_selector, version, opts \\ [])

  @spec get_range(
          storage_ref :: ref(),
          start_key :: Bedrock.key(),
          end_key :: Bedrock.key(),
          version :: Bedrock.version(),
          opts :: [
            limit: pos_integer(),
            timeout: timeout()
          ]
        ) ::
          {:ok, {[{key :: Bedrock.key(), value :: Bedrock.value()}], more :: boolean()}}
          | {:error, :version_too_old | :version_too_new | :unsupported}
          | {:failure, :timeout | :unavailable, ref()}
  def get_range(storage, start_key, end_key, version, opts) when is_binary(start_key) and is_binary(end_key) do
    timeout = opts[:timeout] || :infinity
    start_time = System.monotonic_time()

    try do
      result = GenServer.call(storage, {:get_range, start_key, end_key, version, opts}, timeout)

      Telemetry.emit_materializer_operation(
        :get_range_success,
        %{duration: System.monotonic_time() - start_time},
        %{materializer_id: storage, start_key: start_key, end_key: end_key, version: version, result: elem(result, 0)}
      )

      result
    catch
      :exit, {:timeout, _} ->
        Telemetry.emit_materializer_operation(
          :get_range_timeout,
          %{duration: System.monotonic_time() - start_time, timeout_ms: timeout},
          %{materializer_id: storage, start_key: start_key, end_key: end_key, version: version}
        )

        {:failure, :timeout, storage}

      :exit, reason ->
        Telemetry.emit_materializer_operation(
          :get_range_unavailable,
          %{duration: System.monotonic_time() - start_time},
          %{materializer_id: storage, start_key: start_key, end_key: end_key, version: version, exit_reason: reason}
        )

        {:failure, :unavailable, storage}
    end
  end

  @spec get_range(
          storage_ref :: ref(),
          start_selector :: KeySelector.t(),
          end_selector :: KeySelector.t(),
          version :: Bedrock.version(),
          opts :: [
            limit: pos_integer(),
            timeout: timeout()
          ]
        ) ::
          {:ok, {[{key :: Bedrock.key(), value :: Bedrock.value()}], more :: boolean()}}
          | {:error, :version_too_old | :version_too_new | :unsupported | :not_found | :invalid_range}
          | {:failure, :timeout | :unavailable, ref()}
  def get_range(storage, %KeySelector{} = start_selector, %KeySelector{} = end_selector, version, opts) do
    timeout = opts[:timeout] || :infinity
    start_time = System.monotonic_time()

    try do
      result = GenServer.call(storage, {:get_range, start_selector, end_selector, version, opts}, timeout)

      Telemetry.emit_materializer_operation(
        :get_range_success,
        %{duration: System.monotonic_time() - start_time},
        %{
          materializer_id: storage,
          start_selector: start_selector.key,
          end_selector: end_selector.key,
          version: version,
          result: elem(result, 0)
        }
      )

      result
    catch
      :exit, {:timeout, _} ->
        Telemetry.emit_materializer_operation(
          :get_range_timeout,
          %{duration: System.monotonic_time() - start_time, timeout_ms: timeout},
          %{
            materializer_id: storage,
            start_selector: start_selector.key,
            end_selector: end_selector.key,
            version: version
          }
        )

        {:failure, :timeout, storage}

      :exit, reason ->
        Telemetry.emit_materializer_operation(
          :get_range_unavailable,
          %{duration: System.monotonic_time() - start_time},
          %{
            materializer_id: storage,
            start_selector: start_selector.key,
            end_selector: end_selector.key,
            version: version,
            exit_reason: reason
          }
        )

        {:failure, :unavailable, storage}
    end
  end

  @doc """
  Request that the materializer service lock itself and stop pulling new transactions
  from the logs. This mechanism is used by a newly elected cluster director
  to prevent new transactions from being accepted while it is establishing
  its authority.

  In order for the lock to succeed, the given epoch needs to be greater than
  the current epoch.
  """
  @spec lock_for_recovery(storage_ref :: ref(), recovery_epoch :: Bedrock.epoch()) ::
          {:ok, storage_pid :: pid(),
           recovery_info :: [
             {:kind, :materializer}
             | {:durable_version, Bedrock.version()}
             | {:oldest_durable_version, Bedrock.version()}
           ]}
          | {:error, :newer_epoch_exists}
  defdelegate lock_for_recovery(storage, epoch), to: Worker

  @doc """
  Unlocks the materializer after recovery is complete. This allows the materializer
  to start accepting new transactions again and continue normal operation.

  The durable version and transaction system layout must be provided to
  ensure that the materializer is unlocked at the correct state.
  """
  @spec unlock_after_recovery(
          storage :: ref(),
          durable_version :: Bedrock.version(),
          TransactionSystemLayout.t(),
          opts :: [timeout_in_ms: Bedrock.timeout_in_ms()]
        ) :: :ok | {:error, :unavailable} | {:failure, :timeout, ref()}
  def unlock_after_recovery(storage, durable_version, transaction_system_layout, opts \\ []) do
    timeout = opts[:timeout_in_ms] || :infinity
    start_time = System.monotonic_time()

    try do
      result = GenServer.call(storage, {:unlock_after_recovery, durable_version, transaction_system_layout}, timeout)

      Telemetry.emit_materializer_operation(
        :unlock_after_recovery_success,
        %{duration: System.monotonic_time() - start_time},
        %{materializer_id: storage, durable_version: durable_version}
      )

      result
    catch
      :exit, {:timeout, _} ->
        Telemetry.emit_materializer_operation(
          :unlock_after_recovery_timeout,
          %{duration: System.monotonic_time() - start_time, timeout_ms: timeout},
          %{materializer_id: storage, durable_version: durable_version}
        )

        {:failure, :timeout, storage}

      :exit, reason ->
        Telemetry.emit_materializer_operation(
          :unlock_after_recovery_unavailable,
          %{duration: System.monotonic_time() - start_time},
          %{materializer_id: storage, durable_version: durable_version, exit_reason: reason}
        )

        {:failure, :unavailable, storage}
    end
  end

  @doc """
  Ask the materializer for various facts about itself.
  """
  @spec info(storage :: ref(), [fact_name()], opts :: [timeout_in_ms: Bedrock.timeout_in_ms()]) ::
          {:ok,
           %{
             fact_name() => Bedrock.value() | Bedrock.version() | [key_range()] | non_neg_integer() | Path.t()
           }}
          | {:error, :unavailable}
  defdelegate info(storage, fact_names, opts \\ []), to: Worker
end
