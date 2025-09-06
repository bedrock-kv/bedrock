defmodule Bedrock.Cluster.Gateway.TransactionBuilder.PointReads do
  @moduledoc """
  Point read operations for the Transaction Builder.

  This module handles single-point read operations, including regular key fetches
  and key selector resolution. All operations ensure proper transaction semantics
  with repeatable reads and conflict tracking.
  """

  import Bedrock.Cluster.Gateway.TransactionBuilder.ReadVersions, only: [ensure_read_version!: 2]

  alias Bedrock.Cluster.Gateway.TransactionBuilder.State
  alias Bedrock.Cluster.Gateway.TransactionBuilder.StorageRacing
  alias Bedrock.Cluster.Gateway.TransactionBuilder.Tx
  alias Bedrock.DataPlane.Storage
  alias Bedrock.KeySelector

  @type storage_get_fn() :: (pid(), KeySelector.t() | binary(), Bedrock.version(), keyword() ->
                               {:ok, binary()} | {:error, atom()})

  @doc """
  Get a regular key within the transaction context.

  Expects pre-encoded keys and returns raw values.
  """
  @spec get_key(State.t(), key :: Bedrock.key(), opts :: [storage_get_fn: storage_get_fn()]) ::
          {State.t(),
           {:ok, Bedrock.value()}
           | {:error, :not_found}
           | {:error, :version_too_old}
           | {:error, :version_too_new}
           | {:error, :unavailable}
           | {:error, :timeout}}
  def get_key(t, key, opts \\ []) do
    case Tx.repeatable_read(t.tx, key) do
      nil ->
        operation_fn = get_key_operation(key, opts)

        case fetch_as_key_value(t, key, key, operation_fn, opts) do
          {:ok, new_state, {_key, raw_value}} ->
            {new_state, {:ok, raw_value}}

          {:error, reason, new_state} ->
            {new_state, {:error, reason}}
        end

      :clear ->
        {t, {:error, :not_found}}

      value ->
        {t, {:ok, value}}
    end
  end

  @doc """
  Get a KeySelector within the transaction context.
  """
  @spec get_key_selector(State.t(), KeySelector.t(), opts :: [storage_get_fn: storage_get_fn()]) ::
          {State.t(),
           {:ok, Bedrock.key_value()}
           | {:error, :not_found}
           | {:error, :version_too_old}
           | {:error, :version_too_new}
           | {:error, :decode_error}
           | {:error, :unavailable}
           | {:error, :timeout}}
  def get_key_selector(t, %KeySelector{} = key_selector, opts \\ []) do
    operation_fn = get_key_selector_operation(key_selector, opts)

    case fetch_as_key_value(t, key_selector.key, :no_merge, operation_fn, opts) do
      {:ok, new_state, {resolved_key, value}} -> {new_state, {:ok, {resolved_key, value}}}
      {:error, reason, new_state} -> {new_state, {:error, reason}}
    end
  end

  # Private helper functions

  @spec get_key_operation(binary(), keyword()) :: (pid(), State.t() -> {:ok, any()} | {:error, any()})
  defp get_key_operation(key, opts) do
    storage_get_fn = Keyword.get(opts, :storage_get_fn, &Storage.get/4)

    fn storage_server, state ->
      case storage_get_fn.(storage_server, key, state.read_version, timeout: state.fetch_timeout_in_ms) do
        {:ok, raw_value} -> {:ok, {:ok, {key, raw_value}}}
        {:error, reason} when reason in [:not_found, :version_too_old, :version_too_new] -> {:ok, {:error, reason}}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @spec get_key_selector_operation(KeySelector.t(), keyword()) :: (pid(), State.t() -> {:ok, any()} | {:error, any()})
  defp get_key_selector_operation(key_selector, opts) do
    storage_get_fn = Keyword.get(opts, :storage_get_fn, &Storage.get/4)

    fn storage_server, state ->
      case storage_get_fn.(storage_server, key_selector, state.read_version, timeout: state.fetch_timeout_in_ms) do
        {:ok, {resolved_key, value}} -> {:ok, {:ok, {resolved_key, value}}}
        {:error, reason} -> {:ok, {:error, reason}}
      end
    end
  end

  @spec fetch_as_key_value(
          State.t(),
          racing_key :: binary(),
          merge_key :: binary() | :no_merge,
          operation_fn :: (pid(), State.t() -> {:ok, any()} | {:error, any()}),
          opts :: keyword()
        ) ::
          {:ok, State.t(), {binary(), binary()}}
          | {:error, atom(), State.t()}
  defp fetch_as_key_value(state, racing_key, merge_key, operation_fn, opts) do
    state
    |> ensure_read_version!(opts)
    |> StorageRacing.race_storage_servers(racing_key, operation_fn)
    |> case do
      {:ok, {{:ok, {key, value}}, _shard_range}, final_state} ->
        {:ok, %{final_state | tx: Tx.merge_storage_read(final_state.tx, key, value)}, {key, value}}

      {:ok, {{:error, :not_found}, _shard_range}, final_state} when merge_key != :no_merge ->
        {:error, :not_found, %{final_state | tx: Tx.merge_storage_read(final_state.tx, merge_key, :not_found)}}

      {:ok, {{:error, :not_found}, _shard_range}, final_state} ->
        {:error, :not_found, final_state}

      {:ok, {{:error, reason}, _shard_range}, final_state} ->
        {:error, reason, final_state}

      {:error, reason, state} ->
        {:error, reason, state}
    end
  end
end
