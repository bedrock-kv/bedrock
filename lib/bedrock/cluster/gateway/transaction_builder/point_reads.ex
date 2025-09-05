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

  @type storage_fetch_fn() :: (pid(), KeySelector.t() | binary(), Bedrock.version(), keyword() ->
                                 {:ok, binary()} | {:error, atom()})

  @doc """
  Fetch a regular key within the transaction context.

  Expects pre-encoded keys and returns raw values.
  """
  @spec fetch_key(State.t(), key :: Bedrock.key()) ::
          {State.t(),
           {:ok, Bedrock.value()}
           | {:error, :not_found}
           | {:error, :version_too_old}
           | {:error, :version_too_new}
           | {:error, :unavailable}
           | {:error, :timeout}}
  @spec fetch_key(
          State.t(),
          key :: Bedrock.key(),
          opts :: [
            storage_fetch_fn: storage_fetch_fn()
          ]
        ) ::
          {State.t(),
           {:ok, Bedrock.value()}
           | {:error, :not_found | :version_too_old | :version_too_new | :unavailable | :timeout}}
  def fetch_key(t, key, opts \\ []) do
    case Tx.repeatable_read(t.tx, key) do
      nil ->
        operation_fn = fn storage_server, state ->
          storage_fetch_fn = Keyword.get(opts, :storage_fetch_fn, &Storage.fetch/4)
          wrap_storage_fetch_result(storage_fetch_fn, storage_server, key, state)
        end

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
  Fetch a KeySelector within the transaction context.
  """
  @spec fetch_key_selector(State.t(), KeySelector.t()) :: {State.t(), {:ok, Bedrock.key_value()} | {:error, atom()}}
  @spec fetch_key_selector(
          State.t(),
          KeySelector.t(),
          opts :: [
            storage_fetch_fn: storage_fetch_fn()
          ]
        ) ::
          {State.t(),
           {:ok, Bedrock.key_value()}
           | {:error, :not_found}
           | {:error, :version_too_old}
           | {:error, :version_too_new}
           | {:error, :decode_error}
           | {:error, :unavailable}
           | {:error, :timeout}}
  def fetch_key_selector(t, %KeySelector{} = key_selector, opts \\ []) do
    operation_fn = fn storage_server, state ->
      storage_fetch_fn = Keyword.get(opts, :storage_fetch_fn, &Storage.fetch/4)

      case storage_fetch_fn.(storage_server, key_selector, state.read_version, timeout: state.fetch_timeout_in_ms) do
        {:ok, {resolved_key, value}} -> {:ok, {:ok, {resolved_key, value}}}
        {:error, reason} -> {:ok, {:error, reason}}
      end
    end

    case fetch_as_key_value(t, key_selector.key, :no_merge, operation_fn, opts) do
      {:ok, new_state, {resolved_key, value}} -> {new_state, {:ok, {resolved_key, value}}}
      {:error, reason, new_state} -> {new_state, {:error, reason}}
    end
  end

  # Private helper functions

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
    |> StorageRacing.race_storage_servers(racing_key, operation_fn, opts)
    |> case do
      {:ok, {:ok, {key, value}}, final_state} ->
        {:ok, %{final_state | tx: Tx.merge_storage_read(final_state.tx, key, value)}, {key, value}}

      {:ok, {:error, :not_found}, final_state} when merge_key != :no_merge ->
        {:error, :not_found, %{final_state | tx: Tx.merge_storage_read(final_state.tx, merge_key, :not_found)}}

      {:ok, {:error, :not_found}, final_state} ->
        {:error, :not_found, final_state}

      {:ok, {:error, reason}, final_state} ->
        {:error, reason, final_state}

      {:error, reason, state} ->
        {:error, reason, state}
    end
  end

  # Helper function to avoid deep nesting in fetch_key operation_fn
  defp wrap_storage_fetch_result(storage_fetch_fn, storage_server, key, state) do
    case storage_fetch_fn.(storage_server, key, state.read_version, timeout: state.fetch_timeout_in_ms) do
      {:ok, raw_value} -> {:ok, {:ok, {key, raw_value}}}
      {:error, reason} when reason in [:not_found, :version_too_old, :version_too_new] -> {:ok, {:error, reason}}
      {:error, reason} -> {:error, reason}
    end
  end
end
