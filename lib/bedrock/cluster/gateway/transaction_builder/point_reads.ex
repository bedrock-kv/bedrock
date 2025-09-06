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

  @type storage_get_key_fn() :: (pid(), binary(), Bedrock.version(), keyword() ->
                                   {:ok, binary()} | {:error, atom()})

  @type storage_get_key_selector_fn() :: (pid(), KeySelector.t(), Bedrock.version(), keyword() ->
                                            {:ok, binary()} | {:error, atom()})

  @doc """
  Get a regular key within the transaction context.

  Expects pre-encoded keys and returns raw values.
  """
  @spec get_key(State.t(), key :: Bedrock.key(), opts :: [storage_get_key_fn: storage_get_key_fn()]) ::
          {State.t(),
           {:ok, {Bedrock.key(), Bedrock.value()}}
           | {:error, :not_found}
           | {:error, :version_too_old}
           | {:error, :version_too_new}
           | {:error, :unavailable}
           | {:error, :timeout}}
  def get_key(t, key, opts \\ []) do
    case Tx.repeatable_read(t.tx, key) do
      nil ->
        storage_get_key_fn = Keyword.get(opts, :storage_get_key_fn, &Storage.get/4)

        t
        |> ensure_read_version!(opts)
        |> get_value_from_storage(
          key,
          &case storage_get_key_fn.(&1, key, &2, timeout: &3) do
            {:ok, raw_value} -> {:ok, {key, raw_value}}
            {:error, reason} -> {:error, reason}
            {:failure, reason, storage_id} -> {:failure, reason, storage_id}
          end
        )

      :clear ->
        {t, {:error, :not_found}}

      value ->
        {t, {:ok, {key, value}}}
    end
  end

  @doc """
  Get a KeySelector within the transaction context.
  """
  @spec get_key_selector(
          State.t(),
          KeySelector.t(),
          opts :: [storage_get_key_selector_fn: storage_get_key_selector_fn()]
        ) ::
          {State.t(),
           {:ok, Bedrock.key_value()}
           | {:error, :not_found}
           | {:error, :version_too_old}
           | {:error, :version_too_new}
           | {:error, :decode_error}
           | {:error, :unavailable}
           | {:error, :timeout}}
  def get_key_selector(t, %KeySelector{} = key_selector, opts \\ []) do
    storage_get_key_selector_fn = Keyword.get(opts, :storage_get_key_selector_fn, &Storage.get/4)

    t
    |> ensure_read_version!(opts)
    |> get_value_from_storage(
      key_selector.key,
      &case storage_get_key_selector_fn.(&1, key_selector, &2, timeout: &3) do
        {:ok, nil} -> {:ok, nil}
        {:ok, {resolved_key, value}} -> {:ok, {resolved_key, value}}
        {:error, reason} -> {:error, reason}
        {:failure, reason, storage_id} -> {:failure, reason, storage_id}
      end
    )
  end

  # Private helper functions

  @spec get_value_from_storage(
          State.t(),
          racing_key :: binary(),
          operation_fn :: (pid(), Bedrock.version(), Bedrock.timeout_in_ms() -> {:ok, any()} | {:error, any()})
        ) ::
          {State.t(), {:ok, {binary(), binary()}} | {:error, atom()}}
  defp get_value_from_storage(state, racing_key, operation_fn) do
    state
    |> StorageRacing.race_storage_servers(racing_key, operation_fn)
    |> case do
      # Key selector returned nil (not found)
      {final_state, {:ok, {nil, _shard_range}}} ->
        {final_state, {:error, :not_found}}

      # Regular key returned nil (not found)
      {final_state, {:ok, {{key, nil}, _shard_range}}} ->
        {%{final_state | tx: Tx.merge_storage_read(final_state.tx, key, :not_found)}, {:error, :not_found}}

      # Regular key or key selector returned value
      {final_state, {:ok, {{key, value}, _shard_range}}} ->
        {%{final_state | tx: Tx.merge_storage_read(final_state.tx, key, value)}, {:ok, {key, value}}}

      {state, {:error, reason}} ->
        {state, {:error, reason}}
    end
  end
end
