defmodule Bedrock.Internal.TransactionBuilder.PointReads do
  @moduledoc """
  Point read operations for the Transaction Builder.

  This module handles single-point read operations, including regular key fetches
  and key selector resolution. All operations ensure proper transaction semantics
  with repeatable reads and conflict tracking.
  """

  import Bedrock.Internal.TransactionBuilder.ReadVersions, only: [ensure_read_version: 2]

  alias Bedrock.DataPlane.Materializer
  alias Bedrock.Internal.TransactionBuilder.State
  alias Bedrock.Internal.TransactionBuilder.StorageRacing
  alias Bedrock.Internal.TransactionBuilder.Tx
  alias Bedrock.KeySelector

  @type storage_get_key_fn() :: (pid(), binary(), Bedrock.version(), keyword() ->
                                   {:ok, binary()} | {:error, atom()})

  @type storage_get_key_selector_fn() :: (pid(), KeySelector.t(), Bedrock.version(), keyword() ->
                                            {:ok, binary()} | {:error, atom()})

  @doc """
  Get a regular key within the transaction context.

  Expects pre-encoded keys and returns raw values.
  """
  @spec get_key(
          State.t(),
          key :: Bedrock.key(),
          opts :: [storage_get_key_fn: storage_get_key_fn(), snapshot: boolean()]
        ) ::
          {State.t(),
           {:ok, {Bedrock.key(), Bedrock.value()}}
           | {:error, :not_found}
           | {:failure,
              %{
                (:timeout
                 | :unavailable
                 | :version_too_old
                 | :version_too_new
                 | :no_servers_to_race
                 | :layout_lookup_failed) => [pid()]
              }}}
  def get_key(t, key, opts \\ []) do
    case Tx.repeatable_read(t.tx, key) do
      nil ->
        get_key_from_storage(t, key, opts)

      :clear ->
        {t, {:error, :not_found}}

      value ->
        {t, {:ok, {key, value}}}
    end
  end

  defp get_key_from_storage(t, key, opts) do
    storage_get_key_fn = Keyword.get(opts, :storage_get_key_fn, &Materializer.get/4)

    case ensure_read_version(t, opts) do
      {:ok, t} ->
        execute_get_query(
          t,
          key,
          &(&1 |> storage_get_key_fn.(key, &2, timeout: &3) |> wrap_storage_get_result(key)),
          opts
        )

      {:failure, failures_by_reason} ->
        {t, {:failure, failures_by_reason}}
    end
  end

  defp wrap_storage_get_result({:ok, raw_value}, key), do: {:ok, {key, raw_value}}
  defp wrap_storage_get_result({:error, reason}, _key), do: {:error, reason}
  defp wrap_storage_get_result({:failure, reason, storage_id}, _key), do: {:failure, reason, storage_id}

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
           | {:failure,
              %{
                (:timeout
                 | :unavailable
                 | :version_too_old
                 | :version_too_new
                 | :no_servers_to_race
                 | :layout_lookup_failed) => [pid()]
              }}}
  def get_key_selector(t, %KeySelector{} = key_selector, opts \\ []) do
    storage_get_key_selector_fn = Keyword.get(opts, :storage_get_key_selector_fn, &Materializer.get/4)

    case ensure_read_version(t, opts) do
      {:ok, t} ->
        execute_get_query(
          t,
          key_selector.key,
          &case storage_get_key_selector_fn.(&1, key_selector, &2, timeout: &3) do
            {:ok, nil} -> {:ok, nil}
            {:ok, {resolved_key, value}} -> {:ok, {resolved_key, value}}
            {:error, reason} -> {:error, reason}
            {:failure, reason, storage_id} -> {:failure, reason, storage_id}
          end,
          opts
        )

      {:failure, failures_by_reason} ->
        {t, {:failure, failures_by_reason}}
    end
  end

  # Private helper functions

  defp execute_get_query(state, racing_key, operation_fn, opts) do
    snapshot = Keyword.get(opts, :snapshot, false)

    state
    |> StorageRacing.race_storage_servers(racing_key, operation_fn)
    |> case do
      {state, {:failure, failures_by_reason}} ->
        {state, {:failure, failures_by_reason}}

      {state, {:ok, {nil, _shard_range}}} ->
        {state, {:error, :not_found}}

      {state, {:ok, {{key, nil}, _shard_range}}} ->
        state =
          if snapshot do
            state
          else
            %{state | tx: Tx.merge_storage_read(state.tx, key, :not_found)}
          end

        {state, {:error, :not_found}}

      {state, {:ok, {{key, value}, _shard_range}}} ->
        state =
          if snapshot do
            state
          else
            %{state | tx: Tx.merge_storage_read(state.tx, key, value)}
          end

        {state, {:ok, {key, value}}}
    end
  end
end
