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
        storage_get_key_fn = Keyword.get(opts, :storage_get_key_fn, &Storage.get/4)

        t
        |> ensure_read_version!(opts)
        |> execute_get_query(
          key,
          &case storage_get_key_fn.(&1, key, &2, timeout: &3) do
            {:ok, raw_value} -> {:ok, {key, raw_value}}
            {:error, reason} -> {:error, reason}
            {:failure, reason, storage_id} -> {:failure, reason, storage_id}
          end,
          opts
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
    storage_get_key_selector_fn = Keyword.get(opts, :storage_get_key_selector_fn, &Storage.get/4)

    t
    |> ensure_read_version!(opts)
    |> execute_get_query(
      key_selector.key,
      &case storage_get_key_selector_fn.(&1, key_selector, &2, timeout: &3) do
        {:ok, nil} -> {:ok, nil}
        {:ok, {resolved_key, value}} -> {:ok, {resolved_key, value}}
        {:error, reason} -> {:error, reason}
        {:failure, reason, storage_id} -> {:failure, reason, storage_id}
      end,
      opts
    )
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
