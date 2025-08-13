defmodule Bedrock.Cluster.Gateway.Calls do
  @moduledoc false

  alias Bedrock.Cluster.Gateway.State
  alias Bedrock.Cluster.Gateway.TransactionBuilder
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.ControlPlane.Coordinator

  @spec begin_transaction(State.t(), opts :: [key_codec: module(), value_codec: module()]) ::
          {State.t(), {:ok, pid()} | {:error, :unavailable}}
  def begin_transaction(t, opts \\ []) do
    t
    |> ensure_current_tsl()
    |> case do
      {:ok, tsl, updated_state} ->
        TransactionBuilder.start_link(
          gateway: self(),
          transaction_system_layout: tsl,
          key_codec: Keyword.fetch!(opts, :key_codec),
          value_codec: Keyword.fetch!(opts, :value_codec)
        )
        |> case do
          {:ok, pid} -> {updated_state, {:ok, pid}}
          {:error, reason} -> {updated_state, {:error, reason}}
        end

      {:error, reason} ->
        {t, {:error, reason}}
    end
  end

  @spec ensure_current_tsl(State.t()) ::
          {:ok, TransactionSystemLayout.t(), State.t()}
          | {:error, :unavailable}
  defp ensure_current_tsl(%{known_coordinator: :unavailable}), do: {:error, :unavailable}

  defp ensure_current_tsl(%{known_coordinator: _coordinator, transaction_system_layout: tsl} = t)
       when not is_nil(tsl) do
    # Use cached TSL (updated via push notifications)
    {:ok, tsl, t}
  end

  defp ensure_current_tsl(%{known_coordinator: coordinator} = t) do
    # No cached TSL - fetch from coordinator
    case Coordinator.fetch_transaction_system_layout(coordinator) do
      {:ok, tsl} -> {:ok, tsl, %{t | transaction_system_layout: tsl}}
      {:error, reason} -> {:error, reason}
    end
  end

  @spec renew_read_version_lease(State.t(), read_version :: Bedrock.version()) ::
          {State.t(),
           {:ok, expiration_interval_in_ms :: Bedrock.interval_in_ms()}
           | {:error, :lease_expired}}
  def renew_read_version_lease(t, read_version)
      when not is_nil(t.minimum_read_version) and read_version < t.minimum_read_version,
      do: {:error, :lease_expired}

  def renew_read_version_lease(t, read_version) do
    now = :erlang.monotonic_time(:millisecond)

    Map.pop(t.deadline_by_version, read_version)
    |> case do
      {expiration, deadline_by_version} when expiration <= now ->
        {deadline_by_version, {:error, :lease_expired}}

      {_, deadline_by_version} ->
        renewal_deadline_in_ms = t.lease_renewal_interval_in_ms
        new_lease_deadline_in_ms = now + 10 + renewal_deadline_in_ms

        {Map.put(deadline_by_version, read_version, new_lease_deadline_in_ms),
         {:ok, renewal_deadline_in_ms}}
    end
    |> then(fn {updated_deadline_by_version, result} ->
      {t |> Map.put(:deadline_by_version, updated_deadline_by_version), result}
    end)
  end
end
