defmodule Bedrock.Cluster.Gateway.Calls do
  @moduledoc false

  alias Bedrock.Cluster.Gateway.State
  alias Bedrock.Cluster.Gateway.TransactionBuilder
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.ControlPlane.Coordinator
  alias Bedrock.Internal.Time

  @spec begin_transaction(State.t(), opts :: []) ::
          {State.t(), {:ok, pid()} | {:error, :unavailable}}
  def begin_transaction(t, _opts \\ []) do
    t
    |> ensure_current_tsl()
    |> case do
      {:ok, tsl, updated_state} ->
        [
          gateway: self(),
          transaction_system_layout: tsl
        ]
        |> TransactionBuilder.start_link()
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
  @spec renew_read_version_lease(State.t(), read_version :: Bedrock.version(), time_fn :: (-> integer())) ::
          {State.t(),
           {:ok, expiration_interval_in_ms :: Bedrock.interval_in_ms()}
           | {:error, :lease_expired}}
  def renew_read_version_lease(t, read_version, time_fn \\ &Time.monotonic_now_in_ms/0)

  def renew_read_version_lease(t, read_version, _time_fn)
      when not is_nil(t.minimum_read_version) and read_version < t.minimum_read_version,
      do: {t, {:error, :lease_expired}}

  def renew_read_version_lease(t, read_version, time_fn) do
    now = time_fn.()

    t.deadline_by_version
    |> Map.pop(read_version)
    |> case do
      {expiration, deadline_by_version} when expiration <= now ->
        {deadline_by_version, {:error, :lease_expired}}

      {_, deadline_by_version} ->
        renewal_deadline_in_ms = t.lease_renewal_interval_in_ms
        new_lease_deadline_in_ms = now + 10 + renewal_deadline_in_ms

        {Map.put(deadline_by_version, read_version, new_lease_deadline_in_ms), {:ok, renewal_deadline_in_ms}}
    end
    |> then(fn {updated_deadline_by_version, result} ->
      {Map.put(t, :deadline_by_version, updated_deadline_by_version), result}
    end)
  end
end
