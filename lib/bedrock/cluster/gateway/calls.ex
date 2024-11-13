defmodule Bedrock.Cluster.Gateway.Calls do
  alias Bedrock.Cluster.Gateway.State
  alias Bedrock.Cluster.TransactionBuilder
  alias Bedrock.ControlPlane.Director

  @spec begin_transaction(State.t(), opts :: keyword()) :: {:ok, pid()}
  def begin_transaction(t, _opts \\ []) do
    with {:ok, transaction_system_layout} <-
           Director.fetch_transaction_system_layout(t.director, 100),
         {:ok, txn} <-
           TransactionBuilder.start_link(
             gateway: self(),
             transaction_system_layout: transaction_system_layout
           ) do
      {:ok, txn}
    end
  end

  @spec renew_read_version_lease(State.t(), read_version :: Bedrock.version()) ::
          {State.t(),
           {:ok, expiration_interval_in_ms :: Bedrock.interval_in_ms()}
           | {:error, :lease_expired}}
  def renew_read_version_lease(t, read_version)
      when not is_nil(t.minimum_read_version) and t.minimum_read_version > read_version,
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

  def fetch_coordinator(%{coordinator: :unavailable}), do: {:error, :unavailable}
  def fetch_coordinator(t), do: {:ok, t.coordinator}

  def fetch_director(%{director: :unavailable}), do: {:error, :unavailable}
  def fetch_director(t), do: {:ok, t.director}

  def fetch_coordinator_nodes(t), do: {:ok, t.descriptor.coordinator_nodes}
end
