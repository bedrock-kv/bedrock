defmodule Bedrock.Cluster.Gateway.TransactionBuilder.ReadVersions do
  @moduledoc false

  alias Bedrock.Cluster.Gateway
  alias Bedrock.Cluster.Gateway.TransactionBuilder.State

  @spec next_read_version(State.t()) ::
          {:ok, Bedrock.version(), Bedrock.interval_in_ms()}
          | {:error, :unavailable | :timeout | :unknown}
  def next_read_version(t), do: Gateway.next_read_version(t.gateway)

  @spec renew_read_version_lease(State.t()) ::
          {:ok, State.t()} | {:error, :read_version_lease_expired}
  def renew_read_version_lease(t) do
    with {:ok, lease_will_expire_in_ms} <-
           Gateway.renew_read_version_lease(t.gateway, t.read_version) do
      now = :erlang.monotonic_time(:millisecond)
      {:ok, %{t | read_version_lease_expiration: now + lease_will_expire_in_ms}}
    end
  end
end
