defmodule Bedrock.Cluster.TransactionBuilder.ReadVersions do
  alias Bedrock.Cluster.TransactionBuilder.State
  alias Bedrock.Cluster.Gateway

  @spec next_read_version(State.t()) ::
          {:ok, Bedrock.version(), Bedrock.interval_in_ms()} | {:error, :unavailable}
  def next_read_version(t), do: Gateway.next_read_version(t.gateway)

  @spec renew_read_version_lease(State.t()) ::
          {:ok, State.t()} | {:error, :read_version_lease_expired}
  def renew_read_version_lease(t) do
    with {:ok, lease_will_expire_in_ms} <-
           Gateway.renew_read_version_lease(t.gateway, t.read_version) do
      now = :erlang.monotonic_time(:millisecond)
      %{t | read_version_lease_expiration: now + lease_will_expire_in_ms}
    end
  end
end
