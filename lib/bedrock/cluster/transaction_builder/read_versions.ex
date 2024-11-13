defmodule Bedrock.Cluster.TransactionBuilder.ReadVersions do
  alias Bedrock.Cluster.TransactionBuilder.State
  alias Bedrock.Cluster.Gateway
  alias Bedrock.DataPlane.Sequencer

  @spec next_read_version(State.t()) ::
          {:ok, Bedrock.version()}
  def next_read_version(t), do: Sequencer.next_read_version(t.transaction_system_layout.sequencer)

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
