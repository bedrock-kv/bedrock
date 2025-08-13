defmodule Bedrock.Cluster.Gateway.TransactionBuilder.ReadVersions do
  @moduledoc false

  alias Bedrock.Cluster.Gateway
  alias Bedrock.Cluster.Gateway.TransactionBuilder.State
  alias Bedrock.DataPlane.Sequencer
  alias Bedrock.Internal.Time

  @type sequencer_fn() :: (pid() -> {:ok, Bedrock.version()} | {:error, atom()})
  @type gateway_fn() :: (pid(), Bedrock.version() ->
                           {:ok, Bedrock.interval_in_ms()} | {:error, atom()})
  @type time_fn() :: (-> integer())

  @spec next_read_version(State.t()) ::
          {:ok, Bedrock.version(), Bedrock.interval_in_ms()}
          | {:error, :unavailable | :timeout | :unknown}
  @spec next_read_version(State.t(), opts) ::
          {:ok, Bedrock.version(), Bedrock.interval_in_ms()}
          | {:error, :unavailable | :timeout | :unknown}
        when opts: [
               sequencer_fn: sequencer_fn(),
               gateway_fn: gateway_fn()
             ]
  def next_read_version(t, opts \\ []) do
    sequencer_fn = Keyword.get(opts, :sequencer_fn, &Sequencer.next_read_version/1)
    gateway_fn = Keyword.get(opts, :gateway_fn, &Gateway.renew_read_version_lease/2)

    with {:ok, read_version} <-
           sequencer_fn.(t.transaction_system_layout.sequencer),
         {:ok, lease_deadline_ms} <-
           gateway_fn.(t.gateway, read_version) do
      {:ok, read_version, lease_deadline_ms}
    end
  end

  @spec renew_read_version_lease(State.t()) ::
          {:ok, State.t()} | {:error, :read_version_lease_expired}
  @spec renew_read_version_lease(State.t(), opts) ::
          {:ok, State.t()} | {:error, :read_version_lease_expired}
        when opts: [
               gateway_fn: gateway_fn(),
               time_fn: time_fn()
             ]
  def renew_read_version_lease(t, opts \\ []) do
    gateway_fn = Keyword.get(opts, :gateway_fn, &Gateway.renew_read_version_lease/2)
    time_fn = Keyword.get(opts, :time_fn, &Time.monotonic_now_in_ms/0)

    with {:ok, lease_will_expire_in_ms} <-
           gateway_fn.(t.gateway, t.read_version) do
      now = time_fn.()
      {:ok, %{t | read_version_lease_expiration: now + lease_will_expire_in_ms}}
    end
  end
end
