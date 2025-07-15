defmodule Bedrock.Cluster.Gateway.Calls do
  alias Bedrock.Cluster.Gateway.State
  alias Bedrock.Cluster.Gateway.TransactionBuilder
  alias Bedrock.DataPlane.Sequencer

  @spec begin_transaction(State.t(), opts :: [key_codec: module()]) :: {:ok, pid()}
  def begin_transaction(t, opts \\ []) do
    TransactionBuilder.start_link(
      gateway: self(),
      storage_table: t.storage_table,
      key_codec: Keyword.fetch!(opts, :key_codec),
      value_codec: Keyword.fetch!(opts, :value_codec)
    )
  end

  @spec next_read_version(State.t()) ::
          {:ok, {Bedrock.version(), Bedrock.interval_in_ms()}}
          | {:error, :unavailable}
  def next_read_version(t) when is_nil(t.transaction_system_layout), do: {:error, :unavailable}

  def next_read_version(t) do
    case Sequencer.next_read_version(t.transaction_system_layout.sequencer) do
      {:ok, version} -> {:ok, version, t.lease_renewal_interval_in_ms}
      {:error, _} -> {:error, :unavailable}
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

  @spec fetch_coordinator(State.t()) :: {:ok, GenServer.server()} | {:error, :unavailable}
  def fetch_coordinator(%{coordinator: :unavailable}), do: {:error, :unavailable}
  def fetch_coordinator(t), do: {:ok, t.coordinator}

  @spec fetch_director(State.t()) :: {:ok, GenServer.server()} | {:error, :unavailable}
  def fetch_director(%{director: :unavailable}), do: {:error, :unavailable}
  def fetch_director(t), do: {:ok, t.director}

  @spec fetch_commit_proxy(State.t()) :: {:ok, GenServer.server()} | {:error, :unavailable}
  def fetch_commit_proxy(%{director: :unavailable}), do: {:error, :unavailable}

  def fetch_commit_proxy(%{transaction_system_layout: nil}), do: {:error, :unavailable}

  def fetch_commit_proxy(%{transaction_system_layout: %{proxies: []}}), do: {:error, :unavailable}

  def fetch_commit_proxy(t), do: {:ok, t.transaction_system_layout.proxies |> Enum.random()}

  @spec fetch_coordinator_nodes(State.t()) :: {:ok, [node()]}
  def fetch_coordinator_nodes(t), do: {:ok, t.descriptor.coordinator_nodes}
end
