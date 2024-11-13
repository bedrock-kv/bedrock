defmodule Bedrock.Cluster.TransactionBuilder.Fetching do
  alias Bedrock.Cluster.Gateway
  alias Bedrock.ControlPlane.Config.ServiceDescriptor
  alias Bedrock.ControlPlane.Config.StorageTeamDescriptor
  alias Bedrock.DataPlane.Storage
  alias Bedrock.Service.Worker
  alias Bedrock.Cluster.TransactionBuilder.State

  import Bedrock.Cluster.TransactionBuilder.KeyEncoding
  import Bedrock.Cluster.TransactionBuilder.ReadVersions, only: [next_read_version: 1]

  @doc false
  @spec do_fetch(State.t(), key :: binary()) :: {State.t(), term()}
  def do_fetch(t, key) do
    {:ok, encoded_key} = encode_key(key)

    with :error <- Map.fetch(t.writes, encoded_key),
         :error <- Map.fetch(t.reads, encoded_key),
         :error <- fetch_from_stack(encoded_key, t.stack),
         {:ok, t, value} <- fetch_from_storage(t, encoded_key) do
      {%{t | reads: Map.put(t.reads, encoded_key, value)}, {:ok, value}}
    else
      {:ok, _value} = ok -> {t, ok}
      :error -> {%{t | reads: Map.put(t.reads, encoded_key, :error)}, :error}
      error -> {t, error}
    end
  end

  @spec fetch_from_stack(Bedrock.key(), [{reads :: map(), writes :: map()}]) ::
          :error | {State.t(), binary()}
  def fetch_from_stack(_, []), do: :error

  def fetch_from_stack(key, [{reads, writes} | stack]) do
    with :error <- Map.fetch(writes, key),
         :error <- Map.fetch(reads, key) do
      fetch_from_stack(key, stack)
    end
  end

  @spec fetch_from_storage(State.t(), key :: binary()) :: {:ok, State.t(), binary()} | :error
  def fetch_from_storage(%{read_version: nil} = t, key) do
    with {:ok, read_version} <- next_read_version(t),
         {:ok, read_version_lease_expiration_in_ms} <-
           Gateway.renew_read_version_lease(t.gateway, read_version) do
      read_version_lease_expiration =
        :erlang.monotonic_time(:millisecond) + read_version_lease_expiration_in_ms

      t
      |> Map.put(:read_version, read_version)
      |> Map.put(:read_version_lease_expiration, read_version_lease_expiration)
      |> fetch_from_storage(key)
    end
  end

  @spec fetch_from_storage(State.t(), key :: binary()) :: {:ok, State.t(), binary()} | :error
  def fetch_from_storage(t, key) do
    determine_storage_server_or_team_for_key(t, key)
    |> case do
      nil ->
        raise "No storage server or team found for key: #{inspect(key)}"

      storage_server when is_pid(storage_server) ->
        Storage.fetch(storage_server, key, t.read_version, timeout_in_ms: t.fetch_timeout_in_ms)
        |> case do
          {:ok, value} -> {:ok, t, value}
          error -> error
        end

      storage_team when is_map(storage_team) ->
        fetch_from_storage_team(t, storage_team, key)
    end
  end

  @spec fetch_from_storage_team(State.t(), StorageTeamDescriptor.t(), key :: binary()) ::
          {:ok, State.t(), binary()} | :error
  def fetch_from_storage_team(t, storage_team, key) do
    t.transaction_system_layout.services
    |> resolve_storage_ids_to_pids(storage_team.storage_ids)
    |> horse_race_storage_servers_for_key(t.read_version, key, t.fetch_timeout_in_ms)
    |> case do
      {:ok, storage_server, value} ->
        {:ok,
         %{
           t
           | storage_servers: Map.put(t.storage_servers, storage_team.key_range, storage_server)
         }, value}

      error ->
        error
    end
  end

  @spec determine_storage_server_or_team_for_key(State.t(), key :: binary()) ::
          nil | pid() | StorageTeamDescriptor.t()
  def determine_storage_server_or_team_for_key(t, key) do
    selected_storage_server_for_key(t.storage_servers, key) ||
      storage_team_for_key(t.transaction_system_layout.storage_teams, key)
  end

  @spec selected_storage_server_for_key(%{Bedrock.key_range() => pid()}, key :: binary()) ::
          pid() | nil
  def selected_storage_server_for_key(storage_servers, key) do
    Enum.find_value(storage_servers, fn
      {{min_key, max_key_exclusive}, storage_server}
      when min_key <= key and key < max_key_exclusive ->
        storage_server

      _ ->
        nil
    end)
  end

  @spec storage_team_for_key([StorageTeamDescriptor.t()], key :: binary()) ::
          StorageTeamDescriptor.t() | nil
  def storage_team_for_key(storage_teams, key) do
    Enum.find_value(storage_teams, fn
      %{key_range: {min_key, max_key_exclusive}} = storage_team
      when min_key <= key and key < max_key_exclusive ->
        storage_team

      _ ->
        nil
    end)
  end

  @spec resolve_storage_ids_to_pids(%{Worker.id() => ServiceDescriptor.t()}, [Storage.id()]) ::
          [pid()]
  def resolve_storage_ids_to_pids(services, storage_ids) do
    services
    |> Map.take(storage_ids)
    |> Enum.map(fn
      {_storage_id, %{status: {:up, pid}}} -> pid
      _ -> nil
    end)
    |> Enum.reject(&is_nil/1)
  end

  @doc """
  Performs a "horse race" across multiple storage servers to fetch the value
  for a given key. Each storage server is queried in parallel, and the first
  successful response is returned. If none of the servers return a value
  within the specified timeout, `:error` is returned.

  ## Parameters
  - `storage_servers`: A list of PIDs representing the storage servers to
    query.
  - `read_version`: The read version used to ensure consistency.
  - `key`: The binary key for which to fetch the value.
  - `fetch_timeout_in_ms`: The maximum time in milliseconds to wait for each
    server's response.

  ## Returns
  - `{:ok, pid(), binary()}` if a storage server returns a value successfully.
  - `:error` if no storage server returns a value within the specified
    timeout.
  """
  @spec horse_race_storage_servers_for_key(
          storage_servers :: [pid()],
          read_version :: non_neg_integer(),
          key :: binary(),
          fetch_timeout_in_ms :: pos_integer()
        ) :: {:ok, pid(), binary()} | :error
  def horse_race_storage_servers_for_key([], _, _, _), do: :error

  def horse_race_storage_servers_for_key(
        storage_servers,
        read_version,
        key,
        fetch_timeout_in_ms
      ) do
    storage_servers
    |> Task.async_stream(
      fn storage_server ->
        Storage.fetch(storage_server, key, read_version, timeout_in_ms: fetch_timeout_in_ms)
        |> case do
          {:ok, value} -> {storage_server, value}
          error -> error
        end
      end,
      ordered: false,
      timeout: fetch_timeout_in_ms
    )
    |> Enum.find_value(fn
      {:ok, {:error, :version_too_old} = error} -> error
      {:ok, {:error, :not_found} = error} -> error
      {:ok, {storage_server, value}} -> {:ok, storage_server, value}
      _ -> nil
    end) || :error
  end
end
