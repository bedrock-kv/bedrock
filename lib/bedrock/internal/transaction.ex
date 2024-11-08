defmodule Bedrock.Internal.Transaction do
  alias Bedrock.DataPlane.CommitProxy
  alias Bedrock.DataPlane.Sequencer
  alias Bedrock.DataPlane.Storage
  alias Bedrock.ControlPlane.Config.ServiceDescriptor
  alias Bedrock.ControlPlane.Config.StorageTeamDescriptor
  alias Bedrock.DataPlane.Storage
  alias Bedrock.Service.Worker

  @doc false
  @spec start_link(cluster :: module(), opts :: keyword()) :: {:ok, pid()} | {:error, term()}
  def start_link(cluster, opts),
    do: GenServer.start_link(__MODULE__, {cluster.config!().transaction_system_layout, opts})

  @type t :: %__MODULE__{
          read_version: Bedrock.version() | nil,
          reads: %{},
          writes: %{},
          stack: [%{reads: map(), writes: map()}],
          transaction_system_layout: map(),
          storage_servers: map(),
          fetch_timeout_in_ms: pos_integer()
        }
  defstruct read_version: nil,
            reads: %{},
            writes: %{},
            stack: [],
            transaction_system_layout: nil,
            storage_servers: %{},
            fetch_timeout_in_ms: 50

  use GenServer
  import Bedrock.Internal.GenServer.Replies

  @impl true
  def init({transaction_system_layout, _opts}),
    do: {:ok, transaction_system_layout, {:continue, :initialization}}

  @impl true
  def handle_continue(:initialization, transaction_system_layout) do
    %__MODULE__{
      transaction_system_layout: transaction_system_layout
    }
    |> noreply()
  end

  @impl true
  def handle_call(:nested_transaction, _from, t),
    do:
      %{t | stack: [{t.reads, t.writes} | t.stack], reads: %{}, writes: %{}}
      |> reply(:ok)

  def handle_call(:commit, _from, t) do
    case do_commit(t) do
      {:ok, t} -> t |> reply(:ok)
      {:error, _reason} = error -> t |> reply(error)
    end
  end

  def handle_call({:get, key}, _from, t) do
    case do_get(t, key) do
      {t, value} -> t |> reply(value)
    end
  end

  @impl true
  def handle_cast({:put, key, value}, t),
    do: t |> do_put(key, value) |> noreply()

  def handle_cast(:rollback, t) do
    case do_rollback(t) do
      :stop -> {:stop, :normal, nil}
      t -> t |> noreply()
    end
  end

  @impl true
  def handle_info(:timeout, t), do: {:stop, :normal, t}

  @doc false
  @spec do_get(t(), key :: binary()) :: {t(), term()}
  def do_get(t, key) do
    {:ok, encoded_key} = encode_key(key)

    with :error <- Map.fetch(t.writes, encoded_key),
         :error <- Map.fetch(t.reads, encoded_key),
         :error <- fetch_from_stack(encoded_key, t.stack),
         {:ok, t, value} <- fetch_from_storage(t, encoded_key) do
      {%{t | reads: Map.put(t.reads, encoded_key, value)}, value}
    else
      {:ok, value} -> {t, value}
      :error -> {%{t | reads: Map.put(t.reads, encoded_key, nil)}, nil}
      error -> {t, error}
    end
  end

  @doc false
  def do_put(t, key, value) do
    with {:ok, encoded_key} <- encode_key(key) do
      %{t | writes: Map.put(t.writes, encoded_key, value)}
    end
  end

  @doc false
  def do_commit(%{stack: []} = t) do
    with commit_proxy <- t.transaction_system_layout.proxies |> Enum.random(),
         {:ok, _version} <-
           CommitProxy.commit(
             commit_proxy,
             {t.read_version, t.reads, t.writes}
           ) do
      {:ok, t}
    end
  end

  def do_commit(%{stack: [{reads, writes} | stack]} = t) do
    {:ok,
     %{
       t
       | reads: Map.merge(t.reads, reads),
         writes: Map.merge(t.writes, writes),
         stack: stack
     }}
  end

  def do_rollback(%{stack: []}), do: :stop
  def do_rollback(%{stack: [_ | stack]} = t), do: %{t | stack: stack}

  @spec fetch_from_stack(Bedrock.key(), [{reads :: map(), writes :: map()}]) ::
          :error | {t(), binary()}
  def fetch_from_stack(_, []), do: :error

  def fetch_from_stack(key, [{reads, writes} | stack]) do
    with :error <- Map.fetch(writes, key),
         :error <- Map.fetch(reads, key) do
      fetch_from_stack(key, stack)
    end
  end

  @spec fetch_from_storage(t(), key :: binary()) :: {:ok, t(), binary()} | :error
  def fetch_from_storage(%{read_version: nil} = t, key) do
    with {:ok, read_version} <- next_read_version(t) do
      %{t | read_version: read_version}
      |> fetch_from_storage(key)
    end
  end

  @spec fetch_from_storage(t(), key :: binary()) :: {:ok, t(), binary()} | :error
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

  @spec fetch_from_storage_team(t(), StorageTeamDescriptor.t(), key :: binary()) ::
          {:ok, t(), binary()} | :error
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

  @spec determine_storage_server_or_team_for_key(t(), key :: binary()) ::
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
      %{status: {:up, pid}} -> pid
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

  def next_read_version(t),
    do: Sequencer.next_read_version(t.transaction_system_layout.sequencer)

  def encode_key(key) when is_binary(key), do: {:ok, key}
  def encode_key(_key), do: :key_error
end
