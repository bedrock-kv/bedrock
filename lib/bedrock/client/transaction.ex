defmodule Bedrock.Client.Transaction do
  alias Bedrock.DataPlane.CommitProxy
  alias Bedrock.DataPlane.Sequencer
  alias Bedrock.DataPlane.Storage
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout

  import Bedrock.Internal.GenServer.Calls

  @opaque t :: pid()

  def default_timeout_in_ms(), do: 1_000

  @spec commit(t(), opts :: [timeout_in_ms :: pos_integer()]) :: :ok | {:error, :aborted}
  def commit(t, opts \\ []),
    do: call(t, :commit, opts[:timeout_in_ms] || default_timeout_in_ms())

  @spec cancel(t()) :: :ok
  def cancel(t),
    do: :ok = GenServer.stop(t, :normal)

  @spec get(t(), key :: binary()) :: nil | binary()
  def get(t, key) when is_binary(key),
    do: call(t, {:get, key}, :infinity)

  @spec put(t(), key :: binary(), value :: binary()) :: :ok
  def put(t, key, value),
    do: cast(t, {:put, key, value})

  @doc false
  @spec start_link(TransactionSystemLayout.t()) :: {:ok, t()} | {:error, term()}
  def start_link(transaction_system_layout) do
    GenServer.start_link(__MODULE__.Impl, transaction_system_layout)
  end

  defmodule Impl do
    alias Bedrock.ControlPlane.Config.ServiceDescriptor
    alias Bedrock.ControlPlane.Config.StorageTeamDescriptor
    alias Bedrock.DataPlane.Storage

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
    def init(transaction_system_layout),
      do: {:ok, transaction_system_layout, {:continue, :initialization}}

    @impl true
    def handle_continue(:initialization, transaction_system_layout) do
      %__MODULE__{
        transaction_system_layout: transaction_system_layout
      }
      |> noreply()
    end

    @impl true
    def handle_call(:commit, _from, t) do
      t
      |> do_commit()
      |> case do
        {:ok, t} -> t |> reply(:ok)
        {:error, _reason} = error -> t |> reply(error)
      end
    end

    def handle_call({:get, key}, _from, t) do
      t
      |> do_get(key)
      |> case do
        {t, value} -> t |> reply(value)
      end
    end

    @impl true
    def handle_cast({:put, key, value}, t),
      do: t |> do_put(key, value) |> noreply()

    @impl true
    def handle_info(:timeout, t), do: {:stop, :normal, t}

    @doc false
    @spec do_get(t(), key :: binary()) :: {t(), term()}
    def do_get(t, key) do
      {:ok, encoded_key} = encode_key(key)

      with :error <- Map.fetch(t.writes, encoded_key),
           :error <- Map.fetch(t.reads, encoded_key),
           :error <- fetch_from_stack(encoded_key, t.stack),
           {:ok, t, value} <-
             fetch_from_storage(t, encoded_key) do
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

    def fetch_from_storage_team(t, storage_team, key) do
      t.transaction_system_layout
      |> fetch_live_storage_servers(storage_team.storage_ids)
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
      Enum.find_value(t.storage_servers, fn
        {{min_key, max_key_exclusive}, storage_server}
        when min_key <= key and key < max_key_exclusive ->
          storage_server

        _ ->
          nil
      end)
      |> case do
        nil ->
          Enum.find_value(t.transaction_system_layout.storage_teams, fn
            %{key_range: {min_key, max_key_exclusive}} = storage_team
            when min_key <= key and key < max_key_exclusive ->
              storage_team

            _ ->
              nil
          end)

        storage_server ->
          storage_server
      end
    end

    def fetch_live_storage_servers(%{services: services}, storage_ids) do
      storage_ids
      |> Enum.map(&ServiceDescriptor.find_pid_by_id(services, &1))
      |> Enum.reject(&is_nil/1)
    end

    @spec horse_race_storage_servers_for_key(
            storage_servers :: [pid()],
            read_version :: non_neg_integer(),
            key :: binary(),
            fetch_timeout_in_ms :: pos_integer()
          ) :: {:ok, pid(), binary()} | :error
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
        max_concurrency: length(storage_servers),
        timeout: fetch_timeout_in_ms
      )
      |> Enum.find_value(fn
        {:ok, {storage_server, value}} -> {:ok, storage_server, value}
        _ -> nil
      end)
    end

    def next_read_version(t),
      do: Sequencer.next_read_version(t.transaction_system_layout.sequencer)

    def encode_key(key) when is_binary(key), do: {:ok, key}
    def encode_key(_key), do: :key_error
  end
end
