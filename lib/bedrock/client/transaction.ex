defmodule Bedrock.Client.Transaction do
  alias Bedrock.DataPlane.CommitProxy
  alias Bedrock.DataPlane.Sequencer
  alias Bedrock.DataPlane.Storage

  @type t :: pid()

  @spec commit(txn :: pid()) :: :ok | {:error, :aborted}
  def commit(txn),
    do: GenServer.call(txn, :commit)

  @spec cancel(txn :: pid()) :: :ok
  def cancel(txn),
    do: :ok = GenServer.stop(txn, :normal)

  @spec get(txn :: pid(), key :: binary()) :: nil | binary()
  def get(txn, key) when is_pid(txn),
    do: GenServer.call(txn, {:get, key})

  @spec put(txn :: pid(), key :: binary(), value :: binary()) :: :ok
  def put(txn, key, value),
    do: GenServer.cast(txn, {:put, key, value})

  @doc false
  def start_link(_client, config) do
    GenServer.start_link(__MODULE__.Impl, {config})
  end

  defmodule Impl do
    use GenServer

    alias Bedrock.DataPlane.Storage

    defstruct [:read_version, :commit_proxy, :rx, :wx]

    @type t :: %__MODULE__{}

    @impl GenServer
    def init({config}) do
      with {:ok, read_version} <-
             Sequencer.next_read_version(config.transaction_system_layout.sequencer),
           commit_proxy <- Enum.random(config.transaction_system_layout.proxies),
           {:ok, t} <- new_t(read_version, commit_proxy) do
        {:ok, t}
      end
    end

    def new_t(read_version, commit_proxy) do
      {:ok,
       %__MODULE__{
         read_version: read_version,
         commit_proxy: commit_proxy,
         rx: %{},
         wx: %{}
       }}
    end

    @impl GenServer
    def handle_call(:commit, _from, t) do
      t
      |> do_commit()
      |> case do
        {:ok, t} -> {:reply, :ok, t}
        {:error, _reason} = error -> {:reply, error, t}
      end
    end

    def handle_call({:get, key}, _from, t) do
      {t, value} = t |> do_get(key)
      {:reply, value, t}
    end

    @impl GenServer
    def handle_cast({:put, key, value}, t) do
      {:noreply, t |> do_put(key, value)}
    end

    @impl GenServer
    def handle_info(:timeout, t) do
      {:stop, :normal, t}
    end

    @doc false
    def do_get(t, key) do
      Map.fetch(t.wx, key)
      |> case do
        {:ok, value} ->
          {t, value}

        :error ->
          Map.fetch(t.rx, key)
          |> case do
            {:ok, value} ->
              {t, value}

            :error ->
              value =
                t
                |> storage_workers_for_key(key)
                |> Enum.random()
                |> Storage.fetch(key, t.read_version)

              {t |> Map.update!(:rx, &Map.put(&1, key, value)), value}
          end
      end
    end

    @doc false
    def do_put(t, key, value) do
      t |> Map.update!(:wx, &Map.put(&1, key, value))
    end

    @doc false
    def do_commit(t) do
      with {:ok, _version} <- CommitProxy.commit(t.commit_proxy, {t.read_version, t.rx, t.wx}) do
        {:ok, t}
      end
    end

    @doc false
    @spec storage_workers_for_key(transaction :: t(), Bedrock.key()) :: [Storage.ref()]
    def storage_workers_for_key(%{client: _client}, _key),
      do: []
  end
end
