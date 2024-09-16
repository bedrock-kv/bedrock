defmodule Bedrock.Client.Transaction do
  use GenServer

  alias Bedrock.Service.StorageWorker
  alias Bedrock.Client

  defstruct [:client, :read_version, :commit_proxy, :rx, :wx, :started_at]
  @type t :: %__MODULE__{}

  @spec commit(txn :: pid()) :: :ok | {:error, :transaction_expired}
  def commit(txn),
    do: GenServer.call(txn, :commit)

  @spec get(txn :: pid(), key :: binary()) :: nil | binary()
  def get(txn, key) when is_pid(txn),
    do: GenServer.call(txn, {:get, key})

  @spec put(txn :: pid(), key :: binary(), value :: binary()) :: :ok
  def put(txn, key, value),
    do: GenServer.cast(txn, {:put, key, value})

  @doc false
  def start_link(client, read_version) do
    GenServer.start_link(__MODULE__, {client, read_version})
  end

  @impl GenServer
  def init({client, read_version}) do
    with {:ok, state} <- new_state(client, read_version) do
      {:ok, state, client.transaction_window_in_ms}
    end
  end

  def new_state(client, read_version) do
    {:ok,
     %__MODULE__{
       client: client,
       read_version: read_version,
       commit_proxy: nil,
       rx: %{},
       wx: %{},
       started_at: :erlang.monotonic_time()
     }}
  end

  @impl GenServer
  def handle_call(:commit, _from, state) do
    {:reply, commit(state), state}
  end

  def handle_call({:get, key}, _from, state) do
    {state, value} = state |> do_get(key)
    {:reply, value, state}
  end

  @impl GenServer
  def handle_cast({:put, key, value}, state) do
    {:noreply, state |> do_put(key, value)}
  end

  @impl GenServer
  def handle_info(:timeout, state) do
    {:stop, :normal, state}
  end

  @doc false
  def do_get(state, key) do
    Map.fetch(state.wx, key)
    |> case do
      {:ok, value} ->
        {state, value}

      :error ->
        Map.fetch(state.rx, key)
        |> case do
          {:ok, value} ->
            {state, value}

          :error ->
            value =
              state
              |> storage_workers_for_key(key)
              |> Enum.random()
              |> StorageWorker.get(key, state.read_version)

            {state |> Map.update!(:rx, &Map.put(&1, key, value)), value}
        end
    end
  end

  @doc false
  def do_put(state, key, value) do
    state |> Map.update!(:wx, &Map.put(&1, key, value))
  end

  @doc false
  def do_commit(state) do
    elapsed_in_ms =
      System.convert_time_unit(:erlang.monotonic_time() - state.started_at, :native, :millisecond)

    if elapsed_in_ms > state.client.transaction_window_in_ms do
      {:error, :transaction_expired}
    else
      raise "Not implemented"
      :ok
    end
  end

  @doc false
  @spec storage_workers_for_key(transaction :: t(), key :: binary()) :: [StorageWorker.name()]
  def storage_workers_for_key(%{client: client}, key),
    do: client |> Client.storage_workers_for_key(key)
end
