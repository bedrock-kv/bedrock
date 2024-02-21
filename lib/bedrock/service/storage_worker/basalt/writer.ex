defmodule Bedrock.Service.StorageWorker.Basalt.Writer do
  use GenStage
  use Bedrock.Cluster, :types

  alias Bedrock.Service.StorageWorker.Basalt.Database

  defstruct ~w[database storage_engine id loader_task]a
  @type t :: %__MODULE__{}

  @spec stop(writer :: GenStage.server(), reason :: term()) :: :ok
  def stop(writer, reason),
    do: GenStage.stop(writer, reason)

  def start_link(args) do
    id = args[:id] || raise ":id is required"
    database = args[:database] || raise ":database is required"
    storage_engine = args[:storage_engine] || raise ":storage_engine is required"
    otp_name = args[:otp_name] || raise ":otp_name is required"
    GenStage.start_link(__MODULE__, {id, database, storage_engine}, name: otp_name)
  end

  # GenStage callbacks

  @impl GenStage
  def init({id, database, storage_engine}) do
    state =
      %__MODULE__{
        id: id,
        database: database,
        storage_engine: storage_engine
      }
      |> start_loader()

    {:consumer, state}
  end

  @spec start_loader(t()) :: t()
  def start_loader(state) do
    {:ok, loader_task} =
      Task.start_link(fn ->
        Database.load_keys(state.database)
      end)

    %{state | loader_task: loader_task}
  end

  @impl GenStage
  def handle_events(transactions, _from, state) do
    Database.apply_transactions(state.database, transactions)
    {:noreply, [], state}
  end

  @impl GenStage
  def handle_cancel(_cancellation_reason, _from, state) do
    GenServer.cast(state.storage_engine, {:waiting_for_log, state.id})
    {:noreply, [], state}
  end

  @impl GenStage
  def handle_info({_task, :ok}, state) do
    GenServer.cast(state.storage_engine, {:waiting_for_log, state.id})
    {:noreply, [], state}
  end

  @impl GenStage
  def terminate(_reason, state) do
    Database.close(state.database)
    state
  end
end
