defmodule Bedrock.DataPlane.Log.Limestone.Server do
  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.Log.Limestone.Logic
  alias Bedrock.DataPlane.Log.Limestone.State
  alias Bedrock.DataPlane.Log.Limestone.Subscriptions
  alias Bedrock.Service.LogController

  import Bedrock.DataPlane.Log.Limestone.Pulling, only: [pull: 4]
  import Bedrock.DataPlane.Log.Limestone.Pushing, only: [push: 3]
  import Bedrock.DataPlane.Log.Limestone.Facts, only: [info: 2]
  import Bedrock.DataPlane.Log.Limestone.Locking, only: [lock_for_recovery: 3]

  use GenServer

  def child_spec(opts) do
    id = Keyword.fetch!(opts, :id)
    otp_name = Keyword.fetch!(opts, :otp_name)
    controller = Keyword.fetch!(opts, :controller)
    transactions = Keyword.fetch!(opts, :transactions)

    %{
      id: __MODULE__,
      start:
        {GenServer, :start_link,
         [
           __MODULE__,
           {id, otp_name, controller, transactions},
           [name: otp_name]
         ]}
    }
  end

  @spec report_health_to_transaction_log_controller(State.t(), Log.health()) :: :ok
  def report_health_to_transaction_log_controller(t, health),
    do: :ok = LogController.report_health(t.controller, t.id, health)

  @impl true
  def init(args),
    # We use a continuation here to ensure that the controller isn't blocked
    # waiting for the worker to finish it's startup sequence (which could take
    # a few seconds or longer if the transaction log is large.) The
    # controller will be notified when the worker is ready to accept requests.
    do: {:ok, args, {:continue, :finish_startup}}

  @impl GenServer
  def handle_call({:info, fact_names}, _from, %State{} = t),
    do: {:reply, info(t, fact_names), t}

  def handle_call({:push, transaction, prev_version}, _from, %State{} = t) do
    push(t, transaction, prev_version)
    |> case do
      {:ok, t} -> {:reply, :ok, t}
      {:error, _reason} = error -> {:reply, error, t}
    end
  end

  def handle_call({:pull, last_version, count, opts}, _from, %State{} = t) do
    pull(t, last_version, count, opts)
    |> case do
      {:ok, []} -> {:reply, {:ok, []}, t}
      {:ok, transactions} -> {:reply, {:ok, transactions}, t}
    end
  end

  def handle_call({:lock_for_recovery, epoch}, controller, %State{} = t) do
    with {:ok, t} <- lock_for_recovery(t, controller, epoch),
         {:ok, info} <- info(t, Log.recovery_info()) do
      {:reply, {:ok, self(), info}, t}
    else
      error -> {:reply, error, t}
    end
  end

  @impl true
  def handle_continue(:finish_startup, {id, otp_name, controller, transactions}) do
    {:ok,
     %State{
       state: :starting,
       id: id,
       otp_name: otp_name,
       controller: controller,
       subscriptions: Subscriptions.new(),
       transactions: transactions,
       oldest_version: :initial,
       last_version: :initial
     }}
    |> case do
      {:ok, t} -> {:noreply, t, {:continue, :report_health_to_controller}}
    end
  end

  def handle_continue(:report_health_to_controller, %State{} = t) do
    :ok = Logic.report_health_to_transaction_log_controller(t, :ok)
    {:noreply, t}
  end
end
