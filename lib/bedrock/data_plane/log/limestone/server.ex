defmodule Bedrock.DataPlane.Log.Limestone.Server do
  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.Log.Limestone.Logic
  alias Bedrock.DataPlane.Log.Limestone.State
  alias Bedrock.DataPlane.Log.Limestone.Subscriptions

  import Bedrock.DataPlane.Log.Limestone.Pulling, only: [pull: 4]
  import Bedrock.DataPlane.Log.Limestone.Pushing, only: [push: 3]
  import Bedrock.DataPlane.Log.Limestone.Facts, only: [info: 2]
  import Bedrock.DataPlane.Log.Limestone.Locking, only: [lock_for_recovery: 3]

  use GenServer

  def child_spec(opts) do
    id = Keyword.fetch!(opts, :id)
    otp_name = Keyword.fetch!(opts, :otp_name)
    foreman = Keyword.fetch!(opts, :foreman)
    transactions = Keyword.fetch!(opts, :transactions)

    %{
      id: __MODULE__,
      start:
        {GenServer, :start_link,
         [
           __MODULE__,
           {id, otp_name, foreman, transactions},
           [name: otp_name]
         ]}
    }
  end

  @impl true
  def init(args),
    # We use a continuation here to ensure that the foreman isn't blocked
    # waiting for the worker to finish it's startup sequence (which could take
    # a few seconds or longer if the transaction log is large.) The
    # foreman will be notified when the worker is ready to accept requests.
    do: {:ok, args, {:continue, :finish_startup}}

  @impl GenServer
  def handle_call({:info, fact_names}, _from, %State{} = t),
    do: info(t, fact_names) |> then(&(t |> reply(&1)))

  def handle_call({:push, transaction, prev_version}, _from, %State{} = t) do
    t
    |> push(transaction, prev_version)
    |> case do
      {:ok, t} -> t |> reply(:ok)
      {:error, _reason} = error -> t |> reply(error)
    end
  end

  def handle_call({:pull, last_version, count, opts}, _from, %State{} = t) do
    t
    |> pull(last_version, count, opts)
    |> case do
      {:ok, []} -> t |> reply({:ok, []})
      {:ok, transactions} -> t |> reply({:ok, transactions})
    end
  end

  def handle_call({:lock_for_recovery, epoch}, foreman, %State{} = t) do
    with {:ok, t} <- lock_for_recovery(t, foreman, epoch),
         {:ok, info} <- info(t, Log.recovery_info()) do
      t |> reply({:ok, self(), info})
    else
      error -> t |> reply(error)
    end
  end

  @impl true
  def handle_continue(:finish_startup, {id, otp_name, foreman, transactions}) do
    %State{
      state: :starting,
      id: id,
      otp_name: otp_name,
      foreman: foreman,
      subscriptions: Subscriptions.new(),
      transactions: transactions,
      oldest_version: 0,
      last_version: 0
    }
    |> report_health_to_foreman()
    |> noreply()
  end

  def report_health_to_foreman(t) do
    :ok = Logic.report_health_to_foreman(t, {:ok, self()})
    t
  end

  defp reply(t, reply), do: {:reply, reply, t}
  defp noreply(t), do: {:noreply, t}
end
