defmodule Bedrock.DataPlane.Log.Shale.Server do
  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.Log.Shale.State

  import Bedrock.DataPlane.Log.Shale.Facts, only: [info: 2]
  import Bedrock.DataPlane.Log.Shale.Locking, only: [lock_for_recovery: 3]
  import Bedrock.DataPlane.Log.Shale.Recovery, only: [recover_from: 4]
  import Bedrock.DataPlane.Log.Shale.Pushing, only: [push: 3]
  import Bedrock.DataPlane.Log.Shale.Pulling, only: [pull: 3]

  import Bedrock.DataPlane.Log.Telemetry

  use GenServer

  @doc false
  @spec child_spec(opts :: keyword() | []) :: Supervisor.child_spec()
  def child_spec(opts) do
    cluster = opts[:cluster] || raise "Missing :cluster option"
    otp_name = opts[:otp_name] || raise "Missing :otp_name option"
    id = Keyword.fetch!(opts, :id) || raise "Missing :id option"
    foreman = Keyword.fetch!(opts, :foreman)

    %{
      id: {__MODULE__, id},
      start:
        {GenServer, :start_link,
         [
           __MODULE__,
           {
             cluster,
             otp_name,
             id,
             foreman
           },
           [name: otp_name]
         ]}
    }
  end

  @impl true
  def init({cluster, otp_name, id, foreman}) do
    log = :ets.new(:log, [:protected, :ordered_set])

    {:ok,
     %State{
       cluster: cluster,
       mode: :starting,
       id: id,
       otp_name: otp_name,
       foreman: foreman,
       log: log,
       oldest_version: 0,
       last_version: 0
     }, {:continue, :initialization}}
  end

  @impl true
  def handle_continue(:initialization, t) do
    trace_log_started(t.cluster, t.id, t.otp_name)

    t |> noreply()
  end

  @impl true
  def handle_call({:info, fact_names}, _from, t),
    do: info(t, fact_names) |> then(&(t |> reply(&1)))

  def handle_call({:lock_for_recovery, epoch}, from, t) do
    trace_log_lock_for_recovery(t.cluster, t.id, epoch)

    with {:ok, t} <- lock_for_recovery(t, epoch, from),
         {:ok, info} <- info(t, Log.recovery_info()) do
      t |> reply({:ok, self(), info})
    else
      error -> t |> reply(error)
    end
  end

  def handle_call({:recover_from, source_log, first_version, last_version}, _, t) do
    trace_log_recover_from(t.cluster, t.id, source_log, first_version, last_version)

    case recover_from(t, source_log, first_version, last_version) do
      {:ok, t} -> t |> reply(:ok)
      {:error, reason} -> t |> reply({:error, {:failed_to_recover, reason}})
    end
  end

  def handle_call({:push, transaction, expected_version}, from, t) do
    trace_log_push_transaction(t.cluster, t.id, transaction, expected_version)

    case push(t, expected_version, {transaction, ack_fn(from)}) do
      {:waiting, t} -> t |> noreply()
      {:ok, t} -> t |> reply(:ok)
      {:error, _reason} = error -> t |> reply(error)
    end
  end

  def handle_call({:pull, from_version, opts}, _from, t) do
    trace_log_pull_transactions(t.cluster, t.id, from_version, opts)

    case pull(t, from_version, opts) do
      {:ok, t, transactions} -> t |> reply({:ok, transactions})
      {:error, _reason} = error -> t |> reply(error)
    end
  end

  def ack_fn(from), do: fn -> GenServer.reply(from, :ok) end

  defp reply(%State{} = t, result), do: {:reply, result, t}
  defp noreply(%State{} = t), do: {:noreply, t}
end
