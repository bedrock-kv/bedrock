defmodule Bedrock.DataPlane.Log.Shale.Server do
  alias Bedrock.DataPlane.Log
  alias Bedrock.DataPlane.Log.Shale.State

  import Bedrock.DataPlane.Log.Shale.Facts, only: [info: 2]
  import Bedrock.DataPlane.Log.Shale.Locking, only: [lock_for_recovery: 3]
  import Bedrock.DataPlane.Log.Shale.Recovery, only: [recover_from: 3]
  import Bedrock.DataPlane.Log.Shale.Pushing, only: [push: 4]
  import Bedrock.DataPlane.Log.Shale.Pulling, only: [pull: 3]

  use GenServer

  @doc false
  @spec child_spec(opts :: keyword() | []) :: Supervisor.child_spec()
  def child_spec(opts) do
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
             otp_name,
             id,
             foreman
           },
           [name: otp_name]
         ]}
    }
  end

  @impl true
  def init({otp_name, id, foreman}) do
    log = :ets.new(:log, [:protected, :ordered_set])

    {:ok,
     %State{
       mode: :starting,
       id: id,
       otp_name: otp_name,
       foreman: foreman,
       log: log,
       oldest_version: 0,
       last_version: 0
     }}
  end

  @impl true
  def handle_call({:info, fact_names}, _from, t),
    do: info(t, fact_names) |> then(&(t |> reply(&1)))

  def handle_call({:lock_for_recovery, epoch}, from, t) do
    with {:ok, t} <- lock_for_recovery(t, epoch, from),
         {:ok, info} <- info(t, Log.recovery_info()) do
      t |> reply({:ok, self(), info})
    else
      error -> t |> reply(error)
    end
  end

  def handle_call({:recover_from, source_log, version_vector}, _, t) do
    recover_from(t, source_log, version_vector)
    |> case do
      {:ok, t} -> t |> reply(:ok)
      {:error, reason} -> t |> reply({:error, {:failed_to_recover, reason}})
    end
  end

  def handle_call({:push, expected_version, transaction}, from, t) do
    push(t, expected_version, transaction, from)
    |> case do
      {:waiting, t} -> t |> noreply()
      {:ok, t} -> t |> reply(:ok)
      {:error, _reason} = error -> t |> reply(error)
    end
  end

  def handle_call({:pull, from_version, opts}, _from, t) do
    pull(t, from_version, opts)
    |> case do
      {:ok, t, transactions} -> t |> reply({:ok, transactions})
      {:error, _reason} = error -> t |> reply(error)
    end
  end

  defp reply(%State{} = t, result), do: {:reply, result, t}
  defp noreply(%State{} = t), do: {:noreply, t}
end
