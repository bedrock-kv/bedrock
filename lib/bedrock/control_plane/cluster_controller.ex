defmodule Bedrock.ControlPlane.ClusterController do
  @moduledoc """
  The controller is a singleton within the cluster. It is created by the winner
  of the coordinator election. It is responsible for bringing up the data plane
  and putting the cluster into a writable state.
  """
  use GenServer

  alias Bedrock.ControlPlane.DataDistributor
  alias Bedrock.DataPlane.Sequencer

  @type name :: GenServer.name()

  @type t :: %__MODULE__{}
  defstruct [
    :otp_name,
    :cluster,
    :sequencer,
    :data_distributor,
    :coordinator
  ]

  def child_spec(opts) do
    cluster = Keyword.get(opts, :cluster) || raise "Missing :cluster option"
    epoch = Keyword.get(opts, :epoch) || raise "Missing :epoch option"
    coordinator = Keyword.get(opts, :coordinator) || raise "Missing :coordinator option"
    otp_name = opts[:otp_name] || raise "Missing :otp_name option"

    %{
      id: __MODULE__,
      start:
        {GenServer, :start_link,
         [
           __MODULE__,
           {cluster, epoch, coordinator, otp_name},
           [name: otp_name]
         ]},
      restart: :temporary
    }
  end

  @spec join_cluster(name(), node(), [atom()]) :: :ok | {:error, :unavailable}
  def join_cluster(controller, node, services) do
    GenServer.call(controller, {:join_cluster, node, services})
  catch
    :exit, {:noproc, {GenServer, :call, _}} -> {:error, :unavailable}
  end

  @spec get_sequencer(name()) :: {:ok, pid()} | {:error, :unavailable}
  def get_sequencer(controller) do
    GenServer.call(controller, :get_sequencer)
  catch
    :exit, {:noproc, {GenServer, :cast, _}} -> {:error, :unavailable}
  end

  @spec get_data_distributor(name()) :: {:ok, pid()} | {:error, :unavailable}
  def get_data_distributor(controller) do
    GenServer.call(controller, :get_data_distributor)
  catch
    :exit, {:noproc, {GenServer, :cast, _}} -> {:error, :unavailable}
  end

  @doc false
  @impl GenServer
  def init({cluster, epoch, coordinator, otp_name}) do
    with {:ok, sequencer} <-
           Sequencer.start_link(
             controller: self(),
             cluster: cluster,
             epoch: epoch,
             otp_name: cluster.otp_name(:sequencer)
           ),
         {:ok, data_distributor} <-
           DataDistributor.start_link(
             controller: self(),
             otp_name: cluster.otp_name(:data_distributor)
           ) do
      {:ok,
       %__MODULE__{
         cluster: cluster,
         otp_name: otp_name,
         sequencer: sequencer,
         data_distributor: data_distributor,
         coordinator: coordinator
       }, {:continue, :find_and_stop_existing_logs}}
    end
  end

  @impl GenServer
  def handle_continue(:find_and_stop_existing_logs, state) do
    {:noreply, state}
  end

  @impl GenServer
  def handle_call(:get_sequencer, _from, state),
    do: {:reply, {:ok, state.sequencer}, state}

  def handle_call(:get_data_distributor, _from, state),
    do: {:reply, {:ok, state.data_distributor}, state}

  def handle_call({:join_cluster, node, services}, _from, state) do
    IO.inspect({:join_cluster, node, services}, label: "join_cluster")
    {:reply, :ok, state}
  end
end
