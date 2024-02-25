defmodule Bedrock.ControlPlane.ClusterController do
  @moduledoc """
  The controller is a singleton within the cluster. It is created by the winner
  of the coordinator election. It is responsible for bringing up the data plane
  and putting the cluster into a writable state.
  """
  use GenServer

  alias Bedrock.ControlPlane.DataDistributor
  alias Bedrock.DataPlane.Sequencer

  @type service :: GenServer.name()

  @spec request_to_rejoin(service(), node(), [atom()]) :: :ok | {:error, :unavailable}
  def request_to_rejoin(controller, node, services) do
    GenServer.call(controller, {:request_to_rejoin, node, services})
  catch
    :exit, {:noproc, _} -> {:error, :unavailable}
  end

  @spec get_sequencer(service()) :: {:ok, pid()} | {:error, :unavailable}
  def get_sequencer(controller) do
    GenServer.call(controller, :get_sequencer)
  catch
    :exit, {:noproc, _} -> {:error, :unavailable}
  end

  @spec get_data_distributor(service()) :: {:ok, pid()} | {:error, :unavailable}
  def get_data_distributor(controller) do
    GenServer.call(controller, :get_data_distributor)
  catch
    :exit, {:noproc, _} -> {:error, :unavailable}
  end

  @type t :: %__MODULE__{}
  defstruct [
    :otp_name,
    :cluster,
    :sequencer,
    :data_distributor,
    :coordinator
  ]

  @doc false
  def child_spec(opts) do
    cluster = opts[:cluster] || raise "Missing :cluster param"
    epoch = opts[:epoch] || raise "Missing :epoch param"
    coordinator = opts[:coordinator] || raise "Missing :coordinator param"
    otp_name = opts[:otp_name] || raise "Missing :otp_name param"

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
       }}
    end
  end

  @impl GenServer
  def handle_call(:get_sequencer, _from, state),
    do: {:reply, {:ok, state.sequencer}, state}

  def handle_call(:get_data_distributor, _from, state),
    do: {:reply, {:ok, state.data_distributor}, state}

  def handle_call({:request_to_rejoin, node, services}, _from, state) do
    IO.inspect({:request_to_rejoin, node, services}, label: "request_to_rejoin")
    {:reply, :ok, state}
  end
end
