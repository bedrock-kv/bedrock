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

  @type t :: %__MODULE__{
          cluster: Module.t(),
          sequencer: Sequencer.name(),
          data_distributor: DataDistributor.name(),
          coordinator: String.t()
        }
  defstruct [
    :otp_name,
    :cluster,
    :sequencer,
    :data_distributor,
    :coordinator,
    :log_system_controller_otp_name,
    :storage_controller_otp_name
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

  @spec get_sequencer(service()) :: pid() | {:error, :unavailable}
  def get_sequencer(controller) do
    GenServer.call(controller, :get_sequencer)
  catch
    :exit, {:noproc, {GenServer, :cast, _}} ->
      {:error, :unavailable}
  end

  @spec get_data_distributor(service()) :: pid() | {:error, :unavailable}
  def get_data_distributor(controller) do
    GenServer.call(controller, :get_data_distributor)
  catch
    :exit, {:noproc, {GenServer, :cast, _}} ->
      {:error, :unavailable}
  end

  @spec report_for_duty(service(), subsystem :: atom(), subsystem_controller :: GenServer.name()) ::
          :ok | {:error, :unavailable}
  def report_for_duty(controller, subsystem, subsystem_controller),
    do: GenServer.cast(controller, {:report_for_duty, subsystem, subsystem_controller})

  @impl GenServer
  def init({cluster, epoch, coordinator, otp_name}) do
    log_system_controller_otp_name = cluster.otp_name(:log_system_controller)
    storage_controller_otp_name = cluster.otp_name(:storage_system_controller)

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
             storage_controller_otp_name: storage_controller_otp_name,
             otp_name: cluster.otp_name(:data_distributor)
           ) do
      {:ok,
       %__MODULE__{
         cluster: cluster,
         otp_name: otp_name,
         sequencer: sequencer,
         data_distributor: data_distributor,
         coordinator: coordinator,
         log_system_controller_otp_name: log_system_controller_otp_name,
         storage_controller_otp_name: storage_controller_otp_name
       }, {:continue, :find_and_stop_existing_logs}}
    end
  end

  @impl GenServer
  def handle_continue(:find_and_stop_existing_logs, state) do
    {responses, failing_nodes} =
      GenServer.multi_call(
        Node.list(),
        state.log_system_controller_otp_name,
        {:cluster_controller_replaced, {state.otp_name, Node.self()}}
      )

    IO.inspect({responses, failing_nodes})

    {:noreply, state}
  end

  @impl GenServer
  def handle_call(:get_sequencer, _from, state),
    do: {:reply, state.sequencer, state}

  def handle_call(:get_data_distributor, _from, state),
    do: {:reply, state.data_distributor, state}

  @impl GenServer
  def handle_cast({:report_for_duty, subsystem, _subsystem_controller}, state) do
    case subsystem do
      :log_system ->
        {:noreply, state}

      :storage_system ->
        {:noreply, state}
    end
  end
end
