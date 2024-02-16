defmodule Bedrock.ControlPlane.DataDistributor do
  use GenServer
  use Bedrock.Cluster, :types

  alias Bedrock.Cluster
  alias Bedrock.ControlPlane.DataDistributor.LogInfoTable
  alias Bedrock.ControlPlane.DataDistributor.TagRanges
  alias Bedrock.ControlPlane.DataDistributor.Team

  require Logger

  defstruct ~w[
    controller
    storage_controller_otp_name
    state
    tag_ranges
    log_info_table
    storage_teams
  ]a
  @type t :: %__MODULE__{}
  @type tag :: integer()

  @spec storage_team_for_key(cluster :: binary(), key()) :: {:ok, Team.t()} | {:error, :not_found}
  def storage_team_for_key(cluster, key) do
    cluster
    |> otp_name()
    |> GenServer.call({:storage_team_for_key, key})
  end

  @spec request_to_rejoin(cluster :: binary()) ::
          :ok | {:error, :unable_to_contact_data_distributor}
  def request_to_rejoin(cluster) do
    cluster
    |> otp_name()
    |> GenServer.call(:request_to_rejoin)
  catch
    :exit, {:noproc, {GenServer, :call, _}} ->
      {:error, :unable_to_contact_data_distributor}
  end

  @spec otp_name(cluster :: binary()) :: atom()
  def otp_name(cluster), do: Cluster.otp_name(cluster, :data_distributor)

  def start_link(opts) do
    controller = opts[:controller] || raise "Missing :controller option"

    storage_controller_otp_name =
      opts[:storage_controller_otp_name] || raise "Missing :storage_controller_otp_name option"

    otp_name = opts[:otp_name] || raise "Missing :otp_name option"
    GenServer.start_link(__MODULE__, {controller, storage_controller_otp_name}, name: otp_name)
  end

  @impl GenServer
  def init({controller, storage_controller_otp_name}) do
    {:ok,
     %__MODULE__{
       controller: controller,
       storage_controller_otp_name: storage_controller_otp_name,
       state: :starting,
       tag_ranges: TagRanges.new(),
       log_info_table: LogInfoTable.new(),
       storage_teams: %{}
     }, {:continue, :startup}}
  end

  @impl GenServer
  def handle_continue(:startup, state) do
    GenServer.multi_call(Node.list(), state.storage_controller_otp_name, :ping, 1_000)
    |> case do
      {replies, unresponsive} ->
        {replies, unresponsive}
    end

    {:noreply, state}
  end

  @impl GenServer
  def handle_call({:storage_team_for_key, key}, _from, state) do
    state.tag_ranges
    |> TagRanges.tag_for_key(key)
    |> case do
      {:ok, tag} -> {:reply, state.storage_teams |> storage_team_for_tag(tag), state}
      {:error, _reason} = error -> {:reply, error, state}
    end
  end

  def handle_call(:request_to_rejoin, storage_engine, state) do
    Logger.notice("DataDistributor: Request to rejoin from #{inspect(storage_engine)}")
    {:reply, :ok, state}
  end

  @spec storage_team_for_tag(map(), any()) :: {:error, :not_found} | {:ok, Team.t()}
  def storage_team_for_tag(storage_teams, tag) do
    storage_teams
    |> Map.get(tag)
    |> case do
      nil -> {:error, :not_found}
      storage_team -> {:ok, storage_team}
    end
  end
end
