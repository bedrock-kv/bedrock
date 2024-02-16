defmodule Bedrock.DataPlane.Sequencer do
  use GenServer
  alias Bedrock.Cluster

  defstruct [:cluster_name, :controller, epoch: 0, latest_committed_version: <<>>]

  @spec otp_name(binary()) :: atom()
  def otp_name(cluster_name),
    do: Cluster.otp_name(cluster_name, :sequencer)

  def start_link(opts) do
    cluster_name = Keyword.get(opts, :cluster_name) || raise "Missing :cluster_name option"

    controller =
      Keyword.get(opts, :controller) || raise "Missing :controller option"

    epoch = Keyword.get(opts, :epoch, 0)

    GenServer.start_link(__MODULE__, {cluster_name, controller, epoch},
      name: otp_name(cluster_name)
    )
  end

  @impl GenServer
  def init({cluster_name, controller, epoch}) do
    {:ok,
     %__MODULE__{
       cluster_name: cluster_name,
       controller: controller,
       epoch: epoch
     }}
  end

  @impl GenServer
  def handle_call(:next_read_version, _from, state),
    do: {:reply, {:ok, state.latest_committed_version}, state}

  @impl GenServer
  def handle_info(:die, _state), do: raise("die!")
end
