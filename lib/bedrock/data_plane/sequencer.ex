defmodule Bedrock.DataPlane.Sequencer do
  use GenServer

  defstruct [:cluster, :controller, epoch: 0, latest_committed_version: <<>>]

  def start_link(opts) do
    cluster = opts[:cluster] || raise "Missing :cluster option"
    controller = opts[:controller] || raise "Missing :controller option"
    epoch = Keyword.get(opts, :epoch, 0)
    otp_name = opts[:otp_name] || raise "Missing :otp_name option"

    GenServer.start_link(__MODULE__, {cluster, controller, epoch}, name: otp_name)
  end

  @impl GenServer
  def init({cluster, controller, epoch}) do
    {:ok,
     %__MODULE__{
       cluster: cluster,
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
