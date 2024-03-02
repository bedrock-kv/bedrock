defmodule Bedrock.DataPlane.Sequencer do
  use GenServer
  use Bedrock, :types

  defstruct [:cluster, :controller, epoch: 0, latest_committed_version: <<>>]

  @spec invite_to_rejoin(t :: GenServer.name(), controller :: pid(), epoch()) :: :ok
  def invite_to_rejoin(t, controller, epoch),
    do: GenServer.cast(t, {:invite_to_rejoin, controller, epoch})

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
  def handle_cast({:recruitment_invitation, controller, epoch}, t)
      when t.epoch >= epoch,
      do: {:noreply, %{t | controller: controller, epoch: epoch}}

  def handle_cast({:recruitment_invitation, _controller, _epoch}, t),
    do: {:noreply, t}

  @impl GenServer
  def handle_call(:next_read_version, _from, t),
    do: {:reply, {:ok, t.latest_committed_version}, t}

  @impl GenServer
  def handle_info(:die, _t), do: raise("die!")
end
