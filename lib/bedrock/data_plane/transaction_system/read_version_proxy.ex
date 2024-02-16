defmodule Bedrock.DataPlane.TransactionSystem.ReadVersionProxy do
  use GenServer
  alias Bedrock.ControlPlane.ClusterController

  defstruct ~w[controller sequencer]a

  def next_read_version(proxy), do: GenServer.call(proxy, :get_read_version)

  def child_spec(opts) do
    id = Keyword.get(opts, :id) || raise "Missing :id option"
    controller = Keyword.get(opts, :controller) || raise "Missing :controller option"
    sequencer = ClusterController.get_sequencer(controller)

    %{
      id: id,
      start: {GenServer, :start_link, [__MODULE__, {controller, sequencer}]},
      restart: :transient
    }
  end

  @impl GenServer
  def init({controller, sequencer}) do
    {:ok,
     %__MODULE__{
       controller: controller,
       sequencer: sequencer
     }}
  end

  @impl GenServer
  def handle_call(:get_read_version, from, state) do
    forward_call(state.sequencer, from, :next_read_version)
    {:noreply, state}
  end

  defp forward_call(gen_server, message, from),
    do: send(gen_server, {:"$gen_call", from, message})
end
