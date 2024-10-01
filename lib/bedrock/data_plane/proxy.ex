defmodule Bedrock.DataPlane.Proxy do
  use GenServer
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.ControlPlane.ClusterController

  @type t :: %__MODULE__{
          controller: ClusterController.t(),
          layout: TransactionSystemLayout.t()
        }

  defstruct ~w[controller layout]a

  def next_read_version(proxy), do: GenServer.call(proxy, :get_read_version)

  def child_spec(opts) do
    id = Keyword.get(opts, :id) || raise "Missing :id option"
    controller = Keyword.get(opts, :controller) || raise "Missing :controller option"
    {:ok, layout} = ClusterController.fetch_transaction_system_layout(controller)

    %{
      id: id,
      start: {GenServer, :start_link, [__MODULE__, {controller, layout}]},
      restart: :transient
    }
  end

  @impl GenServer
  def init({controller, layout}) do
    {:ok,
     %__MODULE__{
       controller: controller,
       layout: layout
     }}
  end

  @impl GenServer
  def handle_call(:get_read_version, from, state) do
    forward_call(state.layout.sequencer, from, :next_read_version)
    {:noreply, state}
  end

  defp forward_call(gen_server, message, from),
    do: send(gen_server, {:"$gen_call", from, message})
end
