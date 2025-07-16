defmodule Bedrock.DataPlane.Proxy do
  use GenServer
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.ControlPlane.Director
  alias Bedrock.DataPlane.Sequencer

  @type t :: %__MODULE__{
          director: Director.ref(),
          layout: TransactionSystemLayout.t()
        }

  defstruct ~w[director layout]a

  @spec next_read_version(pid() | atom()) :: Bedrock.version()
  def next_read_version(proxy), do: GenServer.call(proxy, :get_read_version)

  @spec child_spec(
          opts :: [id: String.t(), director: Director.ref(), layout: TransactionSystemLayout.t()]
        ) ::
          Supervisor.child_spec()
  def child_spec(opts) do
    id = Keyword.get(opts, :id) || raise "Missing :id option"
    director = Keyword.get(opts, :director) || raise "Missing :director option"
    layout = Keyword.get(opts, :layout) || raise "Missing :layout option"

    %{
      id: id,
      start: {GenServer, :start_link, [__MODULE__, {director, layout}]},
      restart: :transient
    }
  end

  @impl GenServer
  def init({director, layout}) do
    {:ok,
     %__MODULE__{
       director: director,
       layout: layout
     }}
  end

  @impl GenServer
  def handle_call(:get_read_version, from, state) do
    forward_call(state.layout.sequencer, from, :next_read_version)
    {:noreply, state}
  end

  @spec forward_call(Sequencer.ref(), GenServer.from(), :next_read_version) ::
          :ok
  defp forward_call(gen_server, from, message),
    do: send(gen_server, {:"$gen_call", from, message})
end
