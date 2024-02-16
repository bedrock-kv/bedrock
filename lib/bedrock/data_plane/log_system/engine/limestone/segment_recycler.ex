defmodule Bedrock.DataPlane.LogSystem.Engine.Limestone.SegmentRecycler do
  @doc """
  """
  @spec check_out(any(), new_path :: String.t()) :: {:ok, Segment.t()} | {:error, term()}
  def check_out(segment_recycler, new_path),
    do: GenServer.call(segment_recycler, {:check_out, new_path})

  @doc """
  """
  @spec check_in(any(), Segment.t()) :: :ok
  def check_in(segment_recycler, segment),
    do: GenServer.cast(segment_recycler, {:check_in, segment})

  defdelegate child_spec(opts), to: __MODULE__.Service

  defmodule Service do
    use GenServer

    alias Bedrock.DataPlane.LogSystem.Engine.Limestone.UnusedSegments

    defstruct ~w[path minimum_available unused_segments]a
    @type t :: %__MODULE__{}

    def child_spec(args) do
      path = args[:path] || raise "Missing :path option"
      minimum_available = args[:minimum_available] || raise "Missing :minimum_available option"
      segment_size = args[:segment_size] || raise "Missing :segment_size option"
      otp_name = args[:otp_name] || raise "Missing :otp_name option"

      %{
        id: __MODULE__,
        start:
          {GenServer, :start_link,
           [
             __MODULE__,
             {
               path,
               minimum_available,
               segment_size
             },
             [name: otp_name]
           ]}
      }
    end

    @impl GenServer
    def init({path, minimum_available, segment_size}) do
      {:ok,
       %__MODULE__{
         path: path,
         minimum_available: minimum_available,
         unused_segments: UnusedSegments.new!(path, segment_size)
       }, {:continue, :ensure_minimum_available}}
    end

    @impl GenServer
    def handle_continue(:ensure_minimum_available, state) do
      state.unused_segments
      |> UnusedSegments.ensure_minimum_available(state.minimum_available)
      |> case do
        {:ok, unused_segments} ->
          {:noreply, %{state | unused_segments: unused_segments}}

        {:error, reason} ->
          {:stop, :shutdown, reason}
      end
    end

    @impl GenServer
    def handle_call({:check_out, new_path}, _from, state) do
      UnusedSegments.check_out(state.unused_segments, new_path)
      |> case do
        {:ok, segment, unused_segments} ->
          {:reply, segment, state |> with_updated_unused_segments(unused_segments)}

        {:error, reason} ->
          {:reply, {:error, reason}, state}
      end
    end

    def handle_call({:check_in, segment}, _from, state) do
      UnusedSegments.check_in(state.unused_segments, segment)
      |> case do
        {:ok, unused_segments} ->
          {:reply, :ok, state |> with_updated_unused_segments(unused_segments)}

        {:error, reason} ->
          {:reply, {:error, reason}, state}
      end
    end

    def with_updated_unused_segments(state, unused_segments),
      do: %{state | unused_segments: unused_segments}
  end
end
