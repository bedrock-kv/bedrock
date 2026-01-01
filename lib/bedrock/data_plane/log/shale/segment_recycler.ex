defmodule Bedrock.DataPlane.Log.Shale.SegmentRecycler do
  @moduledoc """
  Manages segment file allocation and recycling for the Shale log.
  """

  @type server :: GenServer.server()

  @doc """
  Ask the recycler for a fresh segment. The recycler will return one of the
  segments that it has on-hand, and will attempt to allocate a new one if
  necessary.
  """
  @spec check_out(server(), new_path :: String.t()) :: :ok | {:error, term()}
  def check_out(segment_recycler, new_path), do: GenServer.call(segment_recycler, {:check_out, new_path})

  @doc """
  Return a segment to the recycler. The recycler will attempt to delete the
  segment if it has too many on-hand.
  """
  @spec check_in(server(), path :: String.t()) :: :ok
  def check_in(segment_recycler, segment), do: GenServer.call(segment_recycler, {:check_in, segment})

  @spec child_spec(term()) :: Supervisor.child_spec()
  def child_spec(args) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [args]}
    }
  end

  @doc false
  @spec start_link(
          opts :: [
            path: String.t(),
            min_available: pos_integer(),
            max_available: pos_integer(),
            segment_size: pos_integer()
          ]
        ) :: GenServer.on_start()
  def start_link(args) do
    path = Keyword.fetch!(args, :path)
    min_available = Keyword.fetch!(args, :min_available)
    max_available = Keyword.fetch!(args, :max_available)
    segment_size = Keyword.fetch!(args, :segment_size)

    GenServer.start_link(
      __MODULE__.Server,
      {
        path,
        segment_size,
        min_available,
        max_available
      },
      []
    )
  end

  defmodule State do
    @moduledoc false

    defstruct path: nil,
              segments: [],
              size: 0,
              next_id: 0,
              min_available: 0,
              max_available: 0

    @type t :: %__MODULE__{}
  end

  defmodule Logic do
    @moduledoc false

    @spec unused_file_prefix() :: String.t()
    def unused_file_prefix, do: "preallocated"
    @spec generate_unused_file_name(non_neg_integer()) :: String.t()
    def generate_unused_file_name(id), do: "#{unused_file_prefix()}.#{id}"

    @spec new(
            path_to_dir :: binary(),
            size :: non_neg_integer(),
            min_available :: pos_integer(),
            max_available :: pos_integer()
          ) :: {:ok, State.t()} | {:error, atom()}
    def new(path_to_dir, size, min_available, max_available) do
      if File.dir?(path_to_dir) do
        segments = find_existing_preallocated_files(path_to_dir)
        highest_id = find_highest_id(segments)

        {:ok,
         %State{
           path: path_to_dir,
           segments: segments,
           size: size,
           next_id: highest_id + 1,
           min_available: min_available,
           max_available: max_available
         }}
      else
        {:error, :path_is_not_a_directory}
      end
    end

    @spec new!(
            path_to_dir :: binary(),
            size :: non_neg_integer(),
            min_available :: pos_integer(),
            max_available :: pos_integer()
          ) :: State.t()
    def new!(path_to_dir, size, min_available, max_available) do
      path_to_dir
      |> new(size, min_available, max_available)
      |> case do
        {:ok, t} -> t
        {:error, reason} -> raise reason
      end
    end

    @spec find_existing_preallocated_files(dir_path :: binary()) ::
            [path_to_file :: String.t()]
    def find_existing_preallocated_files(path), do: Path.wildcard(Path.join(path, "#{unused_file_prefix()}.*"))

    @spec check_out(State.t(), new_name :: String.t()) ::
            {:ok, State.t()}
            | {:error, atom()}
    def check_out(%{segments: []}, _new_name), do: {:error, :unavailable}

    def check_out(%{segments: [path_to_file | remaining_segments]} = t, new_name) do
      with :ok <- File.rename(path_to_file, new_name) do
        {:ok, %{t | segments: remaining_segments}}
      end
    end

    @spec check_in(State.t(), path_to_file :: String.t()) ::
            {:ok, State.t()} | {:error, :file_is_not_closed}
    def check_in(t, path_to_file) do
      new_path_to_file = Path.join(t.path, generate_unused_file_name(t.next_id))

      with :ok <- File.rename(path_to_file, new_path_to_file) do
        {:ok, %{t | segments: [new_path_to_file | t.segments], next_id: t.next_id + 1}}
      end
    end

    @spec ensure_min_available(State.t(), non_neg_integer()) ::
            {:ok, State.t()} | {:error, atom()}
    def ensure_min_available(_t, n) when n < 0, do: raise("n must be >= 0")

    def ensure_min_available(t, n), do: create_new_segments(t, max(0, n - length(t.segments)))

    @spec create_new_segments(State.t(), non_neg_integer()) :: {:ok, State.t()} | {:error, atom()}
    def create_new_segments(t, 0), do: {:ok, t}

    @spec create_new_segments(State.t(), non_neg_integer()) :: {:ok, State.t()} | {:error, atom()}
    def create_new_segments(t, n) do
      with {:ok, segment} <-
             allocate_file(
               Path.join(t.path, generate_unused_file_name(t.next_id)),
               t.size
             ) do
        create_new_segments(%{t | segments: [segment | t.segments], next_id: t.next_id + 1}, n - 1)
      end
    end

    @spec find_highest_id(segments :: [path_to_file :: String.t()]) :: non_neg_integer()
    def find_highest_id([]), do: 0

    def find_highest_id(segments) do
      segments
      |> Enum.map(&(&1 |> Path.basename() |> String.split(".")))
      |> Enum.map(fn [_prefix, id] -> String.to_integer(id) end)
      |> Enum.max()
    end

    @spec allocate_file(String.t(), non_neg_integer()) :: {:ok, String.t()} | {:error, atom()}
    def allocate_file(path, size_in_bytes) do
      with {:ok, fd} <- File.open(path, [:write, :binary, :raw, :exclusive]),
           :ok <- :file.allocate(fd, 0, size_in_bytes),
           :ok <- File.close(fd) do
        {:ok, path}
      else
        {:error, :eisdir} -> raise "not implemented"
        {:error, :enoent} -> {:error, :path_does_not_exist}
      end
    end
  end

  defmodule Server do
    @moduledoc false

    use GenServer

    @impl GenServer
    def init({path, segment_size, min_available, max_available}) do
      path
      |> Logic.new(segment_size, min_available, max_available)
      |> case do
        {:ok, state} -> {:ok, state, {:continue, :ensure_min_available}}
        {:error, reason} -> {:stop, reason}
      end
    end

    @impl GenServer
    def handle_call({:check_out, new_path}, _from, state) do
      state
      |> Logic.check_out(new_path)
      |> case do
        {:ok, state} ->
          {:reply, :ok, state, {:continue, :ensure_min_available}}

        {:error, _reason} = error ->
          {:reply, error, state}
      end
    end

    @impl GenServer
    def handle_call({:check_in, segment}, _from, state) do
      state
      |> Logic.check_in(segment)
      |> case do
        {:ok, state} -> {:reply, :ok, state}
        {:error, reason} -> {:reply, {:error, reason}, state}
      end
    end

    @impl GenServer
    def handle_continue(:ensure_min_available, state) do
      state
      |> Logic.ensure_min_available(state.min_available)
      |> case do
        {:ok, state} -> {:noreply, state}
        {:error, reason} -> {:stop, :shutdown, reason}
      end
    end
  end
end
