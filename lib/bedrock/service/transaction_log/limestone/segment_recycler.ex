defmodule Bedrock.Service.TransactionLog.Limestone.SegmentRecycler do
  alias Bedrock.Service.StorageWorker.Basalt.Logic
  alias Bedrock.Service.TransactionLog.Limestone.Segment

  @type server :: GenServer.server()

  @doc """
  Ask the recycler for a fresh segment. The recycler will return one of the
  segments that it has on-hand, and will attempt to allocate a new one if
  necessary.
  """
  @spec check_out(server(), new_path :: String.t()) :: {:ok, Segment.t()} | {:error, term()}
  def check_out(segment_recycler, new_path),
    do: GenServer.call(segment_recycler, {:check_out, new_path})

  @doc """
  Return a segment to the recycler. The recycler will attempt to delete the
  segment if it has too many on-hand.
  """
  @spec check_in(server(), Segment.t()) :: :ok
  def check_in(segment_recycler, segment),
    do: GenServer.cast(segment_recycler, {:check_in, segment})

  @doc false
  @spec child_spec(opts :: keyword) :: map()
  def child_spec(args) do
    id = Keyword.fetch!(args, :id)
    path = Keyword.fetch!(args, :path)
    min_available = Keyword.fetch!(args, :min_available)
    max_available = Keyword.fetch!(args, :max_available)
    segment_size = Keyword.fetch!(args, :segment_size)
    otp_name = Keyword.fetch!(args, :otp_name)

    %{
      id: __MODULE__,
      start:
        {GenServer, :start_link,
         [
           __MODULE__.Server,
           {
             id,
             path,
             segment_size,
             min_available,
             max_available
           },
           [name: otp_name]
         ]}
    }
  end

  defmodule State do
    defstruct id: nil,
              path: nil,
              segments: [],
              size: 0,
              next_id: 0,
              min_available: 0,
              max_available: 0

    @type t :: %__MODULE__{}
  end

  defmodule Logic do
    def unused_file_prefix, do: "preallocated"
    def generate_unused_file_name(id), do: "#{unused_file_prefix()}.#{id}"

    @spec new(
            id :: String.t(),
            path_to_dir :: binary(),
            size :: non_neg_integer(),
            min_available :: pos_integer(),
            max_available :: pos_integer()
          ) :: {:ok, State.t()} | {:error, atom()}
    def new(id, path_to_dir, size, min_available, max_available) do
      with true <- File.dir?(path_to_dir) || {:error, :path_is_not_a_directory},
           segments <- find_existing_preallocated_files(path_to_dir),
           highest_id <- find_highest_id(segments) do
        {:ok,
         %State{
           id: id,
           path: path_to_dir,
           segments: segments,
           size: size,
           next_id: highest_id + 1,
           min_available: min_available,
           max_available: max_available
         }}
      end
    end

    @spec new!(
            id :: String.t(),
            path_to_dir :: binary(),
            size :: non_neg_integer(),
            min_available :: pos_integer(),
            max_available :: pos_integer()
          ) :: State.t()
    def new!(id, path_to_dir, size, min_available, max_available) do
      new(id, path_to_dir, size, min_available, max_available)
      |> case do
        {:ok, t} -> t
        {:error, reason} -> raise reason
      end
    end

    @spec find_existing_preallocated_files(dir_path :: binary()) :: [Segment.t()]
    def find_existing_preallocated_files(path) do
      Path.wildcard(Path.join(path, "#{unused_file_prefix()}.*"))
      |> Enum.reduce([], fn
        _, {:error, _} = error ->
          error

        file_name, segments ->
          Segment.from_path(file_name)
          |> case do
            {:ok, segment} -> [segment | segments]
            {:error, _} -> segments
          end
      end)
    end

    @spec check_out(State.t(), new_name :: String.t()) ::
            {:ok, Segment.t(), State.t()} | {:error, atom()}
    def check_out(%{segments: []}, _new_name), do: {:error, :unavailable}

    def check_out(t, new_name) do
      [first_segment | remaining_segments] = t.segments

      first_segment
      |> Segment.rename(new_name)
      |> case do
        {:ok, renamed} -> {:ok, renamed, %{t | segments: remaining_segments}}
        {:error, _reason} = error -> error
      end
    end

    @spec check_in(State.t(), Segment.t()) :: {:ok, State.t()} | {:error, atom()}
    def check_in(t, file) do
      with new_name <- Path.join(t.path, generate_unused_file_name(t.next_id)),
           {:ok, renamed} <- Segment.rename(file, new_name),
           :ok <- Segment.clear(renamed) do
        {:ok, %{t | segments: [renamed | t.segments], next_id: t.next_id + 1}}
      end
    end

    @spec ensure_min_available(State.t(), non_neg_integer()) ::
            {:ok, State.t()} | {:error, atom()}
    def ensure_min_available(_t, n) when n < 0, do: raise("n must be >= 0")

    def ensure_min_available(t, n), do: create_new_segments(t, max(0, n - length(t.segments)))

    def create_new_segments(t, 0), do: {:ok, t}

    def create_new_segments(t, n) do
      with {:ok, segment} <-
             Segment.allocate(
               Path.join(t.path, generate_unused_file_name(t.next_id)),
               t.size
             ) do
        %{t | segments: [segment | t.segments], next_id: t.next_id + 1}
        |> create_new_segments(n - 1)
      end
    end

    def find_highest_id([]), do: 0

    def find_highest_id(segments) do
      segments
      |> Enum.map(&(&1.path |> Path.basename() |> String.split(".")))
      |> Enum.reject(fn [prefix, _] -> prefix != unused_file_prefix() end)
      |> Enum.map(fn [_prefix, id] -> String.to_integer(id) end)
      |> Enum.max()
    end
  end

  defmodule Server do
    use GenServer

    @impl GenServer
    def init({id, path, segment_size, min_available, max_available}) do
      Logic.new(id, path, segment_size, min_available, max_available)
      |> case do
        {:ok, state} -> {:ok, state, {:continue, :ensure_min_available}}
        {:error, reason} -> {:stop, reason}
      end
    end

    @impl GenServer
    def handle_call({:check_out, new_path}, _from, state) do
      Logic.check_out(state, new_path)
      |> case do
        {:ok, segment, state} ->
          {:reply, {:ok, segment}, state, {:continue, :ensure_min_available}}

        {:error, reason} ->
          {:reply, {:error, reason}, state}
      end
    end

    def handle_call({:check_in, segment}, _from, state) do
      Logic.check_in(state, segment)
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
