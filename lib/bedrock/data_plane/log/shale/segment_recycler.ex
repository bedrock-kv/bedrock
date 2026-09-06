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
  Return a segment to the recycler. The recycler will delete the segment
  if it is already holding `max_available` of them.

  Returns the unlink's error if that deletion fails; callers that match
  on `:ok` will crash the log and re-recover, which is the same exposure
  the rename this replaced always had.
  """
  @spec check_in(server(), path :: String.t()) :: :ok | {:error, File.posix()}
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

    # A segment is preallocated under this name and published into the
    # pool by rename, so it must not match the pool's own glob.
    @scratch_prefix ".partial."

    @spec new(
            path_to_dir :: binary(),
            size :: non_neg_integer(),
            min_available :: pos_integer(),
            max_available :: pos_integer()
          ) :: {:ok, State.t()} | {:error, atom()}
    def new(path_to_dir, size, min_available, max_available) do
      cond do
        # min == max leaves no slack for a returned segment to land in:
        # a checkout drops the pool to max - 1, the min-refill allocates a
        # replacement, and the check-in then finds the pool full and
        # unlinks. Every cycle costs a fresh allocation and recycles
        # nothing — the module's whole purpose, silently off. Refuse the
        # config instead of quietly degrading under it.
        min_available >= max_available ->
          {:error, :max_available_must_exceed_min_available}

        not File.dir?(path_to_dir) ->
          {:error, :path_is_not_a_directory}

        true ->
          discard_scratch_files(path_to_dir)

          segments =
            path_to_dir
            |> find_existing_preallocated_files()
            |> adopt_whole_segments(size)

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

    # Membership in the pool has to be provable, not inferred from the
    # name — the same reasoning that makes a manifest, not a directory
    # entry, the proof that a worker directory holds a worker.
    # `check_out/2` renames a pooled file straight into service and
    # `Writer.open/3` derives its write budget from `File.stat/1`, so a
    # file that is not exactly one segment long would present as a
    # healthy segment carrying a budget it cannot honour. Discard it and
    # let the min-refill preallocate a real one in its place.
    @spec adopt_whole_segments([String.t()], non_neg_integer()) :: [String.t()]
    defp adopt_whole_segments(paths, size) do
      Enum.filter(paths, fn path ->
        case File.stat(path) do
          {:ok, %{size: ^size}} ->
            true

          _not_a_whole_segment ->
            _ = File.rm(path)
            false
        end
      end)
    end

    # The pool directory has exactly one owner — the recycler of the log
    # worker that owns the directory — so a scratch file present at
    # startup is always wreckage from an incarnation that died mid
    # allocation, never a live writer's.
    @spec discard_scratch_files(dir_path :: binary()) :: :ok
    defp discard_scratch_files(path) do
      path
      |> Path.join("#{@scratch_prefix}*")
      |> Path.wildcard(match_dot: true)
      |> Enum.each(&File.rm/1)
    end

    @spec check_out(State.t(), new_name :: String.t()) ::
            {:ok, State.t()}
            | {:error, atom()}
    def check_out(%{segments: []}, _new_name), do: {:error, :unavailable}

    def check_out(%{segments: [path_to_file | remaining_segments]} = t, new_name) do
      with :ok <- File.rename(path_to_file, new_name) do
        {:ok, %{t | segments: remaining_segments}}
      end
    end

    # The pool is a cache, not a ledger: every preallocated file is
    # interchangeable, so at the cap the cheapest correct move is to
    # unlink the segment being returned rather than rename it in and
    # evict another. Without this, a trim burst — `trim_durable_segments/1`
    # splitting off many segments at once when a durability watermark
    # jumps — checks them all in back-to-back with no intervening
    # checkout, and the pool keeps every one of them at `size` bytes
    # apiece for the life of the worker.
    @spec check_in(State.t(), path_to_file :: String.t()) ::
            {:ok, State.t()} | {:error, File.posix()}
    def check_in(%{segments: segments, max_available: max_available} = t, path_to_file)
        when length(segments) >= max_available do
      with :ok <- File.rm(path_to_file), do: {:ok, t}
    end

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

    # Preallocation is create-then-extend, so the file exists at zero
    # length before it is a segment. Doing that under a scratch name and
    # publishing by rename only once it is fully sized and synced keeps
    # the partial state out of the pool: an :enospc, or a kill between
    # the two steps, leaves a scratch file the next incarnation sweeps,
    # never a `preallocated.N` the next incarnation adopts as a whole
    # segment it does not have. Same publish-atomically shape as
    # `LocalFilesystem.put/4`, and with the same caveat — the content is
    # synced but the parent directory entry cannot be, so a power loss
    # can lose the rename and leave the pool short, which the min-refill
    # already handles.
    @spec allocate_file(String.t(), non_neg_integer()) :: {:ok, String.t()} | {:error, atom()}
    def allocate_file(path, size_in_bytes) do
      scratch = scratch_path(path)

      case File.open(scratch, [:write, :binary, :raw, :exclusive]) do
        {:ok, fd} ->
          # Ensure fd is always closed, even if allocate fails
          result =
            try do
              with :ok <- :file.allocate(fd, 0, size_in_bytes), do: :file.sync(fd)
            after
              File.close(fd)
            end

          publish(result, scratch, path)

        {:error, :eisdir} ->
          raise "not implemented"

        {:error, :enoent} ->
          {:error, :path_does_not_exist}

        {:error, reason} ->
          {:error, reason}
      end
    end

    @spec scratch_path(String.t()) :: String.t()
    defp scratch_path(path), do: Path.join(Path.dirname(path), "#{@scratch_prefix}#{Path.basename(path)}")

    @spec publish(:ok | {:error, atom()}, String.t(), String.t()) :: {:ok, String.t()} | {:error, atom()}
    defp publish(:ok, scratch, path) do
      case File.rename(scratch, path) do
        :ok -> {:ok, path}
        {:error, reason} -> discard_scratch(scratch, reason)
      end
    end

    defp publish({:error, reason}, scratch, _path), do: discard_scratch(scratch, reason)

    @spec discard_scratch(String.t(), atom()) :: {:error, atom()}
    defp discard_scratch(scratch, reason) do
      _ = File.rm(scratch)
      {:error, reason}
    end
  end

  defmodule Server do
    @moduledoc false

    use GenServer

    import Bedrock.Internal.GenServer.Replies

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
          reply(state, :ok, continue: :ensure_min_available)

        # Refill on the failure path too. An exhausted pool is the one
        # moment a refill is most needed, and nothing else drives
        # :ensure_min_available — so a pool that ever reached zero could
        # never recover, and every later checkout would fail even after
        # whatever caused the exhaustion had cleared.
        {:error, _reason} = error ->
          reply(state, error, continue: :ensure_min_available)
      end
    end

    @impl GenServer
    def handle_call({:check_in, segment}, _from, state) do
      state
      |> Logic.check_in(segment)
      |> case do
        {:ok, state} -> reply(state, :ok)
        {:error, reason} -> reply(state, {:error, reason})
      end
    end

    @impl GenServer
    def handle_continue(:ensure_min_available, state) do
      state
      |> Logic.ensure_min_available(state.min_available)
      |> case do
        {:ok, state} -> noreply(state)
        # stop/2 is stop(state, reason). Transposed, this exited with
        # :shutdown — an orderly-looking stop — and installed the real
        # cause as the state, discarding exactly the :enospc / :emfile /
        # :enomem distinction Shale's classify_resource_error/1 exists to
        # act on.
        {:error, reason} -> stop(state, reason)
      end
    end
  end
end
