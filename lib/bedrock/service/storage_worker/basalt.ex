defmodule Bedrock.Service.StorageWorker.Basalt do
  use Bedrock.Service.WorkerBehaviour

  alias Agent.Server
  alias Bedrock.Service.Controller
  alias Bedrock.Service.StorageWorker.Basalt.Database
  alias Bedrock.Service.StorageWorker.Basalt.Writer

  @doc false
  @spec child_spec(opts :: keyword()) :: map()
  def child_spec(opts) do
    otp_name = opts[:otp_name] || raise "Missing :otp_name option"
    controller = opts[:controller] || raise "Missing :controller option"
    id = opts[:id] || raise "Missing :id option"
    path = opts[:path] || raise "Missing :path option"

    %{
      id: {__MODULE__, id},
      start:
        {GenServer, :start_link,
         [
           __MODULE__.Server,
           {otp_name, controller, id, path},
           [name: otp_name]
         ]}
    }
  end

  defmodule State do
    @type t :: %__MODULE__{}
    defstruct ~w[otp_name path controller id database writer key_min key_max]a

    def new(path, otp_name, id, controller, database, writer) do
      %State{
        path: path,
        otp_name: otp_name,
        id: id,
        controller: controller,
        database: database,
        writer: writer
      }
    end
  end

  defmodule Logic do
    def startup(otp_name, controller, id, path) do
      with :ok <- ensure_directory_exists(path),
           {:ok, database} <- Database.open(:"#{otp_name}_db", Path.join(path, "dets")),
           {:ok, writer} <-
             Writer.start_link(
               storage_engine: self(),
               database: database,
               id: id,
               otp_name: :"#{otp_name}_writer"
             ) do
        {:ok, State.new(path, otp_name, id, controller, database, writer)}
      end
    end

    @spec ensure_directory_exists(Path.t()) :: :ok | {:error, File.posix()}
    defp ensure_directory_exists(path), do: File.mkdir_p(path)

    def shutdown(%State{} = t) do
      :ok = Writer.stop(t.writer, :normal)
      :ok = Database.close(t.database)
    end

    def fetch(%State{} = t, key, version, timeout),
      do: Database.lookup(t.database, key, version, timeout)

    def info(t, opts) do
      {:ok,
       opts
       |> Enum.reject(&(not Enum.member?(supported_info(), &1)))
       |> Enum.reduce([], fn
         fact_name, acc -> [{fact_name, gather_info(fact_name, t)} | acc]
       end)}
    end

    defp supported_info, do: ~w[
      durable_version
      id
      pid
      path
      key_range
      kind
      n_objects
      otp_name
      size_in_bytes
      supported_info
      utilization
    ]a

    defp gather_info(:durable_version, t), do: Database.last_durable_version(t.database)
    defp gather_info(:id, t), do: t.id
    defp gather_info(:key_range, t), do: Database.key_range(t.database)
    defp gather_info(:kind, _state), do: :storage
    defp gather_info(:n_keys, t), do: Database.info(t.database, :n_keys)
    defp gather_info(:otp_name, t), do: t.otp_name
    defp gather_info(:path, t), do: t.path
    defp gather_info(:pid, _state), do: self()
    defp gather_info(:size_in_bytes, t), do: Database.info(t.database, :size_in_bytes)
    defp gather_info(:supported_info, _state), do: supported_info()
    defp gather_info(:utilization, t), do: Database.info(t.database, :utilization)
  end

  defmodule Server do
    use GenServer

    @impl GenServer
    def init(args),
      # We use a continuation here to ensure that the controller isn't blocked
      # waiting for the worker to finish it's startup sequence which could take
      # a few seconds or longer if the database is large. The controller will
      # be notified when the worker is ready to accept requests.
      do: {:ok, args, {:continue, :finish_startup}}

    @impl GenServer
    def terminate(:normal, %State{} = t) do
      Logic.shutdown(t)
      :normal
    end

    @impl GenServer
    def handle_call({:fetch, key, version, timeout}, _from, %State{} = t),
      do: {:reply, t |> Logic.fetch(key, version, timeout), t}

    def handle_call({:info, opts}, _from, %State{} = t),
      do: {:reply, t |> Logic.info(opts), t}

    def handle_call(_, _from, t),
      do: {:reply, {:error, :not_ready}, t}

    @impl GenServer
    def handle_continue(:finish_startup, {otp_name, controller, id, path}) do
      Logic.startup(otp_name, controller, id, path)
      |> case do
        {:ok, t} -> {:noreply, t, {:continue, :report_health_to_controller}}
        {:error, reason} -> {:stop, :nostate, reason}
      end
    end

    def handle_continue(:report_health_to_controller, %State{} = t) do
      :ok = Controller.report_worker_health(t.controller, t.id, :ok)
      {:noreply, t}
    end
  end
end
