defmodule Bedrock.Service.StorageWorker.Basalt do
  use Bedrock.Service.WorkerBehaviour
  use GenServer

  alias Bedrock.Service.Controller
  alias Bedrock.Service.StorageWorker.Basalt.Database
  alias Bedrock.Service.StorageWorker.Basalt.Writer

  defstruct ~w[otp_name path controller id database writer key_min key_max]a
  @type t :: %__MODULE__{}

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
           __MODULE__,
           {otp_name, controller, id, path},
           [name: otp_name]
         ]}
    }
  end

  @impl GenServer
  def init({otp_name, controller, id, path}) do
    start(otp_name, controller, id, path)
    |> case do
      {:ok, t} -> {:ok, t, {:continue, :report_health_to_controller}}
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl GenServer
  def terminate(:normal, t) do
    :ok = Writer.stop(t.writer, :normal)
    :ok = Database.close(t.database)
    :normal
  end

  defp start(otp_name, controller, id, path) do
    with :ok <- ensure_directory_exists(path),
         {:ok, database} <- Database.open(:"#{otp_name}_db", Path.join(path, "dets")),
         {:ok, writer} <-
           Writer.start_link(
             storage_engine: self(),
             database: database,
             id: id,
             otp_name: :"#{otp_name}_writer"
           ) do
      {:ok,
       %__MODULE__{
         path: path,
         otp_name: otp_name,
         id: id,
         controller: controller,
         database: database,
         writer: writer
       }}
    end
  end

  @impl GenServer
  def handle_call({:get, key, version, opts}, _from, t),
    do: {:reply, t |> get(key, version, opts), t}

  def handle_call({:info, opts}, _from, t),
    do: {:reply, t |> info(opts), t}

  @impl GenServer
  def handle_continue(:report_health_to_controller, t) do
    Controller.report_worker_health(t.controller, t.id, :ok)
    {:noreply, t}
  end

  @spec ensure_directory_exists(Path.t()) :: :ok | {:error, File.posix()}
  defp ensure_directory_exists(path), do: File.mkdir_p(path)

  defp get(t, key, version, opts),
    do: Database.lookup(t.database, key, version, opts)

  defp info(t, opts) do
    {:ok,
     opts
     |> Enum.reject(&(not Enum.member?(supported_info(), &1)))
     |> Enum.reduce([], fn
       fact_name, acc -> [{fact_name, gather_info(fact_name, t)} | acc]
     end)}
  end

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
