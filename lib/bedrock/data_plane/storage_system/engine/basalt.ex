defmodule Bedrock.DataPlane.StorageSystem.Engine.Basalt do
  use Bedrock.Worker
  use GenServer

  alias Bedrock.Worker.Controller
  alias Bedrock.DataPlane.StorageSystem.Engine.Basalt.Database
  alias Bedrock.DataPlane.StorageSystem.Engine.Basalt.Writer

  defstruct ~w[otp_name path controller id database writer key_min key_max]a
  @type t :: %__MODULE__{}

  @supported_info ~w[
    durable_version
    id
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
      id: __MODULE__,
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
    new_state(otp_name, controller, id, path)
    |> case do
      {:ok, state} -> {:ok, state, {:continue, :report_health_to_controller}}
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl GenServer
  def terminate(:normal, state) do
    :ok = Writer.stop(state.writer, :normal)
    :ok = Database.close(state.database)
    :normal
  end

  defp new_state(otp_name, controller, id, path) do
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
    else
      :error -> {:error, :invalid_args}
      {:error, _reason} = error -> error
    end
  end

  @impl GenServer
  def handle_call({:get, key, version, opts}, _from, state),
    do: {:reply, state |> get(key, version, opts), state}

  def handle_call({:info, opts}, _from, state),
    do: {:reply, state |> info(opts), state}

  @impl GenServer
  def handle_continue(:report_health_to_controller, state) do
    Controller.report_engine_health(state.controller, state.id, :ok)
    {:noreply, state}
  end

  defp ensure_directory_exists(path), do: File.mkdir_p(path)

  defp get(state, key, version, opts),
    do: Database.lookup(state.database, key, version, opts)

  defp info(state, opts) do
    {:ok,
     opts
     |> Enum.reject(&(not Enum.member?(@supported_info, &1)))
     |> Enum.reduce([], fn
       fact_name, acc -> [{fact_name, gather_info(fact_name, state)} | acc]
     end)}
  end

  defp gather_info(:durable_version, state), do: Database.last_durable_version(state.database)
  defp gather_info(:id, state), do: state.id
  defp gather_info(:key_range, state), do: Database.key_range(state.database)
  defp gather_info(:kind, _state), do: :storage
  defp gather_info(:n_objects, state), do: Database.info(state.database, :n_objects)
  defp gather_info(:otp_name, state), do: state.otp_name
  defp gather_info(:path, state), do: state.path
  defp gather_info(:size_in_bytes, state), do: Database.info(state.database, :size_in_bytes)
  defp gather_info(:supported_info, _state), do: @supported_info
  defp gather_info(:utilization, state), do: Database.info(state.database, :utilization)
end
