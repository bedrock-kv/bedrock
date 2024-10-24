defmodule Bedrock.DataPlane.Storage.Basalt do
  use Bedrock.Service.WorkerBehaviour

  alias Agent.Server
  alias Bedrock.Service.StorageController
  alias Bedrock.Service.Worker
  alias Bedrock.ControlPlane.ClusterController
  alias Bedrock.DataPlane.Storage
  alias Bedrock.DataPlane.Storage.Basalt.Database

  @doc false
  @spec child_spec(opts :: keyword()) :: map()
  defdelegate child_spec(opts), to: __MODULE__.Server

  defmodule State do
    @type t :: %__MODULE__{
            otp_name: atom(),
            path: Path.t(),
            epoch: Bedrock.epoch() | nil,
            controller: pid() | nil,
            id: Worker.id(),
            database: Database.t()
          }
    defstruct otp_name: nil,
              path: nil,
              epoch: nil,
              controller: nil,
              id: nil,
              database: nil
  end

  defmodule Logic do
    alias Bedrock.DataPlane.Version

    @spec startup(otp_name :: atom(), controller :: pid(), id :: Worker.id(), Path.t()) ::
            {:ok, State.t()} | {:error, term()}
    def startup(otp_name, controller, id, path) do
      with :ok <- ensure_directory_exists(path),
           {:ok, database} <- Database.open(:"#{otp_name}_db", Path.join(path, "dets")) do
        {:ok,
         %State{
           path: path,
           otp_name: otp_name,
           id: id,
           controller: controller,
           database: database
         }}
      end
    end

    @spec ensure_directory_exists(Path.t()) :: :ok | {:error, File.posix()}
    defp ensure_directory_exists(path),
      do: File.mkdir_p(path)

    @spec shutdown(State.t()) :: :ok
    def shutdown(%State{} = t),
      do: :ok = Database.close(t.database)

    @spec lock_for_recovery(State.t(), ClusterController.ref(), Bedrock.epoch()) ::
            {:ok, State.t()} | {:error, :newer_epoch_exists | String.t()}
    def lock_for_recovery(t, _, epoch) when not is_nil(t.epoch) and epoch < t.epoch,
      do: {:error, :newer_epoch_exists}

    def lock_for_recovery(t, controller, epoch),
      do: {:ok, %{t | epoch: epoch, controller: controller}}

    @spec fetch(State.t(), Bedrock.key(), Version.t()) ::
            {:error, :key_out_of_range | :not_found | :tx_too_old} | {:ok, binary()}
    def fetch(%State{} = t, key, version),
      do: Database.fetch(t.database, key, version)

    @spec info(State.t(), Storage.fact_name() | [Storage.fact_name()]) ::
            {:ok, term() | %{Storage.fact_name() => term()}} | {:error, :unsupported_info}
    def info(%State{} = t, fact_name) when is_atom(fact_name),
      do: {:ok, gather_info(fact_name, t)}

    def info(%State{} = t, fact_names) when is_list(fact_names) do
      {:ok,
       fact_names
       |> Enum.reduce([], fn
         fact_name, acc -> [{fact_name, gather_info(fact_name, t)} | acc]
       end)
       |> Map.new()}
    end

    defp supported_info, do: ~w[
      durable_version
      oldest_durable_version
      id
      pid
      path
      key_range
      kind
      n_keys
      otp_name
      size_in_bytes
      supported_info
      utilization
    ]a

    defp gather_info(:oldest_durable_version, t), do: Database.oldest_durable_version(t.database)
    defp gather_info(:durable_version, t), do: Database.last_durable_version(t.database)
    defp gather_info(:id, t), do: t.id
    defp gather_info(:key_range, t), do: Database.key_range(t.database)
    defp gather_info(:kind, _t), do: :storage
    defp gather_info(:n_keys, t), do: Database.info(t.database, :n_keys)
    defp gather_info(:otp_name, t), do: t.otp_name
    defp gather_info(:path, t), do: t.path
    defp gather_info(:pid, _t), do: self()
    defp gather_info(:size_in_bytes, t), do: Database.info(t.database, :size_in_bytes)
    defp gather_info(:supported_info, _t), do: supported_info()
    defp gather_info(:utilization, t), do: Database.info(t.database, :utilization)
    defp gather_info(_unsupported, _t), do: {:error, :unsupported_info}
  end

  defmodule Server do
    use GenServer

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
             __MODULE__,
             {otp_name, controller, id, path},
             [name: otp_name]
           ]}
      }
    end

    @impl GenServer
    def init(args),
      # We use a continuation here to ensure that the controller isn't blocked
      # waiting for the worker to finish it's startup sequence (which could take
      # a few seconds or longer if the database is large.) The controller will
      # be notified when the worker is ready to accept requests.
      do: {:ok, args, {:continue, :finish_startup}}

    @impl GenServer
    def terminate(_, %State{} = t) do
      Logic.shutdown(t)
      :normal
    end

    @impl GenServer
    def handle_call({:fetch, key, version, 0}, _from, %State{} = t),
      do: {:reply, t |> Logic.fetch(key, version), t}

    def handle_call({:info, fact_names}, _from, %State{} = t),
      do: {:reply, t |> Logic.info(fact_names), t}

    def handle_call({:lock_for_recovery, epoch}, controller, t) do
      with {:ok, t} <- t |> Logic.lock_for_recovery(controller, epoch),
           {:ok, info} <- t |> Logic.info(Storage.recovery_info()) do
        {:reply, {:ok, self(), info}, t}
      else
        error -> {:reply, error, t}
      end
    end

    def handle_call(_, _from, t),
      do: {:reply, {:error, :not_ready}, t}

    @impl GenServer
    def handle_continue(:finish_startup, {otp_name, controller, id, path}) do
      Logic.startup(otp_name, controller, id, path)
      |> case do
        {:ok, t} -> {:noreply, t, {:continue, :report_health_to_storage_controller}}
        {:error, reason} -> {:stop, reason, :nostate}
      end
    end

    def handle_continue(:report_health_to_storage_controller, %State{} = t) do
      :ok = StorageController.report_health(t.controller, t.id, :ok)
      {:noreply, t}
    end
  end
end
