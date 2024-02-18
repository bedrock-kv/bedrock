defmodule Bedrock.Engine.Controller do
  use GenServer

  alias Bedrock.Engine
  alias Bedrock.Engine.Manifest

  require Logger

  defstruct ~w[
    cluster
    subsystem
    default_engine
    engine_supervisor_otp_name
    engines
    health
    otp_name
    otp_scope
    path
    registry
    waiting_for_healthy
  ]a
  @type t :: %__MODULE__{}
  @type engine_id :: String.t()
  @type controller_name :: GenServer.name()

  defguard is_controller(t) when is_pid(t) or is_atom(t) or is_tuple(t)

  defmodule Info do
    defstruct ~w[health otp_name]a
    @type t :: %__MODULE__{}
  end

  def wait_for_healthy(t, timeout) when is_controller(t) do
    GenServer.call(t, :wait_for_healthy, timeout)
  catch
    :exit, {:noproc, {GenServer, :call, _}} ->
      {:error, :engine_controller_does_not_exist}
  end

  @spec report_engine_health(
          controller_name(),
          engine_id(),
          any()
        ) :: :ok
  def report_engine_health(t, engine_id, health) when is_controller(t),
    do: GenServer.cast(t, {:engine_health, engine_id, health})

  @spec engines(t :: controller_name()) :: {:ok, [Engine.t()]} | {:error, term()}
  def engines(t) when is_controller(t) do
    GenServer.call(t, :engines)
  catch
    :exit, {:noproc, {GenServer, :call, _}} ->
      {:error, :engine_does_not_exist}
  end

  def child_spec(opts) do
    cluster = opts[:cluster] || raise "Missing :cluster option"
    subsystem = opts[:subsystem] || raise "Missing :subsystem option"
    path = opts[:path] || raise "Missing :path option"
    otp_name = opts[:otp_name] || raise "Missing :otp_name option"
    otp_scope = opts[:otp_scope] || raise "Missing :otp_scope option"
    default_engine = opts[:default_engine] || raise "Missing :default_engine option"

    engine_supervisor_otp_name =
      Keyword.get(opts, :engine_supervisor_otp_name) ||
        raise "Missing :engine_supervisor_otp_name option"

    %{
      id: __MODULE__,
      start:
        {GenServer, :start_link,
         [
           __MODULE__,
           {
             subsystem,
             cluster,
             path,
             default_engine,
             engine_supervisor_otp_name,
             otp_scope,
             otp_name
           },
           [name: otp_name]
         ]}
    }
  end

  @impl GenServer
  def init({
        subsystem,
        cluster,
        path,
        default_engine,
        engine_supervisor_otp_name,
        otp_scope,
        otp_name
      }) do
    t =
      %__MODULE__{
        subsystem: subsystem,
        cluster: cluster,
        path: path,
        default_engine: default_engine,
        engine_supervisor_otp_name: engine_supervisor_otp_name,
        otp_scope: otp_scope,
        otp_name: otp_name,
        #
        health: :starting,
        waiting_for_healthy: [],
        engines: %{}
      }

    {:ok, t, {:continue, :sync_existing}}
  end

  @impl GenServer
  def handle_continue(:sync_existing, t) do
    Logger.debug("Syncing existing engines")

    engines =
      t.engine_supervisor_otp_name
      |> DynamicSupervisor.which_children()
      |> Enum.map(fn
        {_, engine_pid, _, _} when is_pid(engine_pid) ->
          Engine.info(engine_pid, [:id, :health, :otp_name])
          |> case do
            {:ok, info} ->
              {info[:id],
               %Info{
                 health: info[:health] || :ok,
                 otp_name: info[:otp_name]
               }}

            _ ->
              :skip
          end

        _ ->
          :skip
      end)
      |> Enum.reject(&(&1 == :skip))
      |> Map.new()

    {:noreply, %{t | engines: engines} |> recompute_controller_health(), {:continue, :spin_up}}
  end

  def handle_continue(:spin_up, t) do
    Logger.debug("Find existing persistent engines")

    engine_ids_to_start =
      t.path
      |> Path.join("*")
      |> Path.wildcard()
      |> Enum.map(&Path.basename/1)
      |> Enum.reject(&Map.has_key?(t.engines, &1))

    if [] == engine_ids_to_start && map_size(t.engines) == 0 do
      {:noreply, t, {:continue, :setup_first_instance}}
    else
      {:noreply, t, {:continue, {:start_engines, engine_ids_to_start}}}
    end
  end

  def handle_continue(:setup_first_instance, t) do
    Logger.debug("Configuring first instance of engine")

    new_engine(t)
    |> case do
      {:ok, engine_id} -> {:noreply, t, {:continue, {:start_engines, [engine_id]}}}
      {:error, reason} -> {:stop, reason}
    end
  end

  def handle_continue({:start_engines, instance_ids}, t) do
    Logger.debug("Starting engines: #{inspect(instance_ids)}: #{instance_ids |> Enum.join(", ")}")

    engines =
      instance_ids
      |> Enum.into(t.engines, fn instance_id ->
        start_engine_if_necessary(t, instance_id)
        |> case do
          {:ok, _pid} ->
            {instance_id,
             %Info{
               health: :ok,
               otp_name: otp_name_for_engine(t.otp_scope, instance_id)
             }}

          {:error, reason} ->
            {instance_id, %Info{health: {:failed_to_start, reason}}}
        end
      end)

    {:noreply, %{t | engines: engines} |> recompute_controller_health()}
  end

  @impl GenServer
  def handle_call(:ping, _from, t),
    do: {:reply, :pong, t}

  def handle_call(:engines, _from, t) do
    engines =
      t.engines
      |> Enum.map(fn
        {_id, %{otp_name: otp_name}} -> otp_name
      end)

    {:reply, {:ok, engines}, t}
  end

  def handle_call(:wait_for_healthy, _from, %{health: :ok} = t),
    do: {:reply, :ok, t}

  def handle_call(:wait_for_healthy, from, t),
    do: {:noreply, %{t | waiting_for_healthy: [from | t.waiting_for_healthy]}}

  @impl GenServer
  def handle_cast({:engine_health, engine_id, health}, t) do
    {:noreply,
     t
     |> update_engine_health(engine_id, health)}
  end

  def update_engine_health(t, engine_id, health) do
    %{
      t
      | engines:
          Map.update!(t.engines, engine_id, fn info ->
            Map.put(info, :health, health)
          end)
    }
    |> recompute_controller_health()
  end

  def recompute_controller_health(t) do
    %{
      t
      | health:
          t.engines
          |> Map.values()
          |> Enum.map(& &1.health)
          |> Enum.reduce(:ok, fn
            :ok, :ok -> :ok
            :ok, _ -> :starting
            {:failed_to_start, _}, :ok -> :starting
            {:failed_to_start, _}, _ -> {:failed_to_start, :at_least_one_failed_to_start}
          end)
    }
    |> notify_waiting_for_healthy_if_necessary()
  end

  def notify_waiting_for_healthy_if_necessary(%{waiting_for_healthy: []} = t), do: t

  def notify_waiting_for_healthy_if_necessary(%{health: :ok} = t) do
    t.waiting_for_healthy
    |> Enum.each(fn from -> GenServer.reply(from, :ok) end)

    %{t | wait_for_healthy: []}
  end

  @spec new_engine(t()) :: {:ok, engine_id()} | {:error, term()}
  def new_engine(t) do
    with id <- UUID.uuid4(),
         path <- Path.join(t.path, id),
         :ok <- File.mkdir_p(path),
         manifest <- Manifest.new(t.cluster.name(), id, t.default_engine),
         :ok <- manifest.engine.one_time_initialization(path),
         :ok <- manifest |> Manifest.write_to_file(path |> Path.join("manifest.json")) do
      {:ok, id}
    end
  end

  @spec start_engine_if_necessary(t(), engine_id()) ::
          {:ok, pid()} | {:error, term()}
  def start_engine_if_necessary(t, id) do
    t.otp_name
    |> otp_name_for_engine(id)
    |> Process.whereis()
    |> case do
      nil -> start_engine(t, id)
      pid -> {:ok, pid}
    end
  end

  @spec start_engine(t(), engine_id()) ::
          {:ok, pid()} | {:error, term()}
  def start_engine(t, id) do
    with path <- Path.join(t.path, id),
         {:ok, manifest} <- Manifest.load_from_file(Path.join(path, "manifest.json")),
         :ok <- check_manifest_id(manifest, id),
         :ok <- check_manifest_cluster_name(manifest, t.cluster.name()) do
      t.engine_supervisor_otp_name
      |> DynamicSupervisor.start_child(
        manifest.engine.child_spec(
          path: path,
          id: id,
          controller: t.otp_name,
          otp_name: otp_name_for_engine(t.otp_scope, id)
        )
        |> Map.put(:restart, :transient)
      )
    end
  end

  @spec check_manifest_id(manifest :: Manifest.t(), id :: engine_id()) ::
          :ok | {:error, :id_in_manifest_does_not_match}
  defp check_manifest_id(manifest, id) when manifest.id == id, do: :ok
  defp check_manifest_id(_, _), do: {:error, :id_in_manifest_does_not_match}

  @spec check_manifest_cluster_name(manifest :: Manifest.t(), cluster_name :: String.t()) ::
          :ok | {:error, :cluster_name_in_manifest_does_not_match}
  defp check_manifest_cluster_name(manifest, cluster_name)
       when manifest.cluster == cluster_name,
       do: :ok

  defp check_manifest_cluster_name(_, _), do: {:error, :cluster_name_in_manifest_does_not_match}

  @spec otp_name_for_engine(otp_name :: atom(), engine_id()) :: atom()
  def otp_name_for_engine(otp_name, id),
    do: :"#{otp_name}_#{id |> String.replace("-", "_")}"
end
