defmodule Bedrock.Engine.Controller do
  use GenServer

  alias Bedrock.Engine
  alias Bedrock.Engine.Manifest

  defstruct ~w[
    cluster
    otp_name
    otp_scope
    path
    default_engine
    registry
    engine_supervisor_otp_name
    health
    engines
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
        cluster,
        path,
        default_engine,
        engine_supervisor_otp_name,
        otp_scope,
        otp_name
      }) do
    state =
      %__MODULE__{
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

    {:ok, state, {:continue, :sync_existing}}
  end

  @impl GenServer
  def handle_continue(:sync_existing, state) do
    engines =
      state.engine_supervisor_otp_name
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

    {:noreply, %{state | engines: engines} |> recompute_controller_health(),
     {:continue, :spin_up}}
  end

  def handle_continue(:spin_up, state) do
    engine_ids_to_start =
      state.path
      |> Path.join("*")
      |> Path.wildcard()
      |> Enum.map(&Path.basename/1)
      |> Enum.reject(&Map.has_key?(state.engines, &1))

    if [] == engine_ids_to_start && map_size(state.engines) == 0 do
      {:noreply, state, {:continue, :setup_first_instance}}
    else
      {:noreply, state, {:continue, {:start_engines, engine_ids_to_start}}}
    end
  end

  def handle_continue(:setup_first_instance, state) do
    new_engine(state)
    |> case do
      {:ok, engine_id} -> {:noreply, state, {:continue, {:start_engines, [engine_id]}}}
      {:error, reason} -> {:stop, reason}
    end
  end

  def handle_continue({:start_engines, instance_ids}, state) do
    engines =
      instance_ids
      |> Enum.into(state.engines, fn instance_id ->
        start_engine_if_necessary(state, instance_id)
        |> case do
          {:ok, _pid} ->
            {instance_id,
             %Info{
               health: :ok,
               otp_name: otp_name_for_engine(state.otp_scope, instance_id)
             }}

          {:error, reason} ->
            {instance_id, %Info{health: {:failed_to_start, reason}}}
        end
      end)

    {:noreply, %{state | engines: engines} |> recompute_controller_health()}
  end

  @impl GenServer
  def handle_call({:cluster_controller_replaced, cluster_controller}, from, state) do
    IO.inspect({cluster_controller, from})
    {:reply, :ok, state}
  end

  def handle_call(:ping, _from, state),
    do: {:reply, :pong, state}

  def handle_call(:engines, _from, state) do
    engines =
      state.engines
      |> Enum.map(fn
        {_id, %{otp_name: otp_name}} -> otp_name
      end)

    {:reply, {:ok, engines}, state}
  end

  def handle_call(:wait_for_healthy, _from, %{health: :ok} = state),
    do: {:reply, :ok, state}

  def handle_call(:wait_for_healthy, from, state),
    do: {:noreply, %{state | waiting_for_healthy: [from | state.waiting_for_healthy]}}

  @impl GenServer
  def handle_cast({:engine_health, engine_id, health}, state) do
    {:noreply,
     state
     |> update_engine_health(engine_id, health)}
  end

  def update_engine_health(state, engine_id, health) do
    %{
      state
      | engines:
          Map.update!(state.engines, engine_id, fn info ->
            Map.put(info, :health, health)
          end)
    }
    |> recompute_controller_health()
  end

  def recompute_controller_health(state) do
    %{
      state
      | health:
          state.engines
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

  def notify_waiting_for_healthy_if_necessary(%{waiting_for_healthy: []} = state), do: state

  def notify_waiting_for_healthy_if_necessary(%{health: :ok} = state) do
    state.waiting_for_healthy
    |> Enum.each(fn from -> GenServer.reply(from, :ok) end)

    %{state | wait_for_healthy: []}
  end

  @spec new_engine(state :: t()) :: {:ok, engine_id()} | {:error, term()}
  def new_engine(state) do
    with id <- UUID.uuid4(),
         path <- Path.join(state.path, id),
         :ok <- File.mkdir_p(path),
         manifest <- Manifest.new(state.cluster.name(), id, state.default_engine),
         :ok <- manifest.engine.one_time_initialization(path),
         :ok <- manifest |> Manifest.write_to_file(path |> Path.join("manifest.json")) do
      {:ok, id}
    end
  end

  @spec start_engine_if_necessary(state :: t(), engine_id()) ::
          {:ok, pid()} | {:error, term()}
  def start_engine_if_necessary(state, id) do
    state.otp_name
    |> otp_name_for_engine(id)
    |> Process.whereis()
    |> case do
      nil -> start_engine(state, id)
      pid -> {:ok, pid}
    end
  end

  @spec start_engine(state :: t(), engine_id()) ::
          {:ok, pid()} | {:error, term()}
  def start_engine(state, id) do
    with path <- Path.join(state.path, id),
         {:ok, manifest} <- Manifest.load_from_file(Path.join(path, "manifest.json")),
         :ok <- check_manifest_id(manifest, id),
         :ok <- check_manifest_cluster_name(manifest, state.cluster.name()) do
      state.engine_supervisor_otp_name
      |> DynamicSupervisor.start_child(
        manifest.engine.child_spec(
          path: path,
          id: id,
          controller: state.otp_name,
          otp_name: otp_name_for_engine(state.otp_scope, id)
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
