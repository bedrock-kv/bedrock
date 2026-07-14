defmodule Bedrock.ControlPlane.Distributor.Server do
  @moduledoc """
  GenServer implementation of the Distributor singleton.

  Holds the epoch it was recruited under, a reference to the recruiting
  director, and a snapshot of the shard layout. Monitors the director and
  stops (`:normal`) when the director exits or when a newer epoch is
  signaled - the next director recruits a fresh distributor.

  This is the Phase A skeleton: coverage tracking, recruitment, and
  placeholder supervision arrive in later tickets and will all pass
  through the epoch guard implemented here.
  """

  use GenServer

  import Bedrock.ControlPlane.Distributor.Telemetry,
    only: [
      emit_distributor_started: 3,
      emit_distributor_stopped: 3,
      emit_coverage_demand: 2,
      emit_recruitment_started: 3,
      emit_recruitment_succeeded: 5,
      emit_recruitment_failed: 5
    ]

  import Bedrock.Internal.GenServer.Replies

  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.ControlPlane.Director
  alias Bedrock.ControlPlane.Distributor.Placeholder
  alias Bedrock.ControlPlane.Distributor.Recruitment
  alias Bedrock.ControlPlane.Distributor.State
  alias Bedrock.DataPlane.Version

  require Logger

  @default_backoff_ms 5_000

  @doc false
  @spec child_spec(
          opts :: [
            cluster: module(),
            epoch: Bedrock.epoch(),
            director: pid(),
            shard_layout: TransactionSystemLayout.shard_layout(),
            transaction_system_layout: TransactionSystemLayout.t(),
            durable_version: Bedrock.version(),
            node_capabilities: %{Bedrock.Cluster.capability() => [node()]},
            backoff_ms: pos_integer(),
            recruitment: map(),
            otp_name: atom()
          ]
        ) :: Supervisor.child_spec()
  def child_spec(opts) do
    otp_name = opts[:otp_name] || raise "Missing :otp_name option"
    state = initial_state(opts)

    %{
      id: {__MODULE__, state.cluster, state.epoch},
      start: {GenServer, :start_link, [__MODULE__, state, [name: otp_name]]},
      restart: :temporary
    }
  end

  @spec initial_state(keyword()) :: State.t()
  defp initial_state(opts) do
    %State{
      cluster: Keyword.fetch!(opts, :cluster),
      epoch: Keyword.fetch!(opts, :epoch),
      director: Keyword.fetch!(opts, :director),
      shard_layout: Keyword.get(opts, :shard_layout, %{}),
      transaction_system_layout: Keyword.get(opts, :transaction_system_layout, %{}),
      durable_version: Keyword.get_lazy(opts, :durable_version, &Version.zero/0),
      node_capabilities: Keyword.get(opts, :node_capabilities, %{}),
      backoff_ms: Keyword.get(opts, :backoff_ms, @default_backoff_ms),
      recruitment_overrides: Keyword.get(opts, :recruitment, %{})
    }
  end

  @impl true
  @spec init(State.t()) :: {:ok, State.t()}
  def init(%State{} = state) do
    # Monitor the Director - if it dies, this distributor should terminate
    # and let the next director recruit a fresh one. Trap exits so the
    # linked placeholder can be restarted when it dies.
    Process.flag(:trap_exit, true)
    Process.monitor(state.director)

    emit_distributor_started(state.cluster, state.epoch, state.director)

    {:ok, start_placeholder(state)}
  end

  @impl true
  def handle_call({:check_epoch, epoch}, _from, %State{} = t) do
    case State.check_epoch(t, epoch) do
      :ok ->
        reply(t, :ok)

      {:error, :epoch_superseded} = error ->
        Logger.info("Distributor for epoch #{t.epoch} superseded by epoch #{epoch}; stopping")
        {:stop, :normal, error, t}

      {:error, :newer_epoch_exists} = error ->
        reply(t, error)
    end
  end

  @impl true
  def handle_cast({:epoch_changed, new_epoch}, %State{} = t) do
    case State.check_epoch(t, new_epoch) do
      {:error, :epoch_superseded} ->
        Logger.info("Distributor for epoch #{t.epoch} superseded by epoch #{new_epoch}; stopping")
        stop(t, :normal)

      {:error, :newer_epoch_exists} ->
        # A notification carrying an epoch *older* than our own should not
        # occur in a healthy cluster; log it as suspicious and ignore it.
        Logger.warning(
          "Distributor for epoch #{t.epoch} ignoring suspicious epoch change notification " <>
            "for older epoch #{new_epoch}"
        )

        noreply(t)

      :ok ->
        noreply(t)
    end
  end

  # Coverage demand from the placeholder: recruit a materializer for the
  # shard unless a recruitment is already in flight or the tag is inside
  # its failure-backoff window.
  def handle_cast({:coverage_demand, tag}, %State{} = t) do
    Logger.info("Distributor for epoch #{t.epoch} received coverage demand for shard tag #{inspect(tag)}")
    emit_coverage_demand(t.cluster, tag)

    t
    |> maybe_recruit(tag)
    |> noreply()
  end

  # Completion of an asynchronous recruitment attempt (cast back to
  # ourselves from the recruitment task).
  def handle_cast({:recruitment_complete, tag, {:ok, materializer, node}, duration_us}, %State{} = t) do
    emit_recruitment_succeeded(t.cluster, t.epoch, tag, node, duration_us)
    if t.placeholder, do: Placeholder.notify_covered(t.placeholder, tag, materializer)

    noreply(%{t | pending_demands: MapSet.delete(t.pending_demands, tag)})
  end

  def handle_cast({:recruitment_complete, tag, {:error, :newer_epoch_exists}, _duration_us}, %State{} = t) do
    Logger.info(
      "Distributor for epoch #{t.epoch} superseded (TSL delta for shard tag #{inspect(tag)} rejected); stopping"
    )

    stop(t, :normal)
  end

  def handle_cast({:recruitment_complete, tag, {:error, reason}, duration_us}, %State{} = t) do
    t
    |> recruitment_failed(tag, reason, duration_us)
    |> noreply()
  end

  # Internal/test seam: relay a coverage result to the placeholder.
  def handle_cast({:deliver_coverage, tag, materializer}, %State{} = t) do
    if t.placeholder, do: Placeholder.notify_covered(t.placeholder, tag, materializer)

    noreply(%{t | pending_demands: MapSet.delete(t.pending_demands, tag)})
  end

  def handle_cast({:fail_coverage, tag, reason}, %State{} = t) do
    if t.placeholder, do: Placeholder.notify_coverage_failed(t.placeholder, tag, reason)

    noreply(%{t | pending_demands: MapSet.delete(t.pending_demands, tag)})
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, director, reason}, %State{director: director} = t) do
    Logger.info("Distributor for epoch #{t.epoch} stopping: director exited (#{inspect(reason)})")

    stop(t, :normal)
  end

  def handle_info({:EXIT, placeholder, reason}, %State{placeholder: placeholder} = t) do
    Logger.warning("Distributor for epoch #{t.epoch} restarting placeholder (exited: #{inspect(reason)})")

    noreply(start_placeholder(t))
  end

  def handle_info(message, %State{} = t) do
    Logger.debug("Distributor ignoring unexpected message: #{inspect(message)}")
    noreply(t)
  end

  @impl true
  def terminate(reason, %State{} = t) do
    # A `:normal` exit signal would be ignored by the linked placeholder,
    # so shut it down explicitly to avoid orphaning it.
    if t.placeholder && Process.alive?(t.placeholder), do: Process.exit(t.placeholder, :shutdown)

    emit_distributor_stopped(t.cluster, t.epoch, reason)
    :ok
  end

  # Recruitment

  @spec maybe_recruit(State.t(), Bedrock.range_tag()) :: State.t()
  defp maybe_recruit(%State{} = t, tag) do
    cond do
      MapSet.member?(t.pending_demands, tag) ->
        # A recruitment for this tag is already in flight.
        t

      in_backoff?(t, tag) ->
        Logger.debug("Distributor for epoch #{t.epoch} ignoring demand for shard tag #{inspect(tag)} (in backoff)")

        t

      true ->
        start_recruitment(%{t | backoff: Map.delete(t.backoff, tag)}, tag)
    end
  end

  @spec in_backoff?(State.t(), Bedrock.range_tag()) :: boolean()
  defp in_backoff?(%State{backoff: backoff}, tag) do
    case Map.fetch(backoff, tag) do
      {:ok, expires_at_ms} -> System.monotonic_time(:millisecond) < expires_at_ms
      :error -> false
    end
  end

  @spec start_recruitment(State.t(), Bedrock.range_tag()) :: State.t()
  defp start_recruitment(%State{} = t, tag) do
    case shard_range(t.shard_layout, tag) do
      {:ok, _range} ->
        emit_recruitment_started(t.cluster, t.epoch, tag)
        spawn_recruitment_task(t, tag)
        %{t | pending_demands: MapSet.put(t.pending_demands, tag)}

      :error ->
        recruitment_failed(t, tag, :unknown_shard_tag, 0)
    end
  end

  # Resolves the key range for a shard tag from the layout snapshot
  # (keyed by end_key, valued by {tag, start_key}).
  @spec shard_range(TransactionSystemLayout.shard_layout(), Bedrock.range_tag()) ::
          {:ok, Bedrock.key_range()} | :error
  defp shard_range(shard_layout, tag) do
    Enum.find_value(shard_layout, :error, fn {end_key, {shard_tag, start_key}} ->
      if shard_tag == tag, do: {:ok, {start_key, end_key}}
    end)
  end

  # Runs the recruitment (foreman worker creation, epoch lock/unlock, and
  # the TSL delta to the director) off the distributor's message loop, then
  # casts the outcome back so state mutation stays serialized.
  @spec spawn_recruitment_task(State.t(), Bedrock.range_tag()) :: :ok
  defp spawn_recruitment_task(%State{} = t, tag) do
    distributor = self()
    director = t.director
    epoch = t.epoch
    context = recruitment_context(t)

    {:ok, _pid} =
      Task.start(fn ->
        started_at = System.monotonic_time(:microsecond)

        result =
          try do
            with {:ok, materializer, node} <- Recruitment.recruit(tag, context),
                 :ok <- Director.apply_tsl_delta(director, %{tag => materializer}, epoch) do
              {:ok, materializer, node}
            end
          rescue
            exception -> {:error, {:recruitment_crashed, Exception.message(exception)}}
          catch
            :exit, reason -> {:error, {:recruitment_exited, reason}}
          end

        duration_us = System.monotonic_time(:microsecond) - started_at
        GenServer.cast(distributor, {:recruitment_complete, tag, result, duration_us})
      end)

    :ok
  end

  @spec recruitment_context(State.t()) :: Recruitment.context()
  defp recruitment_context(%State{} = t) do
    Map.merge(
      %{
        cluster: t.cluster,
        epoch: t.epoch,
        durable_version: t.durable_version,
        transaction_system_layout: t.transaction_system_layout,
        node_capabilities: t.node_capabilities
      },
      t.recruitment_overrides
    )
  end

  @spec recruitment_failed(State.t(), Bedrock.range_tag(), reason :: term(), duration_us :: non_neg_integer()) ::
          State.t()
  defp recruitment_failed(%State{} = t, tag, reason, duration_us) do
    Logger.warning(
      "Distributor for epoch #{t.epoch} failed to recruit a materializer for shard tag " <>
        "#{inspect(tag)}: #{inspect(reason)}"
    )

    emit_recruitment_failed(t.cluster, t.epoch, tag, reason, duration_us)
    if t.placeholder, do: Placeholder.notify_coverage_failed(t.placeholder, tag, reason)

    %{
      t
      | pending_demands: MapSet.delete(t.pending_demands, tag),
        backoff: Map.put(t.backoff, tag, System.monotonic_time(:millisecond) + t.backoff_ms)
    }
  end

  # The placeholder is linked (so it dies with the distributor) and its
  # exit is caught via trap_exit above so the distributor can restart it.
  #
  # A restarted placeholder loses its `covered` and `demanded` state while
  # `pending_demands` here survives, so the recruitment flow (bedrock-q67.5)
  # must tolerate duplicate `{:coverage_demand, tag}` casts for tags already
  # pending, and should re-deliver coverage if a delivery races a restart
  # (a `notify_covered` cast to the dead pid is silently dropped).
  @spec start_placeholder(State.t()) :: State.t()
  defp start_placeholder(%State{} = t) do
    {:ok, placeholder} =
      GenServer.start_link(
        Placeholder.Server,
        {t.cluster, self(), t.shard_layout, Placeholder.default_hold_ms()}
      )

    %{t | placeholder: placeholder}
  end
end
