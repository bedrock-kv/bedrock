defmodule Bedrock.Cluster.Link.Server do
  @moduledoc false

  use GenServer
  use Bedrock.Internal.TimerManagement

  import Bedrock.Cluster.Link.Discovery,
    only: [
      change_coordinator: 2,
      find_a_live_coordinator: 1
    ]

  import Bedrock.Cluster.Link.Telemetry
  import Bedrock.Internal.GenServer.Replies

  alias Bedrock.Cluster.Descriptor
  alias Bedrock.Cluster.Link.RoutingCache
  alias Bedrock.Cluster.Link.State
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout

  @spec child_spec(
          opts :: [
            cluster: module(),
            descriptor: Descriptor.t(),
            path_to_descriptor: Path.t(),
            otp_name: atom(),
            capabilities: [atom()],
            mode: :active | :passive
          ]
        ) :: Supervisor.child_spec()
  def child_spec(opts) do
    cluster = opts[:cluster] || raise "Missing :cluster option"
    descriptor = opts[:descriptor] || raise "Missing :descriptor option"
    path_to_descriptor = opts[:path_to_descriptor] || raise "Missing :path_to_descriptor option"
    otp_name = opts[:otp_name] || raise "Missing :otp_name option"
    capabilities = opts[:capabilities] || raise "Missing :capabilities option"
    mode = opts[:mode] || :active

    %{
      id: otp_name,
      start: {
        GenServer,
        :start_link,
        [
          __MODULE__,
          {cluster, path_to_descriptor, descriptor, mode, capabilities},
          [name: otp_name]
        ]
      },
      restart: :permanent
    }
  end

  @doc false
  @impl true
  @spec init({
          cluster :: module(),
          path_to_descriptor :: Path.t(),
          descriptor :: Descriptor.t(),
          mode :: :active | :passive,
          capabilities :: [atom()]
        }) :: {:ok, State.t(), {:continue, :find_a_live_coordinator}}
  def init({cluster, path_to_descriptor, descriptor, mode, capabilities}) do
    trace_started(cluster)

    then(
      %State{
        node: Node.self(),
        cluster: cluster,
        # The Link owns the table; every process on the node READS it
        # directly. One writer keeps invalidation ordered against the
        # pushes that trigger it.
        routing_table: RoutingCache.new(cluster.otp_name(:link_routing)),
        descriptor: descriptor,
        path_to_descriptor: path_to_descriptor,
        known_coordinator: :unavailable,
        transaction_system_layout: nil,
        mode: mode,
        capabilities: capabilities
      },
      &{:ok, &1, {:continue, :find_a_live_coordinator}}
    )
  end

  @doc false
  @impl true
  @spec handle_continue(:find_a_live_coordinator, State.t()) :: {:noreply, State.t()}
  def handle_continue(:find_a_live_coordinator, t) do
    t
    |> find_a_live_coordinator()
    |> case do
      {t, :ok} -> noreply(t)
      {t, {:error, :unavailable}} -> noreply(t)
    end
  end

  @doc false
  @impl true
  @spec handle_call(:get_known_coordinator, GenServer.from(), State.t()) ::
          {:reply, {:ok, term()} | {:error, :unavailable}, State.t()}
  def handle_call(:get_known_coordinator, _, t) do
    case t.known_coordinator do
      :unavailable -> reply(t, {:error, :unavailable})
      coordinator -> reply(t, {:ok, coordinator})
    end
  end

  @spec handle_call(:get_transaction_system_layout, GenServer.from(), State.t()) ::
          {:reply, {:ok, term()} | {:error, :unavailable}, State.t()}
  def handle_call(:get_transaction_system_layout, _, t) do
    case t.transaction_system_layout do
      nil -> reply(t, {:error, :unavailable})
      tsl -> reply(t, {:ok, tsl})
    end
  end

  @spec handle_call(:get_descriptor, GenServer.from(), State.t()) ::
          {:reply, {:ok, Descriptor.t()}, State.t()}
  def handle_call(:get_descriptor, _, t) do
    reply(t, {:ok, t.descriptor})
  end

  # The node-wide routing cache (FDB DatabaseContext locationCache): a
  # partial coalescing index of covering entries. The Link only stores;
  # callers fetch the single covering entry from a commit proxy on miss
  # and cast it back. No TTL - entries live until invalidated or a wiring
  # push drops them; staleness is backstopped by the client retry loop.
  # Synchronous: when the reply arrives the stale entries are gone -
  # ordering by construction, not by accident of intervening calls.
  # Coarse (whole index): failures are rare and simple beats surgical.
  @spec handle_call(:invalidate_routing, GenServer.from(), State.t()) :: {:reply, :ok, State.t()}
  def handle_call(:invalidate_routing, _, t) do
    RoutingCache.clear(t.routing_table)
    reply(t, :ok)
  end

  @doc false
  @impl true
  @spec handle_cast({:cache_routing_entry, {Bedrock.key(), Bedrock.key(), term()}}, State.t()) ::
          {:noreply, State.t()}
  def handle_cast({:cache_routing_entry, {start_key, end_key, ref}}, t) do
    RoutingCache.insert(t.routing_table, start_key, end_key, ref)
    noreply(t)
  end

  @doc false
  @impl true
  @spec handle_info({:timeout, :find_a_live_coordinator}, State.t()) ::
          {:noreply, State.t(), {:continue, :find_a_live_coordinator}}
  def handle_info({:timeout, :find_a_live_coordinator}, t), do: noreply(t, continue: :find_a_live_coordinator)

  # A coordinator we have abandoned still holds us in its subscriber set —
  # there is no unsubscribe — so a partitioned-but-alive old leader can
  # push the wiring of its own epoch after we have moved on. Wiring only
  # ever moves forward: a push carrying an epoch we have already passed is
  # dropped, not installed. Compared against the high-water mark rather
  # than the cached layout, so a clear can't be used to slip an old layout
  # in behind it.
  @spec handle_info({:tsl_updated, term()}, State.t()) :: {:noreply, State.t()}
  def handle_info({:tsl_updated, %{epoch: epoch}}, %{wiring_epoch: seen} = t) when is_integer(seen) and epoch < seen,
    do: noreply(t)

  def handle_info({:tsl_updated, new_tsl}, t) do
    # Update cached TSL when coordinator broadcasts updates, and forward to
    # this node's foreman (if any), which relays it to hosted workers: a
    # newly durable layout is what workers self-detect displacement
    # against.
    case Process.whereis(t.cluster.otp_name(:foreman)) do
      nil -> :ok
      foreman -> send(foreman, {:tsl_updated, new_tsl})
    end

    # A wiring push means a recovery happened: drop the routing cache so
    # new-epoch wiring can never pair with old-epoch routing.
    RoutingCache.clear(t.routing_table)
    noreply(%{t | transaction_system_layout: new_tsl, wiring_epoch: wiring_epoch(new_tsl, t.wiring_epoch)})
  end

  @spec handle_info({:DOWN, reference(), :process, term(), term()}, State.t()) ::
          {:noreply, State.t()} | {:noreply, State.t(), {:continue, :find_a_live_coordinator}}
  def handle_info({:DOWN, _ref, :process, name, _reason}, t) do
    coordinator_matches =
      case t.known_coordinator do
        coordinator_ref when coordinator_ref != :unavailable ->
          name == coordinator_ref ||
            (is_tuple(name) and elem(name, 0) == t.cluster.otp_name(:coordinator))

        :unavailable ->
          is_tuple(name) and elem(name, 0) == t.cluster.otp_name(:coordinator)
      end

    if coordinator_matches do
      t
      |> change_coordinator(:unavailable)
      |> noreply(continue: :find_a_live_coordinator)
    else
      noreply(t)
    end
  end

  # A clear carries no epoch, so it can't be attributed to a coordinator:
  # it drops the layout but never lowers the mark.
  @spec wiring_epoch(TransactionSystemLayout.t() | nil, Bedrock.epoch() | nil) :: Bedrock.epoch() | nil
  defp wiring_epoch(nil, seen), do: seen
  defp wiring_epoch(%{epoch: epoch}, _seen), do: epoch
end
