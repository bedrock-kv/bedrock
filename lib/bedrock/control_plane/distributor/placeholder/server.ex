defmodule Bedrock.ControlPlane.Distributor.Placeholder.Server do
  @moduledoc """
  GenServer implementation of the coverage placeholder.

  Accepts the exact read-path call shapes real materializer servers
  handle — `{:get, key_or_selector, version, opts}` and
  `{:get_range, start, end, version, opts}` — and either forwards them
  to a live materializer (when one is known for the shard tag) or parks
  them in a deadline-bounded waiting list while signaling coverage
  demand to the distributor.

  Deadline expiry is timer-driven: every mutation of the waiting list
  reschedules a `Process.send_after/3` for the earliest deadline, and
  expired entries shed `{:error, :unavailable}` — retryable and
  routing-invalidating at the client, which is exactly right for a key
  whose coverage has not arrived.
  """

  use GenServer

  import Bedrock.ControlPlane.Distributor.Telemetry,
    only: [
      emit_placeholder_parked: 2,
      emit_placeholder_forwarded: 2,
      emit_placeholder_shed: 4,
      emit_placeholder_drained: 3
    ]

  import Bedrock.Internal.GenServer.Replies

  alias Bedrock.ControlPlane.Distributor.Placeholder
  alias Bedrock.ControlPlane.Distributor.Placeholder.State
  alias Bedrock.DataPlane.Materializer
  alias Bedrock.Internal.WaitingList
  alias Bedrock.KeySelector

  @doc false
  @spec child_spec(
          opts :: [
            cluster: module(),
            distributor: pid(),
            shard_layout: %{Bedrock.key() => {Bedrock.range_tag(), Bedrock.key()}},
            hold_ms: pos_integer(),
            otp_name: atom()
          ]
        ) :: Supervisor.child_spec()
  def child_spec(opts) do
    cluster = opts[:cluster] || raise "Missing :cluster option"
    distributor = opts[:distributor] || raise "Missing :distributor option"
    shard_layout = opts[:shard_layout] || %{}
    hold_ms = opts[:hold_ms] || Placeholder.default_hold_ms()

    start_opts =
      case opts[:otp_name] do
        nil -> []
        otp_name -> [name: otp_name]
      end

    %{
      id: {__MODULE__, cluster},
      start: {GenServer, :start_link, [__MODULE__, {cluster, distributor, shard_layout, hold_ms}, start_opts]},
      restart: :temporary
    }
  end

  @impl true
  def init({cluster, distributor, shard_layout, hold_ms}) do
    {:ok, %State{cluster: cluster, distributor: distributor, shard_layout: shard_layout, hold_ms: hold_ms}}
  end

  @impl true
  def handle_call({:get, key_or_selector, _version, opts} = request, from, %State{} = t),
    do: handle_read(t, racing_key(key_or_selector), request, opts, from)

  def handle_call({:get_range, start_key_or_selector, _end, _version, opts} = request, from, %State{} = t),
    do: handle_read(t, racing_key(start_key_or_selector), request, opts, from)

  # The placeholder is a wire boundary: real workers also speak
  # lock/info/recovery shapes, and a future or operational caller
  # reaching this ref with one must get a refusal, not a
  # FunctionClauseError that kills every parked request. (Recovery never
  # targets placeholder members — locking draws from the old TSL and
  # re-adoption filters by locked recovery info — so this clause serves
  # foreign callers, not a precluded internal path.)
  def handle_call(_unsupported, _from, %State{} = t), do: reply(t, {:error, :unsupported})

  @impl true
  def handle_cast({:covered, tag, materializer}, %State{} = t) do
    {waiting, entries} = WaitingList.remove_all(t.waiting, tag)

    Enum.each(entries, fn {_deadline, reply_fn, request} ->
      {:ok, _pid} = Task.start(fn -> reply_fn.(reissue(materializer, request)) end)
    end)

    emit_placeholder_drained(t.cluster, tag, length(entries))

    t = %{
      t
      | waiting: waiting,
        covered: Map.put(t.covered, tag, materializer),
        demanded: MapSet.delete(t.demanded, tag)
    }

    t |> reschedule_expiry() |> noreply()
  end

  def handle_cast({:uncovered, tag}, %State{} = t),
    do: noreply(%{t | covered: Map.delete(t.covered, tag), demanded: MapSet.delete(t.demanded, tag)})

  def handle_cast({:coverage_failed, tag, reason}, %State{} = t) do
    {waiting, entries} = WaitingList.remove_all(t.waiting, tag)
    WaitingList.reply_to_expired(entries, {:error, :unavailable})

    emit_placeholder_shed(t.cluster, tag, length(entries), reason)

    t = %{t | waiting: waiting, demanded: MapSet.delete(t.demanded, tag)}
    t |> reschedule_expiry() |> noreply()
  end

  @impl true
  def handle_info(:expire_waiting, %State{} = t) do
    {waiting, expired} = WaitingList.expire(t.waiting)
    WaitingList.reply_to_expired(expired, {:error, :unavailable})

    if expired != [], do: emit_placeholder_shed(t.cluster, nil, length(expired), :deadline_expired)

    # This message may be stale — a mutation since the timer fired may
    # have re-armed `expiry_timer`. Rescheduling cancels rather than
    # leaking a duplicate timer; expiry itself is idempotent.
    %{t | waiting: waiting} |> reschedule_expiry() |> noreply()
  end

  def handle_info(_message, %State{} = t), do: noreply(t)

  # Read handling

  defp handle_read(%State{} = t, key, request, opts, from) do
    case State.resolve_tag(t, key) do
      {:ok, tag} ->
        case Map.fetch(t.covered, tag) do
          {:ok, materializer} -> forward(t, tag, materializer, request, from)
          :error -> park(t, tag, request, opts, from)
        end

      {:error, :no_shard} ->
        reply(t, {:error, :unavailable})
    end
  end

  defp forward(%State{} = t, tag, materializer, request, from) do
    emit_placeholder_forwarded(t.cluster, tag)

    {:ok, _pid} = Task.start(fn -> GenServer.reply(from, reissue(materializer, request)) end)

    noreply(t)
  end

  defp park(%State{} = t, tag, request, opts, from) do
    budget_ms = State.parking_budget_ms(t, opts[:timeout])
    reply_fn = fn result -> GenServer.reply(from, result) end
    {waiting, _next_timeout} = WaitingList.insert(t.waiting, tag, request, reply_fn, budget_ms)

    emit_placeholder_parked(t.cluster, tag)

    t
    |> struct!(waiting: waiting)
    |> signal_demand(tag)
    |> reschedule_expiry()
    |> noreply()
  end

  defp signal_demand(%State{} = t, tag) do
    if MapSet.member?(t.demanded, tag) do
      t
    else
      GenServer.cast(t.distributor, {:coverage_demand, tag})
      %{t | demanded: MapSet.put(t.demanded, tag)}
    end
  end

  # Re-issue a parked or forwarded request against a live materializer,
  # using the same client API the original caller used.
  defp reissue(materializer, {:get, key_or_selector, version, opts}),
    do: Materializer.get(materializer, key_or_selector, version, opts)

  defp reissue(materializer, {:get_range, start_sel, end_sel, version, opts}),
    do: Materializer.get_range(materializer, start_sel, end_sel, version, opts)

  defp racing_key(%KeySelector{key: key}), do: key
  defp racing_key(key) when is_binary(key), do: key

  # A single timer armed for the earliest deadline across the waiting
  # list, rescheduled on every mutation.
  defp reschedule_expiry(%State{} = t) do
    if t.expiry_timer, do: Process.cancel_timer(t.expiry_timer)

    case WaitingList.next_timeout(t.waiting) do
      :infinity -> %{t | expiry_timer: nil}
      timeout_ms -> %{t | expiry_timer: Process.send_after(self(), :expire_waiting, timeout_ms)}
    end
  end
end
