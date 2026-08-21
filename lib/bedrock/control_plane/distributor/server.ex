defmodule Bedrock.ControlPlane.Distributor.Server do
  @moduledoc false
  use GenServer

  alias Bedrock.ControlPlane.Distributor.State
  alias Bedrock.ControlPlane.Distributor.Transactions

  require Logger

  # FDB's MOVEKEYS_LOCK_POLLING_DELAY: a superseded distributor exits
  # within seconds even when idle, instead of waiting to lose a commit.
  @poll_interval_ms 5_000

  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start, [opts]},
      restart: :temporary
    }
  end

  @doc """
  Starts an unlinked distributor: the director supervises by monitor
  (ceded `:normal` exits are not re-recruited; failures are), and the
  distributor watches the director back — a per-epoch singleton dies
  with its epoch.
  """
  @spec start(keyword()) :: {:ok, pid()} | {:error, term()}
  def start(opts) do
    GenServer.start(__MODULE__, opts, name: Keyword.get(opts, :otp_name))
  end

  @impl true
  def init(opts) do
    cluster = Keyword.fetch!(opts, :cluster)
    epoch = Keyword.fetch!(opts, :epoch)
    director = Keyword.fetch!(opts, :director)

    deps =
      Keyword.get_lazy(opts, :deps, fn ->
        Transactions.deps_for(
          cluster,
          epoch,
          Keyword.fetch!(opts, :sequencer),
          Keyword.fetch!(opts, :proxies)
        )
      end)

    state = %State{
      cluster: cluster,
      epoch: epoch,
      director: director,
      director_monitor: Process.monitor(director),
      deps: deps,
      poll_interval_ms: Keyword.get(opts, :poll_interval_ms, @poll_interval_ms)
    }

    {:ok, state, {:continue, :take_lock}}
  end

  # Lock first, everything else second (FDB's DD startup order): a
  # distributor that cannot own the fence must not exist. A commit abort
  # means a newer owner won — cede (:normal, the director does not
  # re-recruit); a transient failure stops :shutdown so the director's
  # retry recruits a fresh instance.
  @impl true
  def handle_continue(:take_lock, %State{} = t) do
    case Transactions.take_lock(t.deps) do
      {:ok, lock} ->
        Logger.info("Bedrock distributor (epoch #{t.epoch}): lock taken")
        {:noreply, schedule_poll(%{t | lock: lock})}

      {:error, :superseded} ->
        Logger.info("Bedrock distributor (epoch #{t.epoch}): superseded at take; ceding")
        {:stop, :normal, t}

      {:error, reason} ->
        {:stop, {:shutdown, {:lock_take_failed, reason}}, t}
    end
  end

  # The poll-to-die loop: a read-only fence evaluation every poll
  # interval. Supersession cedes; an unavailable read is not a verdict —
  # the next tick retries.
  @impl true
  def handle_info(:poll_lock, %State{} = t) do
    case Transactions.poll_verdict(t.lock, t.deps) do
      :superseded ->
        Logger.info("Bedrock distributor (epoch #{t.epoch}): lock superseded; ceding")
        {:stop, :normal, t}

      _ok_or_unavailable ->
        {:noreply, schedule_poll(t)}
    end
  end

  # A per-epoch singleton dies with its epoch: the director's death is a
  # recovery in progress, and the next epoch's director recruits the
  # next distributor.
  def handle_info({:DOWN, ref, :process, _pid, _reason}, %State{director_monitor: ref} = t), do: {:stop, :normal, t}

  defp schedule_poll(%State{} = t) do
    Process.send_after(self(), :poll_lock, t.poll_interval_ms)
    t
  end
end
