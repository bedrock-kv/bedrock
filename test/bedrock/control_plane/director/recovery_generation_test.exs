defmodule Bedrock.ControlPlane.Director.RecoveryGenerationTest do
  @moduledoc """
  A recovery attempt is a generation. When the director abandons a stalled
  attempt and retries, the abandoned attempt's transaction-system processes
  must die before the retry recruits its own — otherwise two live proxy
  generations share one epoch, each beating against its own sequencer and
  pushing empty transactions into the same logs.

  FDB keys this off `recoveryCount`: every attempt bumps it
  (`newState.recoveryCount++`, ClusterRecovery.actor.cpp:1584) before it
  recruits, and a commit proxy that sees a `ServerDBInfo` whose
  `recoveryCount` is at or past its own without naming it throws
  `worker_removed()` (`updateLocalDbInfo`,
  CommitProxyServer.actor.cpp:4199-4202). Bedrock retries in process inside
  one epoch, so the director is the one that has to end the old generation.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Bedrock.ControlPlane.Config.RecoveryAttempt
  alias Bedrock.ControlPlane.Director.Recovery
  alias Bedrock.ControlPlane.Director.Recovery.MonitoringPhase
  alias Bedrock.ControlPlane.Director.State
  alias Bedrock.DataPlane.CommitProxy
  alias Bedrock.DataPlane.CommitProxy.ResolverLayout
  alias Bedrock.DataPlane.CommitProxy.Server, as: CommitProxyServer
  alias Bedrock.DataPlane.Resolver.Server, as: ResolverServer
  alias Bedrock.DataPlane.Sequencer.Server, as: SequencerServer
  alias Bedrock.DataPlane.Version

  @heartbeat_ms 100

  defmodule TestCluster do
    @moduledoc false
    def otp_name(component) when is_atom(component), do: :"recovery_generation_test_#{component}"
  end

  # Fake log that accepts every push and tells the test about it, so an
  # abandoned proxy's heartbeat is observable from outside.
  defmodule ReportingLog do
    @moduledoc false
    use GenServer

    def start_link(test_pid), do: GenServer.start_link(__MODULE__, test_pid)

    @impl true
    def init(test_pid), do: {:ok, test_pid}

    @impl true
    def handle_call({:push, transaction, last_commit_version, _kcv}, _from, test_pid) do
      send(test_pid, {:log_push, transaction, last_commit_version})
      {:reply, :ok, test_pid}
    end
  end

  defmodule StubCoordinator do
    @moduledoc false
    use GenServer

    def start_link, do: GenServer.start_link(__MODULE__, :ok)

    @impl true
    def init(:ok), do: {:ok, :ok}

    @impl true
    def handle_call(:fetch_service_directory, _from, s), do: {:reply, {:ok, %{}}, s}

    @impl true
    def handle_cast(_msg, s), do: {:noreply, s}
  end

  setup do
    sup = start_supervised!({DynamicSupervisor, name: TestCluster.otp_name(:sup), strategy: :one_for_one})
    {:ok, coordinator} = StubCoordinator.start_link()

    director = self()
    epoch = 7
    lock_token = :crypto.strong_rand_bytes(32)

    {:ok, sequencer} =
      DynamicSupervisor.start_child(
        sup,
        SequencerServer.child_spec(
          cluster: TestCluster,
          otp_name: TestCluster.otp_name(:sequencer),
          director: director,
          epoch: epoch,
          last_committed_version: Version.zero()
        )
      )

    {:ok, resolver} =
      DynamicSupervisor.start_child(
        sup,
        ResolverServer.child_spec(
          key_range: {"", <<0xFF, 0xFF>>},
          epoch: epoch,
          last_version: Version.zero(),
          director: director,
          cluster: TestCluster,
          commit_proxy_count: 1
        )
      )

    {:ok, proxy} =
      DynamicSupervisor.start_child(
        sup,
        CommitProxyServer.child_spec(
          cluster: TestCluster,
          director: director,
          epoch: epoch,
          instance: 0,
          max_latency_in_ms: 1,
          max_per_batch: 10,
          empty_transaction_timeout_ms: @heartbeat_ms,
          lock_token: lock_token
        )
      )

    log = start_supervised!({ReportingLog, self()})

    %{
      coordinator: coordinator,
      epoch: epoch,
      lock_token: lock_token,
      log: log,
      proxy: proxy,
      resolver: resolver,
      sequencer: sequencer
    }
  end

  # The attempt as it stands when a recovery stalls after the topology
  # phase: sequencer, resolver and an already-unlocked proxy, all monitored
  # by the director the way the monitoring phase leaves them.
  defp stalled_attempt(ctx) do
    %{
      RecoveryAttempt.new(TestCluster, ctx.epoch, DateTime.utc_now())
      | sequencer: ctx.sequencer,
        proxies: [ctx.proxy],
        resolvers: [{"", ctx.resolver}],
        logs: %{},
        transaction_services: %{}
    }
  end

  defp monitored(attempt) do
    {monitored_attempt, Bedrock.ControlPlane.Director.Recovery.PersistencePhase} =
      MonitoringPhase.execute(attempt, %{})

    monitored_attempt
  end

  defp director_state(ctx, attempt) do
    %State{
      state: :recovery,
      cluster: TestCluster,
      epoch: ctx.epoch,
      coordinator: ctx.coordinator,
      lock_token: ctx.lock_token,
      node_capabilities: %{},
      prior_core_state: %{logs: %{}},
      config: %{
        coordinators: [],
        parameters: %{
          desired_logs: 1,
          desired_replication_factor: 1,
          desired_commit_proxies: 1
        }
      },
      services: %{},
      recovery_attempt: attempt
    }
  end

  defp unlock(ctx) do
    resolver_layout = ResolverLayout.from_layout(%{resolvers: [{"", ctx.resolver}]})

    routing_snapshot = %{
      shard_layout: %{<<0xFF, 0xFF>> => {0, <<>>}},
      log_map: %{0 => "log_1"},
      log_services: %{"log_1" => ctx.log},
      materializers: %{0 => %{"wkr_sys" => "n1@host"}},
      replication_factor: 1
    }

    :ok = CommitProxy.recover_from(ctx.proxy, ctx.lock_token, ctx.sequencer, resolver_layout, routing_snapshot)
  end

  describe "a stalled recovery attempt" do
    test "stops beating, at the stall — no retry needed to end it", ctx do
      unlock(ctx)

      # The abandoned generation is live: it beats on its own, without any
      # client, into its own logs (bedrock-q67.36).
      assert_receive {:log_push, _transaction, _last_commit_version}, @heartbeat_ms * 5

      # Nothing schedules a recovery retry — it takes a cluster event — so
      # the stall itself has to be what ends the generation.
      result = ctx |> director_state(monitored(stalled_attempt(ctx))) |> run(&Recovery.do_recovery/1)

      assert result.state == :recovery

      # The abandoned generation no longer pushes into the epoch's logs…
      flush_log_pushes()
      refute_receive {:log_push, _transaction, _last_commit_version}, @heartbeat_ms * 3

      # …because it is gone.
      refute Process.alive?(ctx.proxy), "the abandoned attempt's commit proxy is still running"
      refute Process.alive?(ctx.resolver), "the abandoned attempt's resolver is still running"
      refute Process.alive?(ctx.sequencer), "the abandoned attempt's sequencer is still running"
    end

    test "is retired in the attempt the retry then builds on", ctx do
      result = ctx |> director_state(monitored(stalled_attempt(ctx))) |> run(&Recovery.try_to_recover/1)

      # The retry did happen — it is attempt 2, and it stalled again — and
      # it carries none of the previous generation's processes.
      assert result.recovery_attempt.attempt == 2
      assert result.recovery_attempt.sequencer == nil
      assert result.recovery_attempt.proxies == []
      assert result.recovery_attempt.resolvers == []
      refute Process.alive?(ctx.proxy)
    end

    test "does not report its retired components as component failures", ctx do
      _result = ctx |> director_state(monitored(stalled_attempt(ctx))) |> run(&Recovery.do_recovery/1)

      # The monitors the monitoring phase installed are released with a
      # flush: a deliberate retirement is not a component failure, and the
      # director's :DOWN clause would stop the epoch over one.
      refute_receive {:DOWN, _ref, :process, _pid, _reason}, 200
    end

    test "that recruited nothing retires cleanly", ctx do
      attempt = RecoveryAttempt.new(TestCluster, ctx.epoch, DateTime.utc_now())
      result = ctx |> director_state(attempt) |> run(&Recovery.try_to_recover/1)

      assert result.recovery_attempt.attempt == 2
      assert Process.alive?(ctx.proxy)
    end
  end

  defp run(state, fun) do
    holder = self()
    capture_log(fn -> send(holder, {:ran, fun.(state)}) end)

    receive do
      {:ran, result} -> result
    end
  end

  defp flush_log_pushes do
    receive do
      {:log_push, _transaction, _last_commit_version} -> flush_log_pushes()
    after
      0 -> :ok
    end
  end
end
