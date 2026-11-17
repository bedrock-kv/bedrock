defmodule Bedrock.Service.Foreman.StaleHealthTest do
  use ExUnit.Case, async: false

  alias Bedrock.ObjectStorage.LocalFilesystem
  alias Bedrock.Service.Foreman
  alias Bedrock.Service.Foreman.Server

  @moduletag :tmp_dir
  @worker_id "aaaaaaaa"

  defmodule Cluster do
    @moduledoc false
    def name, do: "stale_health_test"
    def otp_name_for_worker(id), do: :"stale_health_worker_#{id}"
    def otp_name(role), do: :"stale_health_#{role}"
  end

  defmodule ControlledWorker do
    @moduledoc false
    use GenServer
    use Bedrock.Service.WorkerBehaviour, kind: :materializer

    def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: opts[:otp_name])
    def child_spec(opts), do: %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}, restart: :transient}

    @impl true
    def init(opts) do
      send(:stale_health_controller, {:started, self(), opts})
      {:ok, opts, {:continue, :gate_startup}}
    end

    @impl true
    def handle_continue(:gate_startup, opts) do
      receive do
        :report_startup -> report(opts, {:ok, self()})
      end

      {:noreply, opts}
    end

    @impl true
    def handle_info({:report, health}, opts) do
      report(opts, health)
      {:noreply, opts}
    end

    def handle_info({:capture, :health}, opts) do
      Foreman.report_health(:stale_health_controller, opts[:id], {:ok, self()})
      {:noreply, opts}
    end

    def handle_info({:capture, :retirement}, opts) do
      Foreman.worker_retired(:stale_health_controller, opts[:id])
      {:noreply, opts}
    end

    def handle_info(:retire_and_exit, opts) do
      Foreman.worker_retired(opts[:foreman], opts[:id])
      {:stop, :normal, opts}
    end

    def handle_info(:retire, opts) do
      Foreman.worker_retired(opts[:foreman], opts[:id])
      send(:stale_health_controller, {:retired, self()})
      {:noreply, opts}
    end

    defp report(opts, health) do
      Foreman.report_health(opts[:foreman], opts[:id], health)
      send(:stale_health_controller, {:reported, self(), health})
    end
  end

  defmodule TerminationGate do
    @moduledoc false
    use GenServer

    def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: opts[:name])
    @impl true
    def init(opts), do: {:ok, opts}
    @impl true
    def handle_call({:terminate_child, pid} = request, _from, opts) do
      if opts[:gate] do
        token = make_ref()
        send(opts[:controller], {:termination_resolved, self(), token, pid})

        receive do
          {:forward_termination, ^token} -> :ok
        end
      end

      result = GenServer.call(opts[:backing], request, :infinity)
      send(opts[:controller], {:termination_result, pid, result})
      {:reply, result, Keyword.put(opts, :gate, if(opts[:gate] == :always, do: :always, else: false))}
    end

    def handle_call({:gate, value}, _from, opts), do: {:reply, :ok, Keyword.put(opts, :gate, value)}

    def handle_call(request, _from, opts), do: {:reply, GenServer.call(opts[:backing], request, :infinity), opts}
  end

  setup %{tmp_dir: path} do
    Process.register(self(), :stale_health_controller)
    worker_path = Path.join(path, @worker_id)
    File.mkdir_p!(worker_path)

    File.write!(
      Path.join(worker_path, "manifest.json"),
      Jason.encode!(%{
        id: @worker_id,
        cluster: Cluster.name(),
        worker: ControlledWorker |> Atom.to_string() |> String.trim_leading("Elixir."),
        params: %{}
      })
    )

    supervisor =
      start_supervised!(
        {DynamicSupervisor, strategy: :one_for_one, name: Cluster.otp_name(:worker_supervisor), max_restarts: 100}
      )

    foreman =
      start_supervised!(
        {Server,
         cluster: Cluster,
         path: path,
         capabilities: [:materializer],
         otp_name: Cluster.otp_name(:foreman),
         object_storage: {LocalFilesystem, root: Path.join(path, "objects")}}
      )

    assert_receive {:started, worker, opts}, 5_000
    send(worker, :report_startup)
    assert_receive {:reported, ^worker, {:ok, ^worker}}, 5_000
    assert :pong = GenServer.call(foreman, :ping)
    %{foreman: foreman, worker: worker, opts: opts, supervisor: supervisor, worker_path: worker_path}
  end

  test "removal queued before automatic restart health leaves no membership and the same Foreman alive", ctx do
    :ok = :sys.suspend(ctx.foreman)
    reply_ref = make_ref()

    try do
      send(ctx.foreman, {:"$gen_call", {self(), reply_ref}, {:remove_worker, @worker_id}})
      Process.exit(ctx.worker, :kill)
      assert_receive {:started, replacement, _}, 5_000
      send(replacement, :report_startup)
      assert_receive {:reported, ^replacement, {:ok, ^replacement}}, 5_000
      {:messages, queued} = Process.info(ctx.foreman, :messages)

      assert Enum.find_index(queued, &match?({:"$gen_call", _, {:remove_worker, @worker_id}}, &1)) <
               Enum.find_index(queued, &match?({:"$gen_cast", {:worker_health, @worker_id, _, _}}, &1))
    after
      :sys.resume(ctx.foreman)
    end

    assert_receive {^reply_ref, :ok}, 5_000
    assert :pong = GenServer.call(ctx.foreman, :ping)
    assert :sys.get_state(ctx.foreman).workers == %{}
    refute File.exists?(ctx.worker_path)
  end

  test "explicit removal terminates an automatic replacement whose startup health is still gated", ctx do
    :ok = :sys.suspend(ctx.foreman)
    reply_ref = make_ref()

    replacement =
      try do
        send(ctx.foreman, {:"$gen_call", {self(), reply_ref}, {:remove_worker, @worker_id}})
        Process.exit(ctx.worker, :kill)
        assert_receive {:started, replacement, _}, 5_000
        replacement
      after
        :sys.resume(ctx.foreman)
      end

    assert_receive {^reply_ref, :ok}, 5_000
    refute Process.alive?(replacement), "removal must target the current registered child, not its dead predecessor"
    assert :sys.get_state(ctx.foreman).workers == %{}
  end

  test "restart after termination PID resolution never deletes a surviving replacement", ctx do
    Process.unregister(Cluster.otp_name(:worker_supervisor))

    gate =
      start_supervised!(
        {TerminationGate,
         name: Cluster.otp_name(:worker_supervisor), backing: ctx.supervisor, controller: self(), gate: true}
      )

    task = Task.async(fn -> Foreman.remove_worker(ctx.foreman, @worker_id) end)
    assert_receive {:termination_resolved, ^gate, token, resolved}, 5_000
    assert resolved == ctx.worker
    Process.exit(resolved, :kill)
    assert_receive {:started, replacement, _}, 5_000
    assert replacement != resolved
    assert Process.whereis(Cluster.otp_name_for_worker(@worker_id)) == replacement
    send(gate, {:forward_termination, token})
    assert_receive {:termination_result, ^resolved, {:error, :not_found}}, 5_000
    result = Task.await(task, 5_000)
    state = :sys.get_state(ctx.foreman)

    assert result != :ok or not Process.alive?(replacement),
           "stale terminate_child returned not_found while its automatic replacement still owns the directory"

    if result != :ok do
      assert Map.has_key?(state.workers, @worker_id)
      assert File.exists?(Path.join(ctx.worker_path, "manifest.json"))
      assert :ok = Foreman.remove_worker(ctx.foreman, @worker_id)
    end

    refute Process.alive?(replacement)
    refute File.exists?(ctx.worker_path)
  end

  test "unresolved repeated restarts retain retryable membership and disk", ctx do
    Process.unregister(Cluster.otp_name(:worker_supervisor))

    gate =
      start_supervised!(
        {TerminationGate,
         name: Cluster.otp_name(:worker_supervisor), backing: ctx.supervisor, controller: self(), gate: :always}
      )

    task = Task.async(fn -> Foreman.remove_worker(ctx.foreman, @worker_id) end)
    {result, replacement, attempts} = churn_until_removal_reply(task.ref, gate)
    Process.demonitor(task.ref, [:flush])
    assert attempts <= 3, "termination must have a bounded retry budget"
    assert result == {:error, :worker_shutdown_unresolved}
    assert Map.has_key?(:sys.get_state(ctx.foreman).workers, @worker_id)
    assert File.exists?(Path.join(ctx.worker_path, "manifest.json"))
    assert Process.alive?(replacement)
    :ok = GenServer.call(gate, {:gate, false})
    assert :ok = Foreman.remove_worker(ctx.foreman, @worker_id)
    refute Process.alive?(replacement)
    refute File.exists?(ctx.worker_path)
  end

  test "retirement while abnormal restart is pending preserves directory until supervisor ownership is removed", ctx do
    :ok = :sys.suspend(ctx.supervisor)

    on_exit(fn ->
      safe_resume(ctx.supervisor)
      safe_resume(ctx.foreman)
    end)

    :ok = :sys.suspend(ctx.foreman)
    send(ctx.worker, :retire)
    assert_receive {:retired, old} when old == ctx.worker
    ref = Process.monitor(ctx.worker)
    Process.exit(ctx.worker, :kill)
    assert_receive {:DOWN, ^ref, :process, _, :killed}
    assert Process.whereis(Cluster.otp_name_for_worker(@worker_id)) == nil
    :ok = :sys.resume(ctx.foreman)
    # Both the actual EXIT and the subsequent termination call are queued at the
    # real DynamicSupervisor; EXIT precedes the call and creates a replacement.
    wait_for_termination_call(ctx.supervisor)
    :ok = :sys.resume(ctx.supervisor)
    assert_receive {:started, replacement, _}, 5_000
    assert :pong = GenServer.call(ctx.foreman, :ping)
    state = :sys.get_state(ctx.foreman)

    assert Map.has_key?(state.workers, @worker_id) or not Process.alive?(replacement),
           "retirement must not discard membership while an abnormal restart owns the directory"

    if Process.alive?(replacement), do: assert(File.exists?(ctx.worker_path))
  end

  for health <- [:healthy, :stopped, {:error, :unavailable}] do
    test "a superseded but live worker cannot replace current health with #{inspect(health)}", ctx do
      old_ref = :sys.get_state(ctx.foreman).workers[@worker_id].monitor_ref
      replacement = replace_registered_worker(ctx)
      current = :sys.get_state(ctx.foreman)
      health = if unquote(Macro.escape(health)) == :healthy, do: {:ok, ctx.worker}, else: unquote(Macro.escape(health))
      report(ctx.foreman, ctx.worker, health)
      assert :sys.get_state(ctx.foreman) == current
      assert Process.alive?(ctx.worker)
      assert Process.whereis(Cluster.otp_name_for_worker(@worker_id)) == replacement
      send(ctx.foreman, {:DOWN, old_ref, :process, ctx.worker, :killed})
      assert :pong = GenServer.call(ctx.foreman, :ping)
      assert :sys.get_state(ctx.foreman) == current
    end
  end

  test "a stale dead PID cannot steal the current worker monitor", ctx do
    replacement = replace_registered_worker(ctx)
    current = :sys.get_state(ctx.foreman)
    ref = Process.monitor(ctx.worker)
    GenServer.stop(ctx.worker, :normal)
    assert_receive {:DOWN, ^ref, :process, _, :normal}
    Foreman.report_health(ctx.foreman, @worker_id, {:ok, ctx.worker})
    assert :pong = GenServer.call(ctx.foreman, :ping)
    assert :sys.get_state(ctx.foreman) == current
    assert Process.alive?(replacement)
  end

  test "current reports update health and duplicate healthy reports retain exactly one monitor", ctx do
    initial = :sys.get_state(ctx.foreman).workers[@worker_id].monitor_ref
    report(ctx.foreman, ctx.worker, {:ok, ctx.worker})
    assert :sys.get_state(ctx.foreman).workers[@worker_id].monitor_ref == initial
    report(ctx.foreman, ctx.worker, :stopped)
    assert :sys.get_state(ctx.foreman).health == :starting
    report(ctx.foreman, ctx.worker, {:error, :unavailable})
    assert :sys.get_state(ctx.foreman).health == :unknown
    report(ctx.foreman, ctx.worker, {:ok, ctx.worker})
    assert :sys.get_state(ctx.foreman).health == :ok
    {:monitors, monitors} = Process.info(ctx.foreman, :monitors)
    assert Enum.count(monitors, &(&1 == {:process, ctx.worker})) == 1
  end

  test "late retirement from a superseded worker preserves the replacement and its directory", ctx do
    replacement = replace_registered_worker(ctx)
    current = :sys.get_state(ctx.foreman)
    send(ctx.worker, :retire)
    assert_receive {:retired, old} when old == ctx.worker
    assert :pong = GenServer.call(ctx.foreman, :ping)
    assert :sys.get_state(ctx.foreman) == current
    assert Process.alive?(replacement)
    assert File.exists?(Path.join(ctx.worker_path, "manifest.json"))
  end

  test "old health cannot overwrite a removed and recreated worker ID", ctx do
    replace_registered_worker(ctx)
    send(ctx.worker, {:capture, :health})
    assert_receive captured = {:"$gen_cast", {:worker_health, @worker_id, _, _}}
    GenServer.stop(ctx.worker, :normal)
    assert :ok = Foreman.remove_worker(ctx.foreman, @worker_id)
    assert {:ok, name} = Foreman.new_worker(ctx.foreman, @worker_id, :log)
    current = :sys.get_state(ctx.foreman)
    assert current.workers[@worker_id].health == {:ok, Process.whereis(name)}
    send(ctx.foreman, captured)
    assert :pong = GenServer.call(ctx.foreman, :ping)
    assert :sys.get_state(ctx.foreman) == current
    assert File.exists?(Path.join(ctx.worker_path, "manifest.json"))
  end

  test "old retirement cannot delete a removed and recreated worker ID", ctx do
    replace_registered_worker(ctx)
    send(ctx.worker, {:capture, :retirement})
    assert_receive captured = {:"$gen_cast", {:worker_retired, @worker_id, _}}
    GenServer.stop(ctx.worker, :normal)
    assert :ok = Foreman.remove_worker(ctx.foreman, @worker_id)
    assert {:ok, name} = Foreman.new_worker(ctx.foreman, @worker_id, :log)
    replacement = Process.whereis(name)
    current = :sys.get_state(ctx.foreman)
    send(ctx.foreman, captured)
    assert :pong = GenServer.call(ctx.foreman, :ping)
    assert :sys.get_state(ctx.foreman) == current
    assert Process.alive?(replacement)
    assert File.exists?(Path.join(ctx.worker_path, "manifest.json"))
  end

  test "late DOWN and recheck after explicit removal cannot recreate membership", ctx do
    ref = :sys.get_state(ctx.foreman).workers[@worker_id].monitor_ref
    assert :ok = Foreman.remove_worker(ctx.foreman, @worker_id)
    send(ctx.foreman, {:DOWN, ref, :process, ctx.worker, :killed})
    send(ctx.foreman, {:worker_recheck, @worker_id, 0})
    assert :pong = GenServer.call(ctx.foreman, :ping)
    assert :sys.get_state(ctx.foreman).workers == %{}
  end

  test "absent nonhealthy reports and legacy unattributed messages do not create membership", ctx do
    assert :ok = Foreman.remove_worker(ctx.foreman, @worker_id)
    Foreman.report_health(ctx.foreman, @worker_id, {:error, :unavailable})
    GenServer.cast(ctx.foreman, {:worker_health, @worker_id, :stopped})
    GenServer.cast(ctx.foreman, {:worker_retired, @worker_id})
    assert :pong = GenServer.call(ctx.foreman, :ping)
    assert :sys.get_state(ctx.foreman).workers == %{}
  end

  test "legacy healthy reports restore current monitoring but unattributed forms cannot claim identity", ctx do
    report(ctx.foreman, ctx.worker, :stopped)
    assert :sys.get_state(ctx.foreman).health == :starting
    assert :sys.get_state(ctx.foreman).workers[@worker_id].monitor_ref == nil

    GenServer.cast(ctx.foreman, {:worker_health, @worker_id, {:ok, ctx.worker}})
    assert :pong = GenServer.call(ctx.foreman, :ping)
    current = :sys.get_state(ctx.foreman)
    assert current.health == :ok
    assert current.workers[@worker_id].health == {:ok, ctx.worker}
    assert is_reference(current.workers[@worker_id].monitor_ref)
    {:monitors, monitors} = Process.info(ctx.foreman, :monitors)
    assert Enum.count(monitors, &(&1 == {:process, ctx.worker})) == 1

    GenServer.cast(ctx.foreman, {:worker_health, @worker_id, :stopped})
    GenServer.cast(ctx.foreman, {:worker_retired, @worker_id})
    assert :pong = GenServer.call(ctx.foreman, :ping)
    assert :sys.get_state(ctx.foreman) == current
    assert File.exists?(ctx.worker_path)
  end

  test "a current reporter cannot name another PID in successful health", ctx do
    current = :sys.get_state(ctx.foreman)
    report(ctx.foreman, ctx.worker, {:ok, self()})
    assert :sys.get_state(ctx.foreman) == current
  end

  test "current retirement removes the worker and directory", ctx do
    send(ctx.worker, :retire)
    assert_receive {:retired, old} when old == ctx.worker
    assert :pong = GenServer.call(ctx.foreman, :ping)
    assert :sys.get_state(ctx.foreman).workers == %{}
    refute Process.alive?(ctx.worker)
    refute File.exists?(ctx.worker_path)
  end

  for prior_health <- [:healthy, :stopped] do
    test "post-exit retirement after #{prior_health} health preserves another managed worker", ctx do
      if unquote(prior_health) == :stopped, do: report(ctx.foreman, ctx.worker, :stopped)
      assert {:ok, other_name} = Foreman.new_worker(ctx.foreman, "bbbbbbbb", :log)
      other = Process.whereis(other_name)
      :ok = :sys.suspend(ctx.foreman)

      try do
        ref = Process.monitor(ctx.worker)
        send(ctx.worker, :retire_and_exit)
        assert_receive {:DOWN, ^ref, :process, _, :normal}, 5_000
        refute Enum.any?(DynamicSupervisor.which_children(ctx.supervisor), fn {_, pid, _, _} -> pid == ctx.worker end)
        assert Process.whereis(Cluster.otp_name_for_worker(@worker_id)) == nil
      after
        :sys.resume(ctx.foreman)
      end

      assert :pong = GenServer.call(ctx.foreman, :ping)
      assert Map.keys(:sys.get_state(ctx.foreman).workers) == ["bbbbbbbb"]
      assert Process.alive?(other)
      refute File.exists?(ctx.worker_path)
    end
  end

  test "replacement startup health arriving before old DOWN installs the replacement monitor", ctx do
    :ok = :sys.suspend(ctx.foreman)

    replacement =
      try do
        Process.unregister(Cluster.otp_name_for_worker(@worker_id))
        {:ok, replacement} = DynamicSupervisor.start_child(ctx.supervisor, {ControlledWorker, ctx.opts})
        assert_receive {:started, ^replacement, _}, 5_000
        send(replacement, :report_startup)
        assert_receive {:reported, ^replacement, {:ok, ^replacement}}, 5_000
        GenServer.stop(ctx.worker, :normal)
        replacement
      after
        :sys.resume(ctx.foreman)
      end

    assert :pong = GenServer.call(ctx.foreman, :ping)
    assert :sys.get_state(ctx.foreman).workers[@worker_id].health == {:ok, replacement}
    {:monitors, monitors} = Process.info(ctx.foreman, :monitors)
    assert {:process, replacement} in monitors
    refute {:process, ctx.worker} in monitors
  end

  test "an absent target name does not prove ownership is gone while an unknown supervised child remains", ctx do
    GenServer.stop(ctx.worker, :normal)
    assert :pong = GenServer.call(ctx.foreman, :ping)
    assert Process.whereis(Cluster.otp_name_for_worker(@worker_id)) == nil
    {:ok, unknown} = DynamicSupervisor.start_child(ctx.supervisor, {Agent, fn -> :unaccounted end})
    before = :sys.get_state(ctx.foreman)
    assert before.workers[@worker_id].health == :stopped

    assert {:error, :worker_shutdown_unresolved} = Foreman.remove_worker(ctx.foreman, @worker_id)
    assert :sys.get_state(ctx.foreman) == before
    assert File.exists?(Path.join(ctx.worker_path, "manifest.json"))
    assert Process.alive?(unknown)
    assert Agent.get(unknown, & &1) == :unaccounted

    assert :ok = DynamicSupervisor.terminate_child(ctx.supervisor, unknown)
    assert :ok = Foreman.remove_worker(ctx.foreman, @worker_id)
    assert :sys.get_state(ctx.foreman).workers == %{}
    refute File.exists?(ctx.worker_path)
  end

  test "a filesystem cleanup failure retains stopped membership for retry", ctx do
    parent = Path.dirname(ctx.worker_path)
    File.chmod!(parent, 0o555)

    result =
      try do
        Foreman.remove_worker(ctx.foreman, @worker_id)
      after
        File.chmod!(parent, 0o755)
      end

    assert {:error, {:failed_to_remove_directory, _, _}} = result
    assert :sys.get_state(ctx.foreman).workers[@worker_id].health == :stopped
    refute Process.alive?(ctx.worker)
    assert :ok = Foreman.remove_worker(ctx.foreman, @worker_id)
    assert :sys.get_state(ctx.foreman).workers == %{}
  end

  defp safe_resume(pid) do
    if Process.alive?(pid), do: :sys.resume(pid, 1_000)
  catch
    :exit, _ -> :ok
  end

  defp churn_until_removal_reply(ref, gate, attempts \\ 0, replacement \\ nil) do
    receive do
      {:termination_resolved, ^gate, token, pid} ->
        assert attempts < 10, "unbounded termination retries"
        Process.exit(pid, :kill)
        assert_receive {:started, next, _}, 5_000
        assert next != pid
        send(gate, {:forward_termination, token})
        churn_until_removal_reply(ref, gate, attempts + 1, next)

      {^ref, result} ->
        {result, replacement, attempts}
    after
      5_000 -> flunk("removal did not return a bounded result")
    end
  end

  defp wait_for_termination_call(supervisor, remaining \\ 5_000) do
    {:messages, messages} = Process.info(supervisor, :messages)

    if Enum.any?(messages, &match?({:"$gen_call", _, {:terminate_child, _}}, &1)) do
      :ok
    else
      if remaining == 0, do: flunk("Foreman never reached supervised termination")

      receive do
      after
        1 -> wait_for_termination_call(supervisor, remaining - 1)
      end
    end
  end

  defp replace_registered_worker(ctx) do
    # The old process remains alive deliberately: liveness alone is not identity.
    Process.unregister(Cluster.otp_name_for_worker(@worker_id))
    {:ok, replacement} = DynamicSupervisor.start_child(ctx.supervisor, {ControlledWorker, ctx.opts})
    assert_receive {:started, ^replacement, _}, 5_000
    send(replacement, :report_startup)
    assert_receive {:reported, ^replacement, {:ok, ^replacement}}, 5_000
    assert :pong = GenServer.call(ctx.foreman, :ping)
    assert :sys.get_state(ctx.foreman).workers[@worker_id].health == {:ok, replacement}
    replacement
  end

  defp report(foreman, worker, health) do
    send(worker, {:report, health})
    assert_receive {:reported, ^worker, ^health}, 5_000
    assert :pong = GenServer.call(foreman, :ping)
  end
end
