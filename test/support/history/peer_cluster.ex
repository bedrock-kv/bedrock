defmodule Bedrock.Test.History.PeerCluster do
  @moduledoc "Real three-VM history fixture with stdio control independent of distribution."

  alias Bedrock.Service.Foreman
  alias Bedrock.Test.History.Driver

  defmodule Cluster do
    @moduledoc false
    use Bedrock.Cluster, otp_app: :bedrock, name: "peer_history"
  end

  defmodule Repo do
    @moduledoc false
    use Bedrock.Repo, cluster: Cluster
  end

  def start(root) do
    File.mkdir_p!(root)
    {:ok, state} = Agent.start_link(fn -> %{root: root, peers: %{}, cookie: Node.get_cookie()} end)

    try do
      for role <- [:coordination, :log, :materializer], do: start_peer(state, role)
      coordinator = get_peer(state, :coordination).node
      File.write!(Path.join(root, "descriptor"), "peer_history:#{coordinator}")
      for role <- [:coordination, :log, :materializer], do: boot(state, role)
      state
    catch
      kind, reason ->
        path = artifact(root, "boot_failure", [{kind, reason}])
        IO.puts("Peer boot failure artifact: #{path}")
        stop(state)
        :erlang.raise(kind, reason, __STACKTRACE__)
    end
  end

  defp start_peer(state, role) do
    %{root: root, cookie: cookie} = Agent.get(state, & &1)
    name = String.to_atom("history_#{role}_#{:erlang.phash2(root)}")

    args =
      [
        ~c"+S",
        ~c"2:2",
        ~c"-setcookie",
        Atom.to_charlist(cookie),
        ~c"-kernel",
        ~c"prevent_overlapping_partitions",
        ~c"false",
        ~c"-pa"
      ] ++ :code.get_path()

    {:ok, control, peer_node} = :peer.start_link(%{name: name, connection: :standard_io, args: args})
    peer = %{control: control, node: peer_node, role: role}
    Agent.update(state, &put_in(&1, [:peers, role], peer))
    peer
  end

  defp boot(state, role) do
    root = Agent.get(state, & &1.root)
    call(state, role, :boot_remote, [root, role])
  end

  def boot_remote(root, role) do
    Application.ensure_all_started(:bedrock)
    Logger.configure(level: :warning)
    backend = {Bedrock.ObjectStorage.LocalFilesystem, root: Path.join(root, "objects")}
    local = Path.join(root, Atom.to_string(role))
    File.mkdir_p!(local)
    Application.put_env(:bedrock, Bedrock.ObjectStorage, backend: backend)

    Application.put_env(:bedrock, Cluster,
      capabilities: [role],
      durability_mode: :relaxed,
      path_to_descriptor: Path.join(root, "descriptor"),
      object_storage: backend,
      coordination: [path: local],
      coordinator: [path: local],
      materializer: [path: local, object_storage: backend],
      log: [path: local, object_storage: backend]
    )

    {:ok, sup} = Supervisor.start_link([{Cluster, []}], strategy: :one_for_one)
    Process.unlink(sup)
    :ok
  end

  def nodes(state), do: Agent.get(state, &Map.new(&1.peers, fn {role, peer} -> {role, peer.node} end))
  defp get_peer(state, role), do: Agent.get(state, & &1.peers[role])

  defp call(state, role, function, args \\ []),
    do: :peer.call(get_peer(state, role).control, __MODULE__, function, args, 20_000)

  def ready(state, previous_epoch \\ -1) do
    deadline = System.monotonic_time(:millisecond) + 40_000

    await(
      fn ->
        try do
          layout = call(state, :coordination, :layout_remote)
          placement = Map.new([:log, :materializer], fn role -> {role, call(state, role, :services_remote)} end)
          expected = nodes(state)
          log_ids = Enum.map(placement.log, & &1.id)

          valid = layout.epoch > previous_epoch and valid_placement?(layout, placement, expected, log_ids)

          if valid, do: %{epoch: layout.epoch, layout: layout, services: placement, nodes: expected}, else: false
        catch
          _, _ -> false
        end
      end,
      deadline
    )
  end

  defp valid_placement?(layout, placement, expected, log_ids) do
    checks = [
      map_size(layout.logs) > 0,
      Enum.all?(Map.keys(layout.logs), &(&1 in log_ids)),
      Enum.all?(placement.log, &(&1.node == expected.log and &1.kind == :log)),
      Enum.sort(Enum.map(placement.materializer, & &1.shard_num)) == [0, 1],
      Enum.all?(placement.materializer, &(&1.node == expected.materializer and &1.kind == :materializer)),
      Enum.all?(placement.materializer, &(&1.epoch == layout.epoch and &1.mode == :running)),
      node(layout.sequencer) == expected.coordination,
      layout.resolvers != [],
      Enum.all?(layout.resolvers, &(node(elem(&1, 1)) == expected.coordination)),
      layout.proxies != [],
      Enum.all?(layout.proxies, &(node(&1) == expected.coordination))
    ]

    Enum.all?(checks)
  end

  def layout_remote, do: Cluster.transaction_system_layout!()

  def services_remote do
    {:ok, services} = Foreman.get_all_running_services(Cluster.otp_name(:foreman))

    Enum.map(services, fn {id, kind, name} ->
      pid = Process.whereis(name)
      worker = :sys.get_state(pid)

      %{
        id: id,
        kind: kind,
        node: node(pid),
        pid: pid,
        shard_num: Map.get(worker, :shard_num),
        epoch: worker.epoch,
        mode: worker.mode
      }
    end)
  end

  def attempt(state, id, operations) do
    # A restarted VM has a different monotonic clock origin. Order every RPC
    # attempt on the surviving controller clock, retaining remote times as evidence.
    invoke = System.monotonic_time()
    entry = call(state, :coordination, :attempt_remote, [id, operations])

    entry
    |> Map.put(:remote_times, {entry.invoke, entry.complete})
    |> Map.put(:invoke, invoke)
    |> Map.put(:complete, System.monotonic_time())
  end

  def attempt_remote(id, operations) do
    {:ok, recorder} = Driver.start_recorder()

    try do
      Driver.attempt(Repo, recorder, id, operations, timeout_in_ms: 15_000)
    after
      Agent.stop(recorder)
    end
  end

  def final(state), do: call(state, :coordination, :final_remote)

  def final_remote,
    do:
      Repo.transact(fn -> {"history/", "history0"} |> Repo.get_range() |> Enum.to_list() |> Map.new() end,
        timeout_in_ms: 15_000
      )

  def wal_files(state) do
    root = Agent.get(state, & &1.root)
    root |> Path.join("log/**/wal_*") |> Path.wildcard() |> Enum.filter(&File.regular?/1)
  end

  def stop_log(state), do: :peer.stop(get_peer(state, :log).control)
  def log_down?(state), do: Node.ping(get_peer(state, :log).node) == :pang

  def restart_log(state) do
    start_peer(state, :log)
    boot(state, :log)
  end

  def suspend_coordinator(state), do: call(state, :coordination, :suspend_coordinator_remote)
  def suspend_coordinator_remote, do: :sys.suspend(Cluster.coordinator!())
  def stop_coordinator(state), do: :peer.stop(get_peer(state, :coordination).control)

  def restart_coordinator(state) do
    start_peer(state, :coordination)
    boot(state, :coordination)
  end

  defp edges, do: [{:log, :coordination}, {:coordination, :log}, {:log, :materializer}, {:materializer, :log}]

  def partition_log(state) do
    # Install all four directional cookie barriers before disconnecting any edge.
    cookies =
      for {from, to} <- edges() do
        true = call(state, from, :cookie_remote, [get_peer(state, to).node, String.to_atom("history_wrong_#{from}")])
        {System.monotonic_time(), :cookie_barrier, from, to}
      end

    disconnects =
      for {from, to} <- edges() do
        result = call(state, from, :disconnect_remote, [get_peer(state, to).node])
        {System.monotonic_time(), :disconnect, from, to, result}
      end

    cookies ++ disconnects
  end

  def partition_proof(state) do
    for {from, to} <- edges() do
      target = get_peer(state, to).node
      state |> call(from, :proof_remote, [target]) |> Map.merge(%{from: from, to: to})
    end
  end

  def cookie_remote(target, cookie), do: Node.set_cookie(target, cookie)
  def disconnect_remote(target), do: Node.disconnect(target)

  def proof_remote(target) do
    disconnected = target not in Node.list()
    ping = Node.ping(target)
    %{alive: Node.alive?(), disconnected: disconnected and target not in Node.list(), ping: ping}
  end

  def connected_edges(state) do
    for {from, to} <- edges() do
      {from, to, call(state, from, :ping_remote, [get_peer(state, to).node])}
    end
  end

  def heal(state) do
    cookie = Agent.get(state, & &1.cookie)
    for {from, to} <- edges(), do: call(state, from, :cookie_remote, [get_peer(state, to).node, cookie])
    for {from, to} <- edges(), do: call(state, from, :ping_remote, [get_peer(state, to).node])
  end

  def ping_remote(target), do: Node.ping(target)

  def stop(state) do
    for {_role, peer} <- Agent.get(state, & &1.peers) do
      try do
        :peer.stop(peer.control)
      catch
        _, _ -> :ok
      end
    end

    Agent.stop(state)
  end

  def diagnostics(state) do
    Map.new([:coordination, :log, :materializer], fn role ->
      value =
        try do
          :peer.call(get_peer(state, role).control, __MODULE__, :diagnostics_remote, [], 2_000)
        catch
          kind, reason -> {kind, reason}
        end

      {role, value}
    end)
  end

  def diagnostics_remote do
    coordinator = Process.whereis(Cluster.otp_name(:coordinator))
    coordinator_state = if coordinator, do: :sys.get_state(coordinator, 1_000)
    %{node: node(), connected: Node.list(), coordinator: coordinator_state}
  end

  def artifact(root, scenario, events) do
    directory =
      System.get_env("BEDROCK_HISTORY_ARTIFACT_DIR") || Path.join(System.tmp_dir!(), "bedrock-history-artifacts")

    File.mkdir_p!(directory)
    path = Path.join(directory, "peer-#{scenario}-#{:erlang.phash2(root)}.term")
    {revision, 0} = System.cmd("git", ["rev-parse", "HEAD"])

    File.write!(
      path,
      :erlang.term_to_binary(%{
        seed: 239,
        exunit_seed: ExUnit.configuration()[:seed],
        scenario: scenario,
        revision: String.trim(revision),
        initial: %{},
        root: root,
        events: events
      })
    )

    path
  end

  defp await(predicate, deadline) do
    result = predicate.()

    cond do
      result ->
        result

      System.monotonic_time(:millisecond) >= deadline ->
        raise "peer cluster did not publish the required recovered placement"

      true ->
        Process.sleep(50)
        await(predicate, deadline)
    end
  end
end
