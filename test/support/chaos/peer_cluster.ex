defmodule Bedrock.Test.Chaos.PeerCluster do
  @moduledoc """
  Starts a real multi-node Bedrock cluster out of `:peer` nodes, driven from
  ExUnit.

  Bedrock's identity is node-keyed: `Descriptor.coordinator_nodes` is a list of
  `node()`, capabilities are per-node config, and `ClusterSupervisor` refuses to
  start on `:nonode@nohost`. A single BEAM therefore cannot host a multi-
  coordinator cluster, so anything that exercises coordinator failover needs
  real distribution.

  Each node gets its own data directory; all of them share one descriptor naming
  every coordinator node.
  """

  alias Bedrock.Cluster.Descriptor
  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.LocalFilesystem
  alias Bedrock.Test.Chaos.Cluster
  alias Bedrock.Test.Chaos.Ops

  @type node_spec :: %{name: node(), peer: pid(), data_dir: Path.t()}
  @type t :: %{nodes: [node_spec()], data_root: Path.t(), descriptor_path: Path.t()}

  @primary :"bedrock_chaos_primary@127.0.0.1"
  @host ~c"127.0.0.1"
  @cookie :bedrock_chaos

  @doc """
  Start a cluster of `count` peer nodes, each with the given capabilities, and
  block until it is serving transactions.

  Returns the cluster handle. Raises with diagnostic detail if the cluster does
  not come up within `:ready_timeout_ms`.
  """
  @spec start!(keyword()) :: t()
  def start!(opts \\ []) do
    count = Keyword.get(opts, :count, 3)
    capabilities = Keyword.get(opts, :capabilities, [:coordination, :log, :materializer])
    ready_timeout_ms = Keyword.get(opts, :ready_timeout_ms, 60_000)
    trace = Keyword.get(opts, :trace, [])

    ensure_distribution!()

    run_id = :erlang.unique_integer([:positive])
    data_root = Path.join(System.tmp_dir!(), "bedrock-chaos-#{run_id}")
    File.mkdir_p!(data_root)

    short_names = Enum.map(1..count, fn i -> ~c"bdrk_#{run_id}_c#{i}" end)
    node_names = Enum.map(short_names, fn short -> :"#{short}@#{@host}" end)

    descriptor_path = Path.join(data_root, "bedrock.cluster")
    File.write!(descriptor_path, Descriptor.encode_cluster_file_contents(Descriptor.new(Cluster.name(), node_names)))

    # One object store for the whole cluster. Each node keeps its own local data
    # directory, but cluster bootstrap state lives in object storage, so giving
    # every node a private store would produce N nodes that each bootstrap their
    # own cluster and never converge.
    object_storage_root = Path.join(data_root, "object_storage")
    File.mkdir_p!(object_storage_root)
    object_storage = ObjectStorage.backend(LocalFilesystem, root: object_storage_root)

    nodes =
      short_names
      |> Enum.zip(node_names)
      |> Enum.map(fn {short, name} ->
        data_dir = Path.join(data_root, to_string(short))
        File.mkdir_p!(data_dir)

        peer = start_peer!(short)
        configure_node!(name, data_dir, descriptor_path, capabilities, trace, object_storage)

        %{name: name, peer: peer, data_dir: data_dir}
      end)

    cluster = %{nodes: nodes, data_root: data_root, descriptor_path: descriptor_path}

    Enum.each(nodes, fn %{name: name} -> start_cluster_supervisor!(name) end)

    await_ready!(cluster, ready_timeout_ms)

    cluster
  end

  @doc """
  Stop every peer and remove the run's data directory.
  """
  @spec stop(t()) :: :ok
  def stop(%{nodes: nodes, data_root: data_root}) do
    Enum.each(nodes, fn %{peer: peer} ->
      try do
        :peer.stop(peer)
      catch
        :exit, _ -> :ok
      end
    end)

    File.rm_rf!(data_root)
    :ok
  end

  @doc """
  The nodes in the cluster, in the order they were started.
  """
  @spec node_names(t()) :: [node()]
  def node_names(%{nodes: nodes}), do: Enum.map(nodes, & &1.name)

  @doc """
  Run a function on one of the cluster's nodes.
  """
  @spec call(node(), module(), atom(), [term()], timeout()) :: term()
  def call(node, module, function, args, timeout \\ 30_000), do: :erpc.call(node, module, function, args, timeout)

  # The primary VM must itself be distributed before it can start or reach
  # peers; `mix test` runs as :nonode@nohost.
  defp ensure_distribution! do
    case Node.self() do
      :nonode@nohost ->
        {:ok, _} = :net_kernel.start([@primary, :longnames])
        Node.set_cookie(@cookie)
        :ok

      _already_distributed ->
        Node.set_cookie(@cookie)
        :ok
    end
  end

  defp start_peer!(short_name) do
    {:ok, peer, _node} =
      :peer.start_link(%{
        name: short_name,
        host: @host,
        longnames: true,
        args: peer_args(),
        wait_boot: 30_000
      })

    peer
  end

  defp peer_args do
    code_path_args = Enum.flat_map(:code.get_path(), fn path -> [~c"-pa", path] end)

    [~c"-setcookie", Atom.to_charlist(@cookie)] ++ code_path_args
  end

  defp configure_node!(node, data_dir, descriptor_path, capabilities, trace, object_storage) do
    {:ok, _apps} = :erpc.call(node, Application, :ensure_all_started, [:bedrock])

    # The coordinator and director read :object_storage from the top level; the
    # foreman reads it from its capability sections. Both have to be set.
    config = [
      capabilities: capabilities,
      durability_mode: :relaxed,
      trace: trace,
      path_to_descriptor: descriptor_path,
      object_storage: object_storage,
      coordinator: [path: data_dir, object_storage: object_storage],
      log: [path: data_dir, object_storage: object_storage],
      materializer: [path: data_dir, object_storage: object_storage]
    ]

    :ok = :erpc.call(node, Application, :put_env, [:bedrock, Cluster, config])
  end

  # `:erpc.call/4` runs the work in a process that exits as soon as it has a
  # result, and it signals that result *as its exit reason*. A supervisor
  # started with `Supervisor.start_link` from inside that process is linked to
  # it, so it dies the instant the call returns. The cluster therefore needs an
  # owner on the peer that outlives the call: a plain `spawn` (unlinked from the
  # erpc worker) that starts the supervisor and then parks forever.
  defp start_cluster_supervisor!(node) do
    {:ok, _owner} =
      :erpc.call(node, :erlang, :apply, [
        fn ->
          caller = self()

          owner =
            spawn(fn ->
              case Supervisor.start_link([Cluster], strategy: :one_for_one, name: :bedrock_chaos_root) do
                {:ok, _sup} ->
                  send(caller, {:cluster_started, self()})
                  Process.sleep(:infinity)

                error ->
                  send(caller, {:cluster_failed, error})
              end
            end)

          receive do
            {:cluster_started, ^owner} -> {:ok, owner}
            {:cluster_failed, error} -> error
          after
            30_000 -> {:error, :timeout_starting_cluster}
          end
        end,
        []
      ])

    :ok
  end

  # Readiness has three parts, and all of them matter.
  #
  # ClusterSupervisor.child_spec/1 silently falls back to a single-node
  # descriptor when it cannot read the file, so three nodes can each come up
  # "healthy" as three separate one-node clusters. Checking that every node
  # agrees on the full coordinator set is what distinguishes a real cluster from
  # that failure mode.
  #
  # Agreement alone does not mean the cluster is serving, so we then wait for a
  # transaction system layout, which only exists once recovery has completed.
  #
  # And a layout does not mean it is serving either. Roughly one cluster in a
  # hundred publishes a layout and then never commits anything: every
  # transaction comes back `:unavailable`, and it does not heal (measured: 12
  # attempts over 60s). A caller that took the layout as "ready" would see that
  # as its workload failing rather than as its cluster never having started, so
  # the last gate is an actual committed transaction — on *every* node, since
  # callers drive all of them.
  defp await_ready!(cluster, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    expected = node_names(cluster)

    await_until!(cluster, deadline, "every node to agree on coordinator set #{inspect(expected)}", fn ->
      Enum.all?(expected, fn node ->
        case safe_call(node, Cluster, :fetch_coordinator_nodes, []) do
          {:ok, nodes} -> Enum.sort(nodes) == Enum.sort(expected)
          _ -> false
        end
      end)
    end)

    await_until!(cluster, deadline, "a transaction system layout to be available", fn ->
      Enum.any?(expected, fn node ->
        match?({:ok, _}, safe_call(node, Cluster, :fetch_transaction_system_layout, []))
      end)
    end)

    await_until!(cluster, deadline, "every node to commit a transaction", fn ->
      Enum.all?(expected, fn node -> :ok == safe_call(node, Ops, :put, ["chaos/probe/ready", to_string(node)]) end)
    end)

    :ok
  end

  defp await_until!(cluster, deadline, description, check) do
    if check.() do
      :ok
    else
      if System.monotonic_time(:millisecond) >= deadline do
        raise "Timed out waiting for #{description}.\n\n#{diagnostics(cluster)}"
      end

      Process.sleep(250)
      await_until!(cluster, deadline, description, check)
    end
  end

  # A cluster that never becomes ready is rare enough that reproducing it to
  # diagnose it is expensive. Dumping what each node believed at the moment we
  # gave up is what makes the next occurrence useful instead of merely annoying.
  defp diagnostics(cluster) do
    cluster
    |> node_names()
    |> Enum.map_join("\n", fn node ->
      layout =
        case safe_call(node, Cluster, :fetch_transaction_system_layout, []) do
          {:ok, tsl} ->
            "epoch=#{inspect(tsl[:epoch])} sequencer=#{inspect(tsl[:sequencer])} proxies=#{inspect(tsl[:proxies])}"

          other ->
            inspect(other)
        end

      """
      #{node}
        coordinator_nodes: #{inspect(safe_call(node, Cluster, :fetch_coordinator_nodes, []))}
        coordinator:       #{inspect(safe_call(node, Cluster, :fetch_coordinator, []))}
        layout:            #{layout}
        commit probe:      #{inspect(safe_call(node, Ops, :put, ["chaos/probe/diagnostic", "probe"]))}\
      """
    end)
  end

  defp safe_call(node, module, function, args) do
    :erpc.call(node, module, function, args, 5_000)
  catch
    :error, _ -> :unavailable
    :exit, _ -> :unavailable
  end
end
