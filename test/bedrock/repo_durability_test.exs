defmodule Bedrock.RepoDurabilityTest do
  use ExUnit.Case, async: false

  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.LocalFilesystem
  alias Bedrock.Repo

  defmodule TestCluster do
    @moduledoc false
    use Bedrock.Cluster, otp_app: :bedrock, name: "repo_durability_test_cluster"
  end

  defmodule TestRepo do
    @moduledoc false
    use Repo, cluster: TestCluster
  end

  @tag :tmp_dir
  test "fresh single-node clusters can read committed data before and after restart", %{tmp_dir: tmp_dir} do
    ensure_local_node_started!()

    config = node_config(tmp_dir)
    original_config = Application.get_env(:bedrock, TestCluster)
    Application.put_env(:bedrock, TestCluster, config)

    on_exit(fn ->
      if is_nil(original_config) do
        Application.delete_env(:bedrock, TestCluster)
      else
        Application.put_env(:bedrock, TestCluster, original_config)
      end
    end)

    supervisor = start_cluster_supervisor()
    wait_for_layout!()

    assert :ok =
             TestRepo.transact(
               fn ->
                 TestRepo.put("k1", "v1")
               end,
               retry_limit: 5
             )

    assert {:ok, "v1"} =
             TestRepo.transact(
               fn ->
                 {:ok, TestRepo.get("k1")}
               end,
               retry_limit: 5
             )

    wait_for_bootstrap!(tmp_dir)
    stop_cluster_supervisor!(supervisor)

    restarted_supervisor = start_cluster_supervisor()
    wait_for_layout!()

    assert {:ok, "v1"} =
             TestRepo.transact(
               fn ->
                 {:ok, TestRepo.get("k1")}
               end,
               retry_limit: 5
             )

    stop_cluster_supervisor!(restarted_supervisor)
  end

  defp node_config(tmp_dir) do
    object_storage =
      ObjectStorage.backend(
        LocalFilesystem,
        root: Path.join([tmp_dir, "coordinator", "object_storage"])
      )

    [
      capabilities: [:coordination, :log, :materializer],
      path_to_descriptor: Path.join(tmp_dir, "bedrock.cluster"),
      object_storage: object_storage,
      trace: [:recovery, :storage],
      coordinator: [path: Path.join(tmp_dir, "coordinator"), persistent: true],
      worker: [path: Path.join(tmp_dir, "workers")],
      durability_mode: :relaxed,
      durability: [desired_replication_factor: 1, desired_logs: 1]
    ]
  end

  defp ensure_local_node_started! do
    if Node.self() == :nonode@nohost do
      ensure_epmd_started!()
      node_name = :"bedrock_repo_durability_#{System.unique_integer([:positive])}"

      case :net_kernel.start([node_name, :shortnames]) do
        {:ok, _pid} ->
          Node.set_cookie(:bedrock_repo_durability)
          :ok

        {:error, {:already_started, _pid}} ->
          :ok

        {:error, reason} ->
          flunk("failed to start local Erlang node: #{inspect(reason)}")
      end
    else
      :ok
    end
  end

  defp ensure_epmd_started! do
    epmd =
      System.find_executable("epmd") ||
        flunk("failed to locate epmd executable required for distributed durability test")

    case System.cmd(epmd, ["-daemon"], stderr_to_stdout: true) do
      {_output, 0} ->
        :ok

      {output, status} ->
        flunk("failed to start epmd for distributed durability test: status=#{status} output=#{inspect(output)}")
    end
  end

  defp start_cluster_supervisor do
    {:ok, supervisor} = Supervisor.start_link([TestCluster.child_spec([])], strategy: :one_for_one)
    supervisor
  end

  defp stop_cluster_supervisor!(supervisor) do
    Supervisor.stop(supervisor, :normal, 30_000)

    assert_eventually(fn ->
      is_nil(Process.whereis(TestCluster.otp_name(:link))) and
        is_nil(Process.whereis(TestCluster.otp_name(:coordinator))) and
        is_nil(Process.whereis(TestCluster.otp_name(:foreman)))
    end)
  end

  defp wait_for_layout!(timeout_ms \\ 20_000) do
    Process.put(:last_layout_result, nil)
    Process.put(:last_director_down_reason, nil)
    Process.put(:director_monitor, nil)

    assert_eventually(
      fn ->
        track_director_failure()
        result = TestCluster.fetch_transaction_system_layout()
        Process.put(:last_layout_result, result)
        layout_ready?(result, current_coordinator_state())
      end,
      timeout_ms,
      &layout_wait_failure_message/0
    )
  end

  defp layout_ready?(
         {:ok,
          %{
            epoch: layout_epoch,
            logs: logs,
            services: services,
            proxies: proxies,
            resolvers: resolvers,
            shard_layout: shard_layout,
            metadata_materializer: metadata_materializer,
            shard_materializers: shard_materializers
          }},
         %{epoch: coordinator_epoch}
       ) do
    layout_epoch == coordinator_epoch and
      populated?(logs) and
      populated?(services) and
      populated_list?(proxies) and
      populated_list?(resolvers) and
      populated?(shard_layout) and
      is_pid(metadata_materializer) and
      populated?(shard_materializers) and
      shard_materializers_cover_layout?(shard_layout, shard_materializers)
  end

  defp layout_ready?(_, _), do: false

  defp populated?(value) when is_map(value), do: map_size(value) > 0
  defp populated?(_value), do: false

  defp populated_list?(value) when is_list(value), do: value != []
  defp populated_list?(_value), do: false

  defp layout_wait_failure_message do
    "last layout result: #{inspect(Process.get(:last_layout_result), limit: :infinity)}\n" <>
      "director state: #{inspect(current_director_state(), limit: :infinity)}\n" <>
      "director process info: #{inspect(current_director_process_info(), limit: :infinity)}\n" <>
      "coordinator state: #{inspect(current_coordinator_state(), limit: :infinity)}\n" <>
      "log states: #{inspect(current_log_states(), limit: :infinity)}\n" <>
      "materializer states: #{inspect(current_materializer_states(), limit: :infinity)}\n" <>
      "last director down reason: #{inspect(Process.get(:last_director_down_reason), limit: :infinity)}"
  end

  defp track_director_failure do
    drain_director_down_messages()

    case {Process.get(:director_monitor), current_director_pid()} do
      {nil, pid} when is_pid(pid) ->
        Process.put(:director_monitor, Process.monitor(pid))

      _ ->
        :ok
    end
  end

  defp drain_director_down_messages do
    receive do
      {:DOWN, monitor_ref, :process, _pid, reason} ->
        if Process.get(:director_monitor) == monitor_ref do
          Process.put(:director_monitor, nil)
          Process.put(:last_director_down_reason, reason)
        end

        drain_director_down_messages()
    after
      0 -> :ok
    end
  end

  defp shard_materializers_cover_layout?(shard_layout, shard_materializers) do
    shard_layout
    |> Map.values()
    |> Enum.map(fn {tag, _start_key} -> tag end)
    |> Enum.uniq()
    |> Enum.all?(&match?(pid when is_pid(pid), Map.get(shard_materializers, &1)))
  end

  defp current_director_pid do
    case Process.whereis(TestCluster.otp_name(:coordinator)) do
      nil ->
        nil

      coordinator_pid ->
        case :sys.get_state(coordinator_pid) do
          %{director: director_pid} when is_pid(director_pid) -> director_pid
          _ -> nil
        end
    end
  end

  defp current_director_state do
    case current_director_pid() do
      pid when is_pid(pid) ->
        try do
          :sys.get_state(pid, 1_000)
        catch
          :exit, reason -> {:director_state_unavailable, reason}
        end

      _ ->
        :director_unavailable
    end
  end

  defp current_director_process_info do
    case current_director_pid() do
      pid when is_pid(pid) ->
        Process.info(pid, [:status, :current_function, :current_stacktrace, :message_queue_len, :messages])

      _ ->
        :director_unavailable
    end
  end

  defp current_coordinator_state do
    case Process.whereis(TestCluster.otp_name(:coordinator)) do
      pid when is_pid(pid) ->
        try do
          :sys.get_state(pid, 1_000)
        catch
          :exit, reason -> {:coordinator_state_unavailable, reason}
        end

      _ ->
        :coordinator_unavailable
    end
  end

  defp current_materializer_states do
    case current_coordinator_state() do
      %{service_directory: service_directory} when is_map(service_directory) ->
        service_directory
        |> Enum.filter(fn {_service_id, {kind, _location}} -> kind == :materializer end)
        |> Enum.map(fn {service_id, {_kind, {otp_name, _node}}} ->
          pid = Process.whereis(otp_name)

          {service_id,
           %{
             pid: pid,
             alive?: is_pid(pid) and Process.alive?(pid),
             process_info:
               if(is_pid(pid),
                 do: Process.info(pid, [:status, :current_function, :message_queue_len]),
                 else: :unavailable
               ),
             pull_task_info:
               case safe_sys_get_state(pid) do
                 %{pull_task: %Task{pid: task_pid}} when is_pid(task_pid) ->
                   Process.info(task_pid, [:status, :current_function, :current_stacktrace, :message_queue_len])

                 _ ->
                   :unavailable
               end,
             state: if(is_pid(pid), do: safe_sys_get_state(pid), else: :unavailable)
           }}
        end)

      _ ->
        :coordinator_unavailable
    end
  end

  defp current_log_states do
    case current_coordinator_state() do
      %{service_directory: service_directory} when is_map(service_directory) ->
        service_directory
        |> Enum.filter(fn {_service_id, {kind, _location}} -> kind == :log end)
        |> Enum.map(fn {service_id, {_kind, {otp_name, _node}}} ->
          pid = Process.whereis(otp_name)

          {service_id,
           %{
             pid: pid,
             alive?: is_pid(pid) and Process.alive?(pid),
             process_info:
               if(is_pid(pid),
                 do: Process.info(pid, [:status, :current_function, :message_queue_len]),
                 else: :unavailable
               ),
             state: if(is_pid(pid), do: safe_sys_get_state(pid), else: :unavailable)
           }}
        end)

      _ ->
        :coordinator_unavailable
    end
  end

  defp safe_sys_get_state(pid) do
    :sys.get_state(pid, 1_000)
  catch
    :exit, reason -> {:sys_state_unavailable, reason}
  end

  defp wait_for_bootstrap!(tmp_dir, timeout_ms \\ 10_000) do
    backend =
      ObjectStorage.backend(
        LocalFilesystem,
        root: Path.join([tmp_dir, "coordinator", "object_storage"])
      )

    assert_eventually(
      fn ->
        match?({:ok, _data}, ObjectStorage.get(backend, "bootstrap"))
      end,
      timeout_ms
    )
  end

  defp assert_eventually(fun, timeout_ms \\ 5_000, failure_message_fun \\ fn -> "condition not met before timeout" end) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_assert_eventually(fun, deadline, failure_message_fun)
  end

  defp do_assert_eventually(fun, deadline, failure_message_fun) do
    if fun.() do
      :ok
    else
      if System.monotonic_time(:millisecond) >= deadline do
        flunk(failure_message_fun.())
      else
        Process.sleep(100)
        do_assert_eventually(fun, deadline, failure_message_fun)
      end
    end
  end
end
