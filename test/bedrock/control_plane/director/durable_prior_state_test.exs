defmodule Bedrock.ControlPlane.Director.DurablePriorStateTest do
  @moduledoc """
  A Director recovers from the durable record in object storage, not from
  the prior core state its coordinator handed it (bedrock-w10). The
  coordinator loaded that value when it booted; a follower that later
  wins leadership never learned anything newer.
  """
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Bedrock.ControlPlane.Config
  alias Bedrock.ControlPlane.Director.Recovery
  alias Bedrock.ControlPlane.Director.Server
  alias Bedrock.ControlPlane.Director.State
  alias Bedrock.ObjectStorage
  alias Bedrock.ObjectStorage.LocalFilesystem
  alias Bedrock.SystemKeys.ClusterBootstrap

  @moduletag :tmp_dir

  defmodule TestCluster do
    @moduledoc false
    def node_config, do: Application.fetch_env!(:bedrock, __MODULE__)
    def otp_name(component), do: :"durable_prior_state_test_#{component}"
  end

  defmodule UnreachableStorage do
    @moduledoc false
    def get(_config, _key), do: {:error, :econnrefused}
  end

  setup do
    on_exit(fn -> Application.delete_env(:bedrock, TestCluster) end)
  end

  defp with_bootstrap(tmp_dir, log_ids) do
    backend = ObjectStorage.backend(LocalFilesystem, root: tmp_dir)

    bootstrap = %{
      cluster_id: "cluster-1",
      epoch: 4,
      logs: Enum.map(log_ids, &%{id: &1, otp_ref: nil, shard_tags: []}),
      coordinators: [%{node: Atom.to_string(Node.self())}]
    }

    :ok = ObjectStorage.put(backend, "bootstrap", ClusterBootstrap.to_binary(bootstrap))
    Application.put_env(:bedrock, TestCluster, object_storage: backend)
  end

  # A director as its coordinator launches it, handed that coordinator's
  # copy of the prior core state.
  defp director(handed_prior_core_state) do
    %State{
      cluster: TestCluster,
      epoch: 5,
      config: Config.new([Node.self()]),
      prior_core_state: handed_prior_core_state,
      node_capabilities: %{coordination: [Node.self()], log: [Node.self()], materializer: [Node.self()]}
    }
  end

  defp start_recovery(handed_prior_core_state) do
    capture_log(fn -> send(self(), {:recovered, Recovery.try_to_recover(director(handed_prior_core_state))}) end)
    assert_received {:recovered, result}
    result
  end

  test "a follower's nil prior core state does not re-initialize a cluster the bootstrap names logs for",
       %{tmp_dir: tmp_dir} do
    with_bootstrap(tmp_dir, ["log-a"])

    result = start_recovery(nil)

    assert result.prior_core_state == %{logs: %{"log-a" => []}, system_materializers: %{}}
    # InitializationPhase seeds log vacancies; recovering from "log-a" must not.
    refute Map.has_key?(result.recovery_attempt.logs, {:vacancy, 1})
  end

  test "a stale prior core state yields to the logs the bootstrap names now", %{tmp_dir: tmp_dir} do
    with_bootstrap(tmp_dir, ["log-b"])

    result = start_recovery(%{logs: %{"retired-log" => []}, system_materializers: %{}})

    assert result.prior_core_state == %{logs: %{"log-b" => []}, system_materializers: %{}}
  end

  test "an unreadable bootstrap is not mistaken for a fresh cluster; the director retries the read" do
    Application.put_env(:bedrock, TestCluster, object_storage: ObjectStorage.backend(UnreachableStorage))

    capture_log(fn -> send(self(), {:continued, Server.handle_continue(:start_recovery, director(nil))}) end)

    assert_received {:continued, {:noreply, %State{state: :starting, recovery_attempt: nil}}}
    assert_receive {:timeout, :start_recovery}, 2_000
  end
end
