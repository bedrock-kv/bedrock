defmodule Bedrock.ControlPlane.Director.Recovery.SequencerStartupPhaseTest do
  use ExUnit.Case, async: true

  import Bedrock.Test.ControlPlane.RecoveryTestSupport

  alias Bedrock.ControlPlane.Director.Recovery.SequencerStartupPhase
  alias Bedrock.DataPlane.Sequencer.Server

  # Mock cluster module for testing
  defmodule TestCluster do
    @moduledoc false
    def otp_name(:sup), do: :test_supervisor
    def otp_name(:sequencer), do: :test_sequencer
  end

  # Helper functions for common setup patterns
  defp create_recovery_attempt(cluster, epoch, version_vector, sequencer \\ nil) do
    recovery_attempt()
    |> with_cluster(cluster)
    |> with_epoch(epoch)
    |> with_version_vector(version_vector)
    |> with_sequencer(sequencer)
  end

  defp create_capture_agent do
    {:ok, agent} = Agent.start_link(fn -> nil end)
    agent
  end

  describe "execute/1" do
    test "transitions to error state when sequencer creation fails" do
      # This test verifies that the phase properly handles failures
      # We can't easily mock the DynamicSupervisor call, but we can test
      # that the phase structure is correct for error handling

      recovery_attempt = create_recovery_attempt(TestCluster, 1, {0, 100})

      # The actual execution will fail because TestCluster.otp_name(:sup)
      # doesn't point to a real supervisor, but now it should return an error
      # instead of exiting thanks to our try-catch fix
      assert {%{sequencer: nil}, {:error, {:failed_to_start, :sequencer, _, {:supervisor_exit, _}}}} =
               SequencerStartupPhase.execute(recovery_attempt, %{node_tracking: nil})
    end
  end

  describe "execute/1 with mocked starter functions" do
    test "transitions to next phase when sequencer starts successfully" do
      agent = create_capture_agent()
      sequencer_pid = spawn(fn -> :ok end)

      start_supervised_fn = fn child_spec, node ->
        Agent.update(agent, fn _ -> {child_spec, node} end)
        {:ok, sequencer_pid}
      end

      recovery_attempt = create_recovery_attempt(TestCluster, 42, {10, 100})
      context = %{start_supervised_fn: start_supervised_fn}

      # Should transition to MaterializerBootstrapPhase with sequencer set
      assert {%{sequencer: ^sequencer_pid}, Bedrock.ControlPlane.Director.Recovery.MaterializerBootstrapPhase} =
               SequencerStartupPhase.execute(recovery_attempt, context)

      # Verify the child spec passed to start_supervised_fn
      assert {
               %{
                 id: {Server, TestCluster, 42},
                 start: {GenServer, :start_link, [Server, {_director, 42, 100}, [name: :test_sequencer]]}
               },
               captured_node
             } = Agent.get(agent, & &1)

      # Should be called on current node
      assert captured_node == node()
    end

    test "returns error when starter function fails" do
      start_supervised_fn = fn _child_spec, _node ->
        {:error, :startup_failed}
      end

      recovery_attempt = create_recovery_attempt(TestCluster, 1, {0, 100})
      context = %{start_supervised_fn: start_supervised_fn}

      assert {%{sequencer: nil}, {:error, {:failed_to_start, :sequencer, _, :startup_failed}}} =
               SequencerStartupPhase.execute(recovery_attempt, context)
    end

    test "uses correct last committed version from version vector" do
      agent = create_capture_agent()

      start_supervised_fn = fn child_spec, _node ->
        Agent.update(agent, fn _ -> child_spec end)
        {:ok, spawn(fn -> :ok end)}
      end

      recovery_attempt = create_recovery_attempt(TestCluster, 5, {25, 250})
      context = %{start_supervised_fn: start_supervised_fn}

      {_result, _next_phase} = SequencerStartupPhase.execute(recovery_attempt, context)

      # Verify epoch 5 and last_committed_version 250 are used
      assert %{start: {GenServer, :start_link, [_, {_director, 5, 250}, _]}} =
               Agent.get(agent, & &1)
    end
  end
end
