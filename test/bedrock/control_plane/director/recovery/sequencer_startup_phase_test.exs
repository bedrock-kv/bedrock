defmodule Bedrock.ControlPlane.Director.Recovery.SequencerStartupPhaseTest do
  use ExUnit.Case, async: true

  import RecoveryTestSupport

  alias Bedrock.ControlPlane.Director.Recovery.SequencerStartupPhase
  alias Bedrock.DataPlane.Sequencer.Server

  # Mock cluster module for testing
  defmodule TestCluster do
    @moduledoc false
    def otp_name(:sup), do: :test_supervisor
    def otp_name(:sequencer), do: :test_sequencer
  end

  describe "execute/1" do
    test "transitions to error state when sequencer creation fails" do
      # This test verifies that the phase properly handles failures
      # We can't easily mock the DynamicSupervisor call, but we can test
      # that the phase structure is correct for error handling

      recovery_attempt =
        recovery_attempt()
        |> with_cluster(TestCluster)
        |> with_epoch(1)
        |> with_version_vector({0, 100})
        |> with_sequencer(nil)

      # The actual execution will fail because TestCluster.otp_name(:sup)
      # doesn't point to a real supervisor, but now it should return an error
      # instead of exiting thanks to our try-catch fix
      {result, next_phase_or_error} =
        SequencerStartupPhase.execute(recovery_attempt, %{node_tracking: nil})

      # Should be error due to supervisor not existing - halts recovery
      assert {:error, {:failed_to_start, :sequencer, _, {:supervisor_exit, _}}} =
               next_phase_or_error

      assert result.sequencer == nil
    end
  end

  describe "execute/1 with mocked starter functions" do
    test "transitions to next phase when sequencer starts successfully" do
      agent = fn -> nil end |> Agent.start_link() |> elem(1)
      sequencer_pid = spawn(fn -> :ok end)

      start_supervised_fn = fn child_spec, node ->
        Agent.update(agent, fn _ -> {child_spec, node} end)
        {:ok, sequencer_pid}
      end

      recovery_attempt =
        recovery_attempt()
        |> with_cluster(TestCluster)
        |> with_epoch(42)
        |> with_version_vector({10, 100})
        |> with_sequencer(nil)

      context = %{start_supervised_fn: start_supervised_fn}

      {result, next_phase} = SequencerStartupPhase.execute(recovery_attempt, context)

      # Should transition to next phase
      assert next_phase == Bedrock.ControlPlane.Director.Recovery.CommitProxyStartupPhase
      assert result.sequencer == sequencer_pid

      # Verify the child spec passed to start_supervised_fn
      {captured_child_spec, captured_node} = Agent.get(agent, & &1)

      # Should be called on current node
      assert captured_node == node()

      # Verify child spec has correct tuple-based ID format
      assert %{id: {Server, TestCluster, 42}} = captured_child_spec

      # Verify start args contain correct parameters
      assert %{start: {GenServer, :start_link, [Server, start_args, opts]}} = captured_child_spec
      # epoch 42, last_committed_version 100
      assert {_director, 42, 100} = start_args
      assert [name: :test_sequencer] = opts
    end

    test "returns error when starter function fails" do
      start_supervised_fn = fn _child_spec, _node ->
        {:error, :startup_failed}
      end

      recovery_attempt =
        recovery_attempt()
        |> with_cluster(TestCluster)
        |> with_epoch(1)
        |> with_version_vector({0, 100})
        |> with_sequencer(nil)

      context = %{start_supervised_fn: start_supervised_fn}

      {result, error} = SequencerStartupPhase.execute(recovery_attempt, context)

      assert {:error, {:failed_to_start, :sequencer, _, :startup_failed}} = error
      assert result.sequencer == nil
    end

    test "uses correct last committed version from version vector" do
      agent = fn -> nil end |> Agent.start_link() |> elem(1)

      start_supervised_fn = fn child_spec, _node ->
        Agent.update(agent, fn _ -> child_spec end)
        {:ok, spawn(fn -> :ok end)}
      end

      recovery_attempt =
        recovery_attempt()
        |> with_cluster(TestCluster)
        |> with_epoch(5)
        # last_committed_version should be 250
        |> with_version_vector({25, 250})
        |> with_sequencer(nil)

      context = %{start_supervised_fn: start_supervised_fn}

      {_result, _next_phase} = SequencerStartupPhase.execute(recovery_attempt, context)

      captured_child_spec = Agent.get(agent, & &1)
      assert %{start: {GenServer, :start_link, [_, start_args, _]}} = captured_child_spec
      # epoch 5, last_committed_version 250
      assert {_director, 5, 250} = start_args
    end
  end
end
