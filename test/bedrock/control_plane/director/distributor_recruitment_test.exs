defmodule Bedrock.ControlPlane.Director.DistributorRecruitmentTest do
  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.Director.Server
  alias Bedrock.ControlPlane.Director.State
  alias Bedrock.ControlPlane.Distributor
  alias Bedrock.ControlPlane.Distributor.State, as: DistributorState

  defmodule TestCluster do
    @moduledoc false
    def otp_name, do: :distributor_recruitment_test
    def otp_name(component), do: :"distributor_recruitment_test_#{component}"
  end

  setup do
    start_supervised!({DynamicSupervisor, name: TestCluster.otp_name(:sup), strategy: :one_for_one})
    :ok
  end

  defp running_state(overrides \\ %{}) do
    Map.merge(
      %State{
        state: :running,
        cluster: TestCluster,
        epoch: 5,
        transaction_system_layout: %{shard_layout: %{<<0xFF>> => {0, <<>>}}}
      },
      overrides
    )
  end

  defp kill_and_wait(pid) do
    ref = Process.monitor(pid)
    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^ref, :process, ^pid, :killed}
  end

  describe "recruitment after recovery completes" do
    test "advance_recovery recruits a distributor once the director is running" do
      state = Server.advance_recovery(running_state())

      assert is_pid(state.distributor)
      assert Process.alive?(state.distributor)

      # The distributor is handed the current epoch and TSL shard layout snapshot.
      assert %DistributorState{
               cluster: TestCluster,
               epoch: 5,
               shard_layout: %{<<0xFF>> => {0, <<>>}}
             } = :sys.get_state(state.distributor)

      # Registered under the cluster's otp_name convention.
      assert Process.whereis(TestCluster.otp_name(:distributor)) == state.distributor
    end

    test "no distributor is recruited while recovery has not completed" do
      state = Server.maybe_start_distributor(%State{state: :recovery, cluster: TestCluster, epoch: 5})

      assert state.distributor == nil
      assert Process.whereis(TestCluster.otp_name(:distributor)) == nil
    end

    test "an already-recruited distributor is left alone" do
      state = Server.maybe_start_distributor(running_state())
      assert %{distributor: distributor} = Server.maybe_start_distributor(state)
      assert distributor == state.distributor
    end
  end

  describe "distributor death" do
    test "director re-recruits the distributor on :DOWN without stopping itself" do
      state = Server.maybe_start_distributor(running_state())
      original = state.distributor
      kill_and_wait(original)

      assert {:noreply, new_state} = Server.handle_info({:DOWN, make_ref(), :process, original, :killed}, state)

      assert is_pid(new_state.distributor)
      assert new_state.distributor != original
      assert Process.alive?(new_state.distributor)
      assert %DistributorState{epoch: 5} = :sys.get_state(new_state.distributor)
    end

    test "a :DOWN from any other component still stops the director" do
      state = Server.maybe_start_distributor(running_state())
      other_pid = spawn(fn -> :ok end)

      assert {:stop, {:shutdown, {:component_failure, ^other_pid, :boom}}, _} =
               Server.handle_info({:DOWN, make_ref(), :process, other_pid, :boom}, state)
    end
  end

  describe "epoch guard integration" do
    test "distributor recruited with epoch N terminates when the director signals epoch N+1" do
      state = Server.maybe_start_distributor(running_state())
      distributor = state.distributor
      ref = Process.monitor(distributor)

      :ok = Distributor.notify_epoch_change(distributor, state.epoch + 1)

      assert_receive {:DOWN, ^ref, :process, ^distributor, :normal}

      # The monitoring director would then re-recruit at its current epoch.
      assert {:noreply, new_state} = Server.handle_info({:DOWN, ref, :process, distributor, :normal}, state)

      assert is_pid(new_state.distributor)
      assert %DistributorState{epoch: 5} = :sys.get_state(new_state.distributor)
    end
  end
end
