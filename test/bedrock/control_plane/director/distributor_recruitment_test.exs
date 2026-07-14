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

      # A non-:normal exit schedules a delayed re-recruit (the retry timer
      # puts a floor under a crash-looping distributor).
      assert {:noreply, down_state} = Server.handle_info({:DOWN, make_ref(), :process, original, :killed}, state)
      assert down_state.distributor == nil
      assert_receive {:timeout, :retry_start_distributor}, 2_000

      assert {:noreply, new_state} = Server.handle_info({:timeout, :retry_start_distributor}, down_state)

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

      # A :normal exit means the distributor ceded to a newer epoch: a newer
      # director owns coverage now, so this director must not re-recruit.
      assert {:noreply, new_state} = Server.handle_info({:DOWN, ref, :process, distributor, :normal}, state)

      assert new_state.distributor == nil
    end

    test "a newer-epoch director does not adopt a stale distributor under the shared name" do
      # An epoch-5 distributor is still registered under the shared otp name
      # when an epoch-6 director recruits: the starter's already_started
      # mapping would hand back the stale pid, so the director must epoch
      # check it (the stale distributor stops itself) and retry instead.
      stale_state = Server.maybe_start_distributor(running_state())
      stale = stale_state.distributor
      ref = Process.monitor(stale)

      new_director_state = Server.maybe_start_distributor(running_state(%{epoch: 6}))

      assert new_director_state.distributor == nil
      assert_receive {:DOWN, ^ref, :process, ^stale, :normal}
      assert_receive {:timeout, :retry_start_distributor}, 2_000

      # Once the stale registration has cleared, the retry recruits a fresh
      # epoch-6 distributor.
      assert {:noreply, recruited} = Server.handle_info({:timeout, :retry_start_distributor}, new_director_state)

      assert is_pid(recruited.distributor)
      assert recruited.distributor != stale
      assert %DistributorState{epoch: 6} = :sys.get_state(recruited.distributor)
    end
  end

  describe "recruitment retry" do
    test "a failed distributor start schedules a retry" do
      # Point the starter at a nonexistent supervisor so the start fails.
      defmodule NoSupCluster do
        @moduledoc false
        def otp_name(component), do: :"distributor_recruitment_no_sup_#{component}"
      end

      state = Server.maybe_start_distributor(running_state(%{cluster: NoSupCluster}))

      assert state.distributor == nil
      assert_receive {:timeout, :retry_start_distributor}, 2_000
    end

    test "the retry message re-attempts recruitment" do
      assert {:noreply, state} = Server.handle_info({:timeout, :retry_start_distributor}, running_state())

      assert is_pid(state.distributor)
      assert Process.alive?(state.distributor)
    end
  end
end
