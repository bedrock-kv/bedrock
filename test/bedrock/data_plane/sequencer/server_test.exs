defmodule Bedrock.DataPlane.Sequencer.ServerTest do
  use ExUnit.Case, async: true
  alias Bedrock.DataPlane.Sequencer.Server
  alias Bedrock.DataPlane.Sequencer.State

  alias Faker

  setup do
    cluster = :test_cluster
    controller = :test_controller
    epoch = Faker.random_between(0, 1_000_000)
    started_at = :os.system_time(:millisecond)
    otp_name = :test_otp_name
    last_committed_version = <<0x00>>

    state = %State{
      cluster: cluster,
      controller: controller,
      epoch: epoch,
      started_at: started_at,
      last_timestamp_at: 0,
      sequence: 0,
      max_transactions_per_ms: 1000,
      last_committed_version: last_committed_version
    }

    {:ok, state: state, otp_name: otp_name}
  end

  test "child_spec/1 returns correct spec", %{
    state: %State{
      cluster: cluster,
      controller: controller,
      epoch: epoch,
      started_at: started_at,
      last_committed_version: last_committed_version
    },
    otp_name: otp_name
  } do
    opts = [
      cluster: cluster,
      controller: controller,
      epoch: epoch,
      started_at: started_at,
      otp_name: otp_name,
      last_committed_version: last_committed_version
    ]

    assert %{
             id: Bedrock.DataPlane.Sequencer.Server,
             restart: :temporary,
             start:
               {GenServer, :start_link,
                [
                  Bedrock.DataPlane.Sequencer.Server,
                  {:test_cluster, :test_controller, ^epoch, ^started_at, ^last_committed_version},
                  [name: :test_otp_name]
                ]}
           } = Server.child_spec(opts)
  end

  test "init/1 initializes state correctly", %{
    state: %State{
      cluster: cluster,
      controller: controller,
      epoch: epoch,
      started_at: expected_started_at,
      last_committed_version: last_committed_version
    }
  } do
    {:ok, t, {:continue, {:start_clock, started_at}}} =
      Server.init({cluster, controller, epoch, expected_started_at, last_committed_version})

    assert started_at == expected_started_at

    assert t == %State{
             cluster: cluster,
             controller: controller,
             epoch: epoch,
             last_committed_version: last_committed_version,
             last_timestamp_at: 0,
             max_transactions_per_ms: 1000,
             sequence: 0,
             started_at: 0
           }
  end

  test "handle_continue/2 starts clock correctly", %{state: state} do
    {:noreply, t} = Server.handle_continue({:start_clock, state.started_at}, state)
    assert t.started_at < 0
  end

  test "handle_call :next_read_version returns the last committed version", %{state: state} do
    assert {:reply, {:ok, state.last_committed_version}, state} ==
             Server.handle_call(:next_read_version, self(), state)
  end

  test "handle_call :next_commit_version returns the next commit version", %{
    state: initial_state
  } do
    1..2_000
    |> Enum.reduce(
      initial_state,
      fn _i, initial_state ->
        expected_last_committed_version = initial_state.last_committed_version

        {:reply, {:ok, {^expected_last_committed_version, next_version}}, new_state} =
          Server.handle_call(:next_commit_version, self(), initial_state)

        assert next_version > initial_state.last_committed_version
        assert new_state.last_committed_version == next_version
        new_state
      end
    )
  end

  test "handle_cast :recruitment_invitation with higher epoch updates state", %{
    state: initial_state
  } do
    new_epoch = initial_state.epoch + 1
    system_time_started_at_in_ms = :os.system_time(:millisecond)
    last_committed_version = 1

    {:noreply, new_state, {:continue, {:start_clock, ^system_time_started_at_in_ms}}} =
      Server.handle_cast(
        {:recruitment_invitation, :new_controller, new_epoch, system_time_started_at_in_ms,
         last_committed_version},
        initial_state
      )

    assert new_state.epoch == new_epoch
    assert new_state.controller == :new_controller
    assert new_state.last_committed_version == last_committed_version
  end

  test "handle_cast :recruitment_invitation with lower or equal epoch does not update state", %{
    state: initial_state
  } do
    {:noreply, new_state} =
      Server.handle_cast(
        {:recruitment_invitation, :new_controller, initial_state.epoch,
         :os.system_time(:millisecond), 1},
        initial_state
      )

    assert new_state == initial_state
  end
end
