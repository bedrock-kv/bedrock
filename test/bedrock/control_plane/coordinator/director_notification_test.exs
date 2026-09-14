defmodule Bedrock.ControlPlane.Coordinator.DirectorNotificationTest do
  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.Config
  alias Bedrock.ControlPlane.Coordinator.DirectorManagement
  alias Bedrock.ControlPlane.Coordinator.RaftAdapter
  alias Bedrock.ControlPlane.Coordinator.Server
  alias Bedrock.ControlPlane.Coordinator.State
  alias Bedrock.Raft
  alias Bedrock.Raft.Log.InMemoryLog

  setup do
    owner = self()

    director =
      spawn(fn ->
        receive do
          message -> send(owner, {:director_message, message})
        end
      end)

    on_exit(fn -> Process.exit(director, :kill) end)

    state = %State{
      director: director,
      epoch: 7,
      raft: leader_raft(),
      leader_node: node(),
      my_node: node(),
      transaction_system_layout: %{epoch: 7, logs: %{current: []}},
      prior_core_state: %{logs: %{current: []}},
      config: %{value: :current},
      tsl_subscribers: MapSet.new([self()])
    }

    state = %{state | director_raft_term: 1, raft_term: 1}
    %{state: state, director: director}
  end

  test "unattributed stale layout cannot overwrite routing or epoch", %{state: state} do
    assert {:noreply, ^state} =
             Server.handle_cast(
               {:notify_transaction_system_layout, %{epoch: 6, logs: %{retired: []}}, %{logs: %{retired: []}}},
               state
             )

    refute_received {:tsl_updated, _}
  end

  test "unattributed stale config cannot overwrite cache", %{state: state} do
    assert {:noreply, ^state} = Server.handle_cast({:notify_config, %{value: :retired}}, state)
  end

  for kind <- [:layout, :config] do
    test "#{kind} rejects foreign PID even at equal generation", %{state: state} do
      assert {:noreply, ^state} = Server.handle_cast(message(unquote(kind), {self(), 7, 1}), state)
      refute_received {:tsl_updated, _}
    end

    test "#{kind} rejects a current PID after leadership loss", %{state: state, director: director} do
      follower = %{state | leader_node: :other}
      assert {:noreply, ^follower} = Server.handle_cast(message(unquote(kind), {director, 7, 1}), follower)
      refute_received {:tsl_updated, _}
    end

    test "#{kind} accepts newest once and rejects duplicate/conflicting/older publications", %{
      state: state,
      director: director
    } do
      latest = message(unquote(kind), {director, 7, 3})
      assert {:noreply, updated} = Server.handle_cast(latest, state)
      assert updated.epoch == 7
      if unquote(kind) == :layout, do: assert_received({:tsl_updated, %{logs: %{new: []}}})
      assert {:noreply, ^updated} = Server.handle_cast(latest, updated)
      assert {:noreply, ^updated} = Server.handle_cast(message(unquote(kind), {director, 7, 3}, :conflict), updated)
      assert {:noreply, ^updated} = Server.handle_cast(message(unquote(kind), {director, 7, 2}, :older), updated)
      refute_received {:tsl_updated, _}
    end
  end

  test "config and layout have independent ordering so reordered channels cannot lose a layout", %{
    state: state,
    director: director
  } do
    assert {:noreply, updated} = Server.handle_cast(message(:config, {director, 7, 4}), state)
    assert {:noreply, updated} = Server.handle_cast(message(:layout, {director, 7, 3}), updated)
    assert updated.config == %{value: :new}
    assert updated.transaction_system_layout.logs == %{new: []}
    assert_received {:tsl_updated, _}
  end

  test "current Director cannot change epoch through attributed layout", %{state: state, director: director} do
    for epoch <- [6, 8] do
      assert {:noreply, ^state} = Server.handle_cast(message(:layout, {director, epoch, 1}), state)

      assert {:noreply, ^state} =
               Server.handle_cast(
                 {:notify_transaction_system_layout, {director, 7, 1}, %{epoch: epoch, logs: %{}}, %{logs: %{}}},
                 state
               )
    end

    refute_received {:tsl_updated, _}
  end

  for window <- [:stepped_down, :newer_term_leader] do
    test "#{window} rejects layout/config before cached leadership callback", %{state: state, director: director} do
      raced = %{state | raft: authority_window(state.raft, unquote(window))}
      assert raced.leader_node == raced.my_node
      assert raced.director == director
      assert raced.epoch == 7
      assert raced.director_raft_term == 1

      for kind <- [:layout, :config] do
        assert {:noreply, ^raced} = Server.handle_cast(message(kind, {director, 7, 1}), raced)
      end

      refute_received {:tsl_updated, _}
    end

    test "#{window} rejects ping before cached leadership callback", %{state: state, director: director} do
      raced = %{state | raft: authority_window(state.raft, unquote(window))}
      assert {:noreply, ^raced} = Server.handle_cast({:ping, {7, director}}, raced)
      refute_receive {:director_message, {:"$gen_cast", {:pong, _}}}, 50
    end
  end

  test "nil Raft cannot authorize publication or ping", %{state: state, director: director} do
    state = %{state | raft: nil}
    assert {:noreply, ^state} = Server.handle_cast({:ping, {7, director}}, state)
    refute_receive {:director_message, {:"$gen_cast", {:pong, _}}}, 50
    assert {:noreply, ^state} = Server.handle_cast(message(:config, {director, 7, 1}), state)
  end

  test "leadership cleanup clears cached layout even when Director is already absent", %{state: state} do
    retired =
      DirectorManagement.cleanup_director_on_leadership_loss(%{state | director: :unavailable, leader_node: :other})

    assert retired.transaction_system_layout == nil
    assert_received {:tsl_updated, nil}
  end

  test "retirement clears routing even without successful replacement and duplicate DOWN is inert", %{
    state: state,
    director: director
  } do
    retired = DirectorManagement.handle_director_failure(state, director, :killed)
    assert retired.transaction_system_layout == nil
    assert retired.prior_core_state == state.prior_core_state
    assert_received {:tsl_updated, nil}
    assert DirectorManagement.handle_director_failure(retired, director, :killed) == retired
    refute_received {:tsl_updated, _}
  end

  test "current authoritative Director receives pong", %{state: state, director: director} do
    assert {:noreply, ^state} = Server.handle_cast({:ping, {7, director}}, state)
    assert_receive {:director_message, {:"$gen_cast", {:pong, _}}}
  end

  test "RPC then timer changes authority before queued callbacks are consumed", %{state: state, director: director} do
    assert {:noreply, follower} = Server.handle_cast({:raft, :rpc, {:vote, 2}, :other}, state)
    assert_received {:raft, :leadership_changed, {:undecided, 2}}
    assert follower.leader_node == node()
    assert {:noreply, ^follower} = Server.handle_cast(message(:layout, {director, 7, 1}), follower)
    assert {:noreply, newer} = Server.handle_info({:raft, :timer, :election}, follower)
    assert_received {:raft, :leadership_changed, {_, 3}}
    assert {:noreply, ^newer} = Server.handle_cast(message(:config, {director, 7, 1}), newer)
    assert {:noreply, ^newer} = Server.handle_info({:raft, :leadership_changed, {node(), 1}}, newer)
    assert {:noreply, ^newer} = Server.handle_info({:raft, :leadership_changed, {:undecided, 2}}, newer)
    refute_received {:tsl_updated, _}
  end

  test "duplicate authoritative leadership callback preserves the instance and ordering", %{
    state: state,
    director: director
  } do
    assert {:noreply, state} = Server.handle_cast(message(:layout, {director, 7, 3}), state)
    assert_received {:tsl_updated, _}
    assert {:noreply, ^state} = Server.handle_info({:raft, :leadership_changed, {node(), 1}}, state)
    assert {:noreply, ^state} = Server.handle_cast(message(:layout, {director, 7, 2}), state)
    refute_received {:tsl_updated, _}
  end

  test "changing PID resets ordering but cannot rebind the same PID to a new term", %{state: state, director: director} do
    assert {:noreply, state} = Server.handle_cast(message(:config, {director, 7, 5}), state)
    unchanged = State.Changes.put_director(%{state | raft_term: 3}, director)
    assert unchanged.director_raft_term == 1
    assert unchanged.publication_sequences.config == 5
    replaced = State.Changes.put_director(state, self())
    assert replaced.director_raft_term == 1
    assert replaced.publication_sequences == %{config: 0, layout: 0}
    assert {:noreply, accepted} = Server.handle_cast(message(:config, {self(), 7, 1}), replaced)
    assert accepted.publication_sequences.config == 1
    assert {:noreply, ^accepted} = Server.handle_cast(message(:config, {director, 7, 6}), accepted)
    assert {:noreply, ^accepted} = Server.handle_info({:DOWN, make_ref(), :process, director, :killed}, accepted)
  end

  test "invalid sequence cannot publish", %{state: state, director: director} do
    for sequence <- [0, -1, nil, 1.5, :new] do
      assert {:noreply, ^state} = Server.handle_cast(message(:config, {director, 7, sequence}), state)
    end
  end

  defmodule TestCluster do
    @moduledoc false
    def name, do: "notification_lifecycle"
    def otp_name(_), do: :notification_lifecycle_test
  end

  test "new term retires supervised old Director before registering a new instance", %{state: state} do
    sup = start_supervised!({DynamicSupervisor, strategy: :one_for_one})
    {:ok, old} = DynamicSupervisor.start_child(sup, {Agent, fn -> :old_director end})
    down = Process.monitor(old)

    state = %{
      state
      | director: old,
        supervisor_otp_name: sup,
        cluster: TestCluster,
        config: Config.new([]),
        raft: authority_window(state.raft, :newer_term_leader)
    }

    assert {:noreply, replaced} = Server.handle_info({:raft, :leadership_changed, {node(), 3}}, state)
    assert_receive {:DOWN, ^down, :process, ^old, :shutdown}
    assert_received {:tsl_updated, nil}
    assert is_pid(replaced.director)
    assert replaced.director != old
    assert replaced.director_raft_term == 3
    assert replaced.raft_term == 3
    assert replaced.transaction_system_layout == nil
    assert replaced.prior_core_state == state.prior_core_state
    assert {:noreply, ^replaced} = Server.handle_info({:raft, :leadership_changed, {node(), 3}}, replaced)
    refute_received {:tsl_updated, _}
  end

  test "failed startup still clears routing and preserves prior core", %{state: state} do
    sup = start_supervised!({DynamicSupervisor, strategy: :one_for_one, max_children: 0})
    state = %{state | director: :unavailable, supervisor_otp_name: sup, cluster: TestCluster}
    failed = DirectorManagement.try_to_start_director(state)
    assert failed.director == :unavailable
    assert failed.transaction_system_layout == nil
    assert failed.prior_core_state == state.prior_core_state
    assert_received {:tsl_updated, nil}
  end

  for ending <- [:leadership_loss, :epoch_end] do
    test "#{ending} terminates supervised Director and clears once", %{state: state} do
      sup = start_supervised!({DynamicSupervisor, strategy: :one_for_one})
      {:ok, old} = DynamicSupervisor.start_child(sup, {Agent, fn -> :old_director end})
      down = Process.monitor(old)
      state = %{state | director: old, supervisor_otp_name: sup}

      retired =
        case unquote(ending) do
          :leadership_loss ->
            {:noreply, stepped} = Server.handle_cast({:raft, :rpc, {:vote, 2}, :other}, state)
            {:noreply, retired} = Server.handle_info({:raft, :leadership_changed, {:undecided, 2}}, stepped)
            retired

          :epoch_end ->
            DirectorManagement.shutdown_director_if_running(state)
        end

      assert_receive {:DOWN, ^down, :process, ^old, :shutdown}
      assert_received {:tsl_updated, nil}
      assert retired.transaction_system_layout == nil
      assert retired.director_raft_term == nil
      assert {:noreply, ^retired} = Server.handle_info({:DOWN, make_ref(), :process, old, :shutdown}, retired)
      refute_received {:tsl_updated, _}
    end
  end

  defp leader_raft do
    raft = node() |> Raft.new([], InMemoryLog.new(:tuple), RaftAdapter) |> Raft.handle_event(:election, :timer)
    assert Raft.am_i_the_leader?(raft)
    assert Raft.leadership(raft) == {node(), 1}
    raft
  end

  defp authority_window(raft, window) do
    # Real protocol transition queues the adapter callback in this process;
    # deliberately leave it unprocessed, exactly as a preceding cast would.
    stepped_down = Raft.handle_event(raft, {:vote, 2}, :other)
    refute Raft.am_i_the_leader?(stepped_down)
    assert_received {:raft, :leadership_changed, {:undecided, 2}}

    case window do
      :stepped_down ->
        stepped_down

      :newer_term_leader ->
        reelected = Raft.handle_event(stepped_down, :election, :timer)
        assert Raft.am_i_the_leader?(reelected)
        assert Raft.leadership(reelected) == {node(), 3}
        assert_received {:raft, :leadership_changed, {_, 3}}
        reelected
    end
  end

  defp message(kind, identity, value \\ :new)

  defp message(:layout, identity, value),
    do: {:notify_transaction_system_layout, identity, %{epoch: 7, logs: %{value => []}}, %{logs: %{value => []}}}

  defp message(:config, identity, value), do: {:notify_config, identity, %{value: value}}
end
