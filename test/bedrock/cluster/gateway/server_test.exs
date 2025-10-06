defmodule Bedrock.Cluster.Gateway.ServerTest do
  use ExUnit.Case, async: true

  import Bedrock.Test.Gateway.ServerTestSupport

  alias Bedrock.Cluster.Descriptor
  alias Bedrock.Cluster.Gateway.Server
  alias Bedrock.Cluster.Gateway.State
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout
  alias Bedrock.Test.Gateway.ServerTestSupport.TestCluster

  # Use the shared test cluster
  describe "child_spec/1" do
    test "creates proper child spec with valid options" do
      opts = valid_child_spec_opts()

      assert %{
               id: :test_gateway,
               restart: :permanent,
               start:
                 {GenServer, :start_link,
                  [
                    Server,
                    {TestCluster, "/path/to/descriptor", %Descriptor{coordinator_nodes: [:node1, :node2]}, :active,
                     [:storage, :log]},
                    [name: :test_gateway]
                  ]}
             } = Server.child_spec(opts)
    end

    test "uses custom mode when provided" do
      opts = valid_child_spec_opts(mode: :passive)

      assert %{
               start:
                 {GenServer, :start_link,
                  [
                    Server,
                    {_cluster, _path, _descriptor, :passive, _capabilities},
                    _gen_server_opts
                  ]}
             } = Server.child_spec(opts)
    end

    test "raises error for missing required options" do
      base_opts = valid_child_spec_opts()

      for required_opt <- required_child_spec_options() do
        opts = Keyword.delete(base_opts, required_opt)

        assert_raise RuntimeError, "Missing :#{required_opt} option", fn ->
          Server.child_spec(opts)
        end
      end
    end

    test "handles empty capabilities list" do
      opts = valid_child_spec_opts(capabilities: [])

      assert %{
               start:
                 {GenServer, :start_link,
                  [
                    Server,
                    {_cluster, _path, _descriptor, _mode, []},
                    _gen_server_opts
                  ]}
             } = Server.child_spec(opts)
    end
  end

  describe "init/1" do
    test "initializes state properly and returns continue tuple" do
      cluster = TestCluster
      path = "/test/path"
      descriptor = %Descriptor{coordinator_nodes: [:node1, :node2]}
      mode = :active
      capabilities = [:storage, :log]

      # Test the actual result - telemetry is called internally
      assert {:ok,
              %State{
                node: node,
                cluster: ^cluster,
                descriptor: ^descriptor,
                path_to_descriptor: ^path,
                known_coordinator: :unavailable,
                transaction_system_layout: nil,
                mode: ^mode,
                capabilities: ^capabilities
              }, {:continue, :find_a_live_coordinator}} = Server.init({cluster, path, descriptor, mode, capabilities})

      assert node == Node.self()
    end

    test "handles passive mode" do
      cluster = TestCluster
      path = "/test/path"
      descriptor = %Descriptor{coordinator_nodes: [:node1]}
      mode = :passive
      capabilities = [:storage]

      assert {:ok,
              %State{
                mode: :passive,
                capabilities: [:storage]
              }, {:continue, :find_a_live_coordinator}} = Server.init({cluster, path, descriptor, mode, capabilities})
    end

    test "calls telemetry trace_started - verified by no crash" do
      # If telemetry fails, this would crash - we test that it doesn't
      result = Server.init({TestCluster, "/test/path", %Descriptor{coordinator_nodes: [:node1]}, :active, [:storage]})
      assert {:ok, _state, {:continue, :find_a_live_coordinator}} = result
    end
  end

  describe "handle_call/3" do
    setup do
      %{base_state: base_state()}
    end

    test "handles get_known_coordinator call when coordinator is available", %{
      base_state: base_state
    } do
      coordinator = :test_coordinator
      state = %{base_state | known_coordinator: coordinator}

      assert {:reply, {:ok, ^coordinator}, ^state} =
               Server.handle_call(:get_known_coordinator, :from, state)
    end

    test "handles get_known_coordinator call when coordinator is unavailable", %{
      base_state: base_state
    } do
      state = %{base_state | known_coordinator: :unavailable}

      assert {:reply, {:error, :unavailable}, ^state} =
               Server.handle_call(:get_known_coordinator, :from, state)
    end

    test "handles get_descriptor call", %{base_state: base_state} do
      descriptor = %Descriptor{coordinator_nodes: [:node1, :node2]}
      state = %{base_state | descriptor: descriptor}

      assert {:reply, {:ok, ^descriptor}, ^state} =
               Server.handle_call(:get_descriptor, :from, state)
    end

    test "begin_transaction call delegates properly (structure test)", %{base_state: base_state} do
      opts = [key_codec: TestKeyCodec, value_codec: TestValueCodec]

      # This will call the actual implementation, but we just test that it doesn't crash
      # and returns the expected structure
      result = Server.handle_call({:begin_transaction, opts}, :from, base_state)

      # Should return a reply tuple - exact result depends on coordinator availability
      assert match?({:reply, _result, _state}, result)
    end
  end

  describe "handle_info/3" do
    setup do
      %{base_state: base_state()}
    end

    test "handles timeout message for coordinator discovery", %{base_state: base_state} do
      assert {:noreply, ^base_state, {:continue, :find_a_live_coordinator}} =
               Server.handle_info({:timeout, :find_a_live_coordinator}, base_state)
    end

    test "handles tsl_updated message", %{base_state: base_state} do
      new_tsl = TransactionSystemLayout.default()

      assert {:noreply,
              %State{
                transaction_system_layout: ^new_tsl,
                node: node,
                cluster: cluster,
                known_coordinator: coordinator
              }} = Server.handle_info({:tsl_updated, new_tsl}, base_state)

      # Verify other fields are preserved
      assert node == base_state.node
      assert cluster == base_state.cluster
      assert coordinator == base_state.known_coordinator
    end

    test "handles DOWN message for matching coordinator name", %{base_state: base_state} do
      ref = make_ref()
      coordinator_name = :test_coordinator
      reason = :normal
      state = %{base_state | known_coordinator: coordinator_name}

      result = Server.handle_info({:DOWN, ref, :process, coordinator_name, reason}, state)

      # Should trigger coordinator change and rediscovery
      assert match?({:noreply, _updated_state, {:continue, :find_a_live_coordinator}}, result)
    end

    test "handles DOWN message for coordinator tuple name matching", %{base_state: base_state} do
      ref = make_ref()
      coordinator_tuple_name = {:test_coordinator, :node1}
      reason = :normal
      state = %{base_state | cluster: TestCluster, known_coordinator: :unavailable}

      result = Server.handle_info({:DOWN, ref, :process, coordinator_tuple_name, reason}, state)

      # Should trigger rediscovery since it matches cluster coordinator pattern
      assert match?({:noreply, _state, {:continue, :find_a_live_coordinator}}, result)
    end

    test "ignores DOWN message for non-matching process", %{base_state: base_state} do
      ref = make_ref()
      other_process_name = :other_process
      reason = :normal

      assert {:noreply, ^base_state} =
               Server.handle_info({:DOWN, ref, :process, other_process_name, reason}, base_state)
    end
  end

  describe "handle_cast/3" do
    setup do
      %{base_state: base_state()}
    end

    test "handles advertise_worker cast (structure test)", %{base_state: base_state} do
      # Set coordinator to unavailable to avoid coordinator calls
      state = %{base_state | known_coordinator: :unavailable}
      worker_pid = spawn(fn -> :timer.sleep(100) end)

      assert match?(
               {:noreply, _state},
               Server.handle_cast({:advertise_worker, worker_pid}, state)
             )

      # Clean up
      Process.exit(worker_pid, :kill)
    end
  end

  describe "state consistency and integration" do
    test "state transitions maintain immutability expectations" do
      initial_state = base_state(known_coordinator: :unavailable)
      new_tsl = TransactionSystemLayout.default()

      assert {:noreply,
              %State{
                transaction_system_layout: ^new_tsl,
                node: node,
                cluster: cluster
              }} = Server.handle_info({:tsl_updated, new_tsl}, initial_state)

      # Original state should be unchanged
      assert initial_state.transaction_system_layout == nil
      # Other fields should be identical (referential equality)
      assert node == initial_state.node
      assert cluster == initial_state.cluster
    end

    test "handle_call functions preserve state correctly" do
      descriptor = %Descriptor{coordinator_nodes: [:node1, :node2]}
      state = base_state(descriptor: descriptor, known_coordinator: :test_coordinator)

      # Test get_descriptor preserves state exactly
      assert {:reply, {:ok, ^descriptor}, ^state} =
               Server.handle_call(:get_descriptor, :from, state)

      # Test get_known_coordinator preserves state exactly
      assert {:reply, {:ok, :test_coordinator}, ^state} =
               Server.handle_call(:get_known_coordinator, :from, state)
    end

    test "handle_continue returns expected structure for find_a_live_coordinator" do
      # Use empty coordinator nodes to avoid external calls
      state =
        base_state(
          descriptor: %Descriptor{coordinator_nodes: []},
          known_coordinator: :unavailable
        )

      # Should return noreply tuple regardless of discovery success/failure
      assert match?(
               {:noreply, _state},
               Server.handle_continue(:find_a_live_coordinator, state)
             )
    end

    test "multiple handle_info calls maintain state correctly" do
      initial_state = base_state(known_coordinator: :test_coordinator)

      # First TSL update
      tsl1 = TransactionSystemLayout.default()

      assert {:noreply, %State{transaction_system_layout: ^tsl1} = state1} =
               Server.handle_info({:tsl_updated, tsl1}, initial_state)

      # Second TSL update on the already-updated state
      tsl2 = %{tsl1 | epoch: 2}

      assert {:noreply,
              %State{
                transaction_system_layout: ^tsl2,
                node: node,
                cluster: cluster,
                known_coordinator: coordinator,
                mode: mode,
                capabilities: capabilities
              }} = Server.handle_info({:tsl_updated, tsl2}, state1)

      # Verify other state fields are preserved through both updates
      assert node == initial_state.node
      assert cluster == initial_state.cluster
      assert coordinator == initial_state.known_coordinator
      assert mode == initial_state.mode
      assert capabilities == initial_state.capabilities
    end
  end

  describe "GenServer behavior compliance" do
    test "all GenServer callbacks return properly structured responses" do
      state = base_state(known_coordinator: :test_coordinator)

      # Test init
      assert match?(
               {:ok, %State{}, {:continue, :find_a_live_coordinator}},
               Server.init({TestCluster, "/path", %Descriptor{coordinator_nodes: [:node1]}, :active, []})
             )

      # Test handle_continue with unavailable coordinator to avoid external calls
      state_unavailable = %{state | known_coordinator: :unavailable}

      assert match?(
               {:noreply, %State{}},
               Server.handle_continue(:find_a_live_coordinator, state_unavailable)
             )

      # Test handle_call variants
      assert match?(
               {:reply, {:ok, %Descriptor{}}, %State{}},
               Server.handle_call(:get_descriptor, :from, state)
             )

      assert match?(
               {:reply, {:ok, _}, %State{}},
               Server.handle_call(:get_known_coordinator, :from, state)
             )

      # Test handle_info variants
      assert match?(
               {:noreply, %State{}, {:continue, :find_a_live_coordinator}},
               Server.handle_info({:timeout, :find_a_live_coordinator}, state)
             )

      assert match?(
               {:noreply, %State{}},
               Server.handle_info({:tsl_updated, TransactionSystemLayout.default()}, state)
             )

      # Test handle_cast with unavailable coordinator to avoid external calls
      state_unavailable_cast = %{state | known_coordinator: :unavailable}
      worker_pid = spawn(fn -> :ok end)

      assert match?(
               {:noreply, %State{}},
               Server.handle_cast({:advertise_worker, worker_pid}, state_unavailable_cast)
             )

      Process.exit(worker_pid, :kill)
    end
  end
end
