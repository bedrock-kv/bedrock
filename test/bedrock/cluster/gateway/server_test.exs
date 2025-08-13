defmodule Bedrock.Cluster.Gateway.ServerTest do
  use ExUnit.Case, async: true

  alias Bedrock.Cluster.Descriptor
  alias Bedrock.Cluster.Gateway.Server
  alias Bedrock.Cluster.Gateway.State
  alias Bedrock.ControlPlane.Config.TransactionSystemLayout

  # Test cluster module
  defmodule TestCluster do
    def name, do: "test_cluster"
    def otp_name(:coordinator), do: :test_coordinator
    def otp_name(component), do: :"test_#{component}"
    def coordinator_ping_timeout_in_ms, do: 5000
    def coordinator_discovery_timeout_in_ms, do: 5000
    def gateway_ping_timeout_in_ms, do: 5000
    def capabilities, do: [:storage, :log]
  end

  describe "child_spec/1" do
    test "creates proper child spec with valid options" do
      opts = [
        cluster: TestCluster,
        descriptor: %Descriptor{coordinator_nodes: [:node1, :node2]},
        path_to_descriptor: "/path/to/descriptor",
        otp_name: :test_gateway,
        capabilities: [:storage, :log]
      ]

      child_spec = Server.child_spec(opts)

      assert child_spec.id == :test_gateway
      assert child_spec.restart == :permanent

      {module, function, args} = child_spec.start
      assert module == GenServer
      assert function == :start_link

      [server_module, init_args, gen_server_opts] = args
      assert server_module == Server
      assert gen_server_opts == [name: :test_gateway]

      {cluster, path, descriptor, mode, capabilities} = init_args
      assert cluster == TestCluster
      assert path == "/path/to/descriptor"
      assert descriptor == %Descriptor{coordinator_nodes: [:node1, :node2]}
      # default mode
      assert mode == :active
      assert capabilities == [:storage, :log]
    end

    test "uses custom mode when provided" do
      opts = [
        cluster: TestCluster,
        descriptor: %Descriptor{coordinator_nodes: [:node1]},
        path_to_descriptor: "/path",
        otp_name: :test_gateway,
        capabilities: [:storage],
        mode: :passive
      ]

      child_spec = Server.child_spec(opts)

      [_server_module, init_args, _gen_server_opts] =
        elem(child_spec.start, 2)

      {_cluster, _path, _descriptor, mode, _capabilities} = init_args
      assert mode == :passive
    end

    test "raises error when cluster option is missing" do
      opts = [
        descriptor: %Descriptor{coordinator_nodes: [:node1]},
        path_to_descriptor: "/path",
        otp_name: :test_gateway,
        capabilities: [:storage]
      ]

      assert_raise RuntimeError, "Missing :cluster option", fn ->
        Server.child_spec(opts)
      end
    end

    test "raises error when descriptor option is missing" do
      opts = [
        cluster: TestCluster,
        path_to_descriptor: "/path",
        otp_name: :test_gateway,
        capabilities: [:storage]
      ]

      assert_raise RuntimeError, "Missing :descriptor option", fn ->
        Server.child_spec(opts)
      end
    end

    test "raises error when path_to_descriptor option is missing" do
      opts = [
        cluster: TestCluster,
        descriptor: %Descriptor{coordinator_nodes: [:node1]},
        otp_name: :test_gateway,
        capabilities: [:storage]
      ]

      assert_raise RuntimeError, "Missing :path_to_descriptor option", fn ->
        Server.child_spec(opts)
      end
    end

    test "raises error when otp_name option is missing" do
      opts = [
        cluster: TestCluster,
        descriptor: %Descriptor{coordinator_nodes: [:node1]},
        path_to_descriptor: "/path",
        capabilities: [:storage]
      ]

      assert_raise RuntimeError, "Missing :otp_name option", fn ->
        Server.child_spec(opts)
      end
    end

    test "raises error when capabilities option is missing" do
      opts = [
        cluster: TestCluster,
        descriptor: %Descriptor{coordinator_nodes: [:node1]},
        path_to_descriptor: "/path",
        otp_name: :test_gateway
      ]

      assert_raise RuntimeError, "Missing :capabilities option", fn ->
        Server.child_spec(opts)
      end
    end

    test "handles empty capabilities list" do
      opts = [
        cluster: TestCluster,
        descriptor: %Descriptor{coordinator_nodes: [:node1]},
        path_to_descriptor: "/path",
        otp_name: :test_gateway,
        capabilities: []
      ]

      child_spec = Server.child_spec(opts)

      [_server_module, init_args, _gen_server_opts] =
        elem(child_spec.start, 2)

      {_cluster, _path, _descriptor, _mode, capabilities} = init_args
      assert capabilities == []
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
      {:ok, state, {:continue, :find_a_live_coordinator}} =
        Server.init({cluster, path, descriptor, mode, capabilities})

      assert %State{} = state
      assert state.node == Node.self()
      assert state.cluster == cluster
      assert state.descriptor == descriptor
      assert state.path_to_descriptor == path
      assert state.known_coordinator == :unavailable
      assert state.transaction_system_layout == nil
      assert state.mode == mode
      assert state.capabilities == capabilities
    end

    test "handles passive mode" do
      cluster = TestCluster
      path = "/test/path"
      descriptor = %Descriptor{coordinator_nodes: [:node1]}
      mode = :passive
      capabilities = [:storage]

      {:ok, state, {:continue, :find_a_live_coordinator}} =
        Server.init({cluster, path, descriptor, mode, capabilities})

      assert state.mode == :passive
      assert state.capabilities == [:storage]
    end

    test "calls telemetry trace_started - verified by no crash" do
      cluster = TestCluster
      path = "/test/path"
      descriptor = %Descriptor{coordinator_nodes: [:node1]}
      mode = :active
      capabilities = [:storage]

      # If telemetry fails, this would crash - we test that it doesn't
      result = Server.init({cluster, path, descriptor, mode, capabilities})
      assert {:ok, _state, {:continue, :find_a_live_coordinator}} = result
    end
  end

  describe "handle_call/3" do
    setup do
      base_state = %State{
        node: :test_node,
        cluster: TestCluster,
        descriptor: %Descriptor{coordinator_nodes: [:node1]},
        path_to_descriptor: "/test/path",
        known_coordinator: :test_coordinator,
        transaction_system_layout: nil,
        mode: :active,
        capabilities: [:storage]
      }

      %{base_state: base_state}
    end

    test "handles get_known_coordinator call when coordinator is available", %{
      base_state: base_state
    } do
      coordinator = :test_coordinator
      state = %{base_state | known_coordinator: coordinator}

      result = Server.handle_call(:get_known_coordinator, :from, state)

      assert {:reply, {:ok, ^coordinator}, ^state} = result
    end

    test "handles get_known_coordinator call when coordinator is unavailable", %{
      base_state: base_state
    } do
      state = %{base_state | known_coordinator: :unavailable}

      result = Server.handle_call(:get_known_coordinator, :from, state)

      assert {:reply, {:error, :unavailable}, ^state} = result
    end

    test "handles get_descriptor call", %{base_state: base_state} do
      descriptor = %Descriptor{coordinator_nodes: [:node1, :node2]}
      state = %{base_state | descriptor: descriptor}

      result = Server.handle_call(:get_descriptor, :from, state)

      assert {:reply, {:ok, ^descriptor}, ^state} = result
    end

    test "begin_transaction call delegates properly (structure test)", %{base_state: base_state} do
      opts = [key_codec: TestKeyCodec, value_codec: TestValueCodec]

      # This will call the actual implementation, but we just test that it doesn't crash
      # and returns the expected structure
      result = Server.handle_call({:begin_transaction, opts}, :from, base_state)

      # Should return a reply tuple - exact result depends on coordinator availability
      assert match?({:reply, _result, _state}, result)
    end

    test "renew_read_version_lease call delegates properly (structure test)", %{
      base_state: base_state
    } do
      read_version = 12_345

      # This will call the actual implementation
      result = Server.handle_call({:renew_read_version_lease, read_version}, :from, base_state)

      # Should return a reply tuple
      assert match?({:reply, _result, _state}, result)
    end
  end

  describe "handle_info/3" do
    setup do
      base_state = %State{
        node: :test_node,
        cluster: TestCluster,
        descriptor: %Descriptor{coordinator_nodes: [:node1]},
        path_to_descriptor: "/test/path",
        known_coordinator: :test_coordinator,
        transaction_system_layout: nil,
        mode: :active,
        capabilities: [:storage]
      }

      %{base_state: base_state}
    end

    test "handles timeout message for coordinator discovery", %{base_state: base_state} do
      result = Server.handle_info({:timeout, :find_a_live_coordinator}, base_state)

      assert {:noreply, ^base_state, {:continue, :find_a_live_coordinator}} = result
    end

    test "handles tsl_updated message", %{base_state: base_state} do
      new_tsl = TransactionSystemLayout.default()

      result = Server.handle_info({:tsl_updated, new_tsl}, base_state)

      assert {:noreply, updated_state} = result
      assert updated_state.transaction_system_layout == new_tsl
      # Verify other fields are preserved
      assert updated_state.node == base_state.node
      assert updated_state.cluster == base_state.cluster
      assert updated_state.known_coordinator == base_state.known_coordinator
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

      result = Server.handle_info({:DOWN, ref, :process, other_process_name, reason}, base_state)

      assert {:noreply, ^base_state} = result
    end
  end

  describe "handle_cast/3" do
    setup do
      base_state = %State{
        node: :test_node,
        cluster: TestCluster,
        descriptor: %Descriptor{coordinator_nodes: [:node1]},
        path_to_descriptor: "/test/path",
        known_coordinator: :test_coordinator,
        transaction_system_layout: nil,
        mode: :active,
        capabilities: [:storage]
      }

      %{base_state: base_state}
    end

    test "handles advertise_worker cast (structure test)", %{base_state: base_state} do
      # Set coordinator to unavailable to avoid coordinator calls
      state = %{base_state | known_coordinator: :unavailable}
      worker_pid = spawn(fn -> :timer.sleep(100) end)

      result = Server.handle_cast({:advertise_worker, worker_pid}, state)

      # Should return noreply tuple - exact state changes depend on implementation
      assert match?({:noreply, _state}, result)

      # Clean up
      Process.exit(worker_pid, :kill)
    end
  end

  describe "state consistency and integration" do
    test "state transitions maintain immutability expectations" do
      initial_state = %State{
        node: :test_node,
        cluster: TestCluster,
        descriptor: %Descriptor{coordinator_nodes: [:node1]},
        path_to_descriptor: "/test/path",
        known_coordinator: :unavailable,
        transaction_system_layout: nil,
        mode: :active,
        capabilities: [:storage]
      }

      # Test TSL update doesn't modify original state
      new_tsl = TransactionSystemLayout.default()
      {:noreply, updated_state} = Server.handle_info({:tsl_updated, new_tsl}, initial_state)

      # Original state should be unchanged
      assert initial_state.transaction_system_layout == nil
      # Updated state should have new TSL
      assert updated_state.transaction_system_layout == new_tsl
      # Other fields should be identical (referential equality)
      assert updated_state.node == initial_state.node
      assert updated_state.cluster == initial_state.cluster
    end

    test "handle_call functions preserve state correctly" do
      descriptor = %Descriptor{coordinator_nodes: [:node1, :node2]}

      state = %State{
        node: :test_node,
        cluster: TestCluster,
        descriptor: descriptor,
        path_to_descriptor: "/test/path",
        known_coordinator: :test_coordinator,
        transaction_system_layout: nil,
        mode: :active,
        capabilities: [:storage]
      }

      # Test get_descriptor preserves state exactly
      {:reply, {:ok, returned_descriptor}, returned_state} =
        Server.handle_call(:get_descriptor, :from, state)

      assert returned_descriptor == descriptor
      # Should be exact same reference
      assert returned_state == state

      # Test get_known_coordinator preserves state exactly
      {:reply, {:ok, coordinator}, returned_state2} =
        Server.handle_call(:get_known_coordinator, :from, state)

      assert coordinator == :test_coordinator
      # Should be exact same reference
      assert returned_state2 == state
    end

    test "handle_continue returns expected structure for find_a_live_coordinator" do
      base_state = %State{
        node: :test_node,
        cluster: TestCluster,
        # Empty to avoid external calls
        descriptor: %Descriptor{coordinator_nodes: []},
        path_to_descriptor: "/test/path",
        known_coordinator: :unavailable,
        transaction_system_layout: nil,
        mode: :active,
        capabilities: [:storage]
      }

      # This will call the real discovery implementation but with no coordinator nodes
      result = Server.handle_continue(:find_a_live_coordinator, base_state)

      # Should return noreply tuple regardless of discovery success/failure
      assert match?({:noreply, _state}, result)
    end

    test "multiple handle_info calls maintain state correctly" do
      initial_state = %State{
        node: :test_node,
        cluster: TestCluster,
        descriptor: %Descriptor{coordinator_nodes: [:node1]},
        path_to_descriptor: "/test/path",
        known_coordinator: :test_coordinator,
        transaction_system_layout: nil,
        mode: :active,
        capabilities: [:storage]
      }

      # First TSL update
      tsl1 = TransactionSystemLayout.default()
      {:noreply, state1} = Server.handle_info({:tsl_updated, tsl1}, initial_state)
      assert state1.transaction_system_layout == tsl1

      # Second TSL update on the already-updated state
      # Create a different TSL structure for testing
      tsl2 = %{tsl1 | epoch: 2}
      {:noreply, state2} = Server.handle_info({:tsl_updated, tsl2}, state1)
      assert state2.transaction_system_layout == tsl2

      # Verify other state fields are preserved through both updates
      assert state2.node == initial_state.node
      assert state2.cluster == initial_state.cluster
      assert state2.known_coordinator == initial_state.known_coordinator
      assert state2.mode == initial_state.mode
      assert state2.capabilities == initial_state.capabilities
    end
  end

  describe "GenServer behavior compliance" do
    test "all GenServer callbacks return properly structured responses" do
      # Test that all callbacks return valid GenServer response tuples
      state = %State{
        node: :test_node,
        cluster: TestCluster,
        descriptor: %Descriptor{coordinator_nodes: [:node1]},
        path_to_descriptor: "/test/path",
        known_coordinator: :test_coordinator,
        transaction_system_layout: nil,
        mode: :active,
        capabilities: [:storage]
      }

      # Test init
      init_result =
        Server.init({TestCluster, "/path", %Descriptor{coordinator_nodes: [:node1]}, :active, []})

      assert match?({:ok, %State{}, {:continue, :find_a_live_coordinator}}, init_result)

      # Test handle_continue with unavailable coordinator to avoid external calls
      state_unavailable = %{state | known_coordinator: :unavailable}
      continue_result = Server.handle_continue(:find_a_live_coordinator, state_unavailable)
      assert match?({:noreply, %State{}}, continue_result)

      # Test handle_call variants
      call_result1 = Server.handle_call(:get_descriptor, :from, state)
      assert match?({:reply, {:ok, %Descriptor{}}, %State{}}, call_result1)

      call_result2 = Server.handle_call(:get_known_coordinator, :from, state)
      assert match?({:reply, {:ok, _}, %State{}}, call_result2)

      # Test handle_info variants
      info_result1 = Server.handle_info({:timeout, :find_a_live_coordinator}, state)
      assert match?({:noreply, %State{}, {:continue, :find_a_live_coordinator}}, info_result1)

      info_result2 = Server.handle_info({:tsl_updated, TransactionSystemLayout.default()}, state)
      assert match?({:noreply, %State{}}, info_result2)

      # Test handle_cast with unavailable coordinator to avoid external calls
      state_unavailable_cast = %{state | known_coordinator: :unavailable}
      worker_pid = spawn(fn -> :ok end)
      cast_result = Server.handle_cast({:advertise_worker, worker_pid}, state_unavailable_cast)
      assert match?({:noreply, %State{}}, cast_result)
      Process.exit(worker_pid, :kill)
    end
  end
end
