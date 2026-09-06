defmodule Bedrock.ControlPlane.Director.Recovery.PersistencePhaseTest do
  use ExUnit.Case, async: true

  import Bedrock.Test.ControlPlane.RecoveryTestSupport

  alias Bedrock.ControlPlane.Director.Recovery.PersistencePhase
  alias Bedrock.ControlPlane.Director.Recovery.SystemShardBootstrapPhase
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.Internal.TransactionBuilder.Tx

  # The exact bytes PersistencePhase committed for the fixture below,
  # captured before the Tx was threaded through the phases (bedrock-qb0g).
  @golden_system_transaction Base.decode16!(
                               "4252445401000002030000F0781393A0000000040020FF2F73797374656D2F6D6174657269616C69" <>
                                 "7A6572732F302F776B725F7379730021FF2F73797374656D2F6D6174657269616C697A6572732F" <>
                                 "302F776B725F737973000021FF2F73797374656D2F6D6174657269616C697A6572732F312F776B" <>
                                 "725F757365720022FF2F73797374656D2F6D6174657269616C697A6572732F312F776B725F7573" <>
                                 "6572000015FF2F73797374656D2F73686172645F6B6579732FFF0016FF2F73797374656D2F7368" <>
                                 "6172645F6B6579732FFF000016FF2F73797374656D2F73686172645F6B6579732FFFFF0017FF2F" <>
                                 "73797374656D2F73686172645F6B6579732FFFFF00010000A62928E57400C614FF2F7379737465" <>
                                 "6D2F73686172645F6B6579732FFF05150101000000C615FF2F73797374656D2F73686172645F6B" <>
                                 "6579732FFFFF051401FF000000CC1FFF2F73797374656D2F6D6174657269616C697A6572732F30" <>
                                 "2F776B725F7379730F01676F6C64656E40666978747572650000CC20FF2F73797374656D2F6D61" <>
                                 "74657269616C697A6572732F312F776B725F757365720F01676F6C64656E406669787475726500"
                             )

  # Shared test data setup: the TSL is wiring only (no membership map);
  # shard topology and service records ride the recovery attempt (and
  # from there the keyspace).
  defp mock_transaction_system_layout do
    %{
      epoch: 1,
      sequencer: self(),
      proxies: [self()],
      resolvers: [{<<0>>, self()}],
      logs: %{"log_1" => [1, 2]}
    }
  end

  defp node_string, do: Atom.to_string(node())

  defp base_recovery_attempt do
    mat_sys = spawn(fn -> Process.sleep(:infinity) end)
    mat_user = spawn(fn -> Process.sleep(:infinity) end)

    recovery_attempt()
    |> with_sequencer(self())
    |> with_proxies([self()])
    |> with_resolvers([{<<0>>, self()}])
    |> with_logs(%{"log_1" => [1, 2]})
    |> with_transaction_services(%{
      "log_1" => %{status: {:up, self()}, kind: :log, last_seen: {:log_1, :node1}},
      "wkr_sys" => %{status: {:up, mat_sys}, kind: :materializer, last_seen: {:wkr_sys_name, node()}},
      "wkr_user" => %{status: {:up, mat_user}, kind: :materializer, last_seen: {:wkr_user_name, node()}}
    })
    |> Map.put(:shard_layout, %{
      <<0xFF>> => {1, <<>>},
      <<0xFF, 0xFF>> => {0, <<0xFF>>}
    })
    |> Map.put(:seated_materializer_members, %{0 => %{"wkr_sys" => node_string()}, 1 => %{"wkr_user" => node_string()}})
    |> Map.put(:prior_materializer_members, %{})
    |> Map.put(:pending_tx, accumulated_tx(node_string()))
    |> Map.put(:transaction_system_layout, mock_transaction_system_layout())
  end

  # What the phases ahead of persistence put in the attempt's Tx: the
  # system shard bootstrap phase's fresh-cluster contribution — the layout
  # it seeded, and the membership it decided against an empty prior.
  defp accumulated_tx(node_string) do
    Tx.new()
    |> SystemShardBootstrapPhase.put_shard_keys(%{<<0xFF>> => {1, <<>>}, <<0xFF, 0xFF>> => {0, <<0xFF>>}})
    |> SystemShardBootstrapPhase.put_materializer_members(
      %{0 => %{"wkr_sys" => node_string}, 1 => %{"wkr_user" => node_string}},
      %{}
    )
  end

  describe "execute/2" do
    test "succeeds with existing transaction system layout and transitions to completed" do
      recovery_attempt = base_recovery_attempt()
      expected_layout = recovery_attempt.transaction_system_layout

      context = Map.put(recovery_context(), :commit_transaction_fn, fn _, _, _ -> {:ok, 1, 0} end)

      # Pattern match both result and next phase in single assertion
      assert {%{transaction_system_layout: ^expected_layout}, :completed} =
               PersistencePhase.execute(recovery_attempt, context)
    end

    test "fails when system transaction fails" do
      recovery_attempt = base_recovery_attempt()
      context = Map.put(recovery_context(), :commit_transaction_fn, fn _, _, _ -> {:error, :timeout} end)

      # Pattern match tuple destructuring with expected stall reason
      assert {_, {:stalled, {:recovery_system_failed, :timeout}}} =
               PersistencePhase.execute(recovery_attempt, context)
    end

    test "commits exactly the Tx the phases accumulated, and contributes nothing of its own" do
      recovery_attempt = base_recovery_attempt()
      test_pid = self()

      commit_fn = fn _proxy, _epoch, encoded_transaction ->
        send(test_pid, {:committed, encoded_transaction})
        {:ok, 1, 0}
      end

      context = Map.put(recovery_context(), :commit_transaction_fn, commit_fn)
      assert {_, :completed} = PersistencePhase.execute(recovery_attempt, context)
      assert_received {:committed, encoded_transaction}

      assert encoded_transaction == Tx.commit(recovery_attempt.pending_tx, nil)
    end

    test "an attempt whose phases wrote nothing commits an empty transaction" do
      # Nothing is synthesized here. A phase with no keyspace writes to make
      # leaves the Tx alone, and the commit then carries no mutations --
      # this phase has no state of its own left to rebuild them from.
      recovery_attempt = Map.put(base_recovery_attempt(), :pending_tx, Tx.new())

      assert recovery_attempt |> captured_system_mutations() |> Enum.to_list() == []
    end

    test "the committed bytes are identical to the pre-refactor build on a fixed fixture" do
      # Threading the Tx through the phases must not change one byte of what
      # recovery commits. @golden_system_transaction was captured from
      # PersistencePhase as it stood before this refactor (bedrock-qb0g),
      # running this same fixture. Regenerate it only when the transaction
      # ENCODING changes -- never to accommodate a change in what recovery
      # writes.
      recovery_attempt = Map.put(base_recovery_attempt(), :pending_tx, accumulated_tx("golden@fixture"))
      test_pid = self()

      commit_fn = fn _proxy, _epoch, encoded_transaction ->
        send(test_pid, {:committed, encoded_transaction})
        {:ok, 1, 0}
      end

      context = Map.put(recovery_context(), :commit_transaction_fn, commit_fn)
      assert {_, :completed} = PersistencePhase.execute(recovery_attempt, context)
      assert_received {:committed, encoded_transaction}

      assert encoded_transaction == @golden_system_transaction
    end

    test "commits the system transaction in system mode by default" do
      # Without an injected commit fn, the phase must reach the proxy through
      # the system-mode commit path: user-mode commits cannot write \xFF keys.
      test_pid = self()

      stub_proxy =
        spawn_link(fn ->
          receive do
            {:"$gen_call", from, {:commit, epoch, encoded_transaction, mode}} ->
              send(test_pid, {:committed, epoch, encoded_transaction, mode})
              GenServer.reply(from, {:ok, 1, 0})
          end
        end)

      recovery_attempt =
        base_recovery_attempt()
        |> with_proxies([stub_proxy])
        |> put_in([Access.key!(:transaction_system_layout), :proxies], [stub_proxy])

      assert {_, :completed} = PersistencePhase.execute(recovery_attempt, recovery_context())
      assert_received {:committed, _epoch, encoded_transaction, :system}

      assert Enum.any?(Transaction.mutations!(encoded_transaction), fn
               {:set, <<0xFF, _::binary>>, _} -> true
               _ -> false
             end)
    end
  end

  # Capture the mutations of the system transaction PersistencePhase commits.
  defp captured_system_mutations(recovery_attempt) do
    test_pid = self()

    commit_fn = fn _proxy, _epoch, encoded_transaction ->
      send(test_pid, {:committed, encoded_transaction})
      {:ok, 1, 0}
    end

    context = Map.put(recovery_context(), :commit_transaction_fn, commit_fn)
    assert {_, :completed} = PersistencePhase.execute(recovery_attempt, context)
    assert_received {:committed, encoded_transaction}
    Transaction.mutations!(encoded_transaction)
  end

  describe "rewriting a bootstrap record that predates a field" do
    alias Bedrock.ControlPlane.Config.Parameters
    alias Bedrock.ControlPlane.Config.RecoveryAttempt
    alias Bedrock.SystemKeys.ClusterBootstrap

    test "a legacy record without system_materializers is rewritten, not crashed on" do
      # The real decoded shape, not a hand-built map: a record written
      # before the field existed comes back WITHOUT the key, and
      # %{record | key: ...} raises badkey for a key the map lacks. That
      # crashed the director in a tight retry loop on every cluster
      # created before bedrock-q67.21.12 — the ones an upgrade must
      # carry, not brick.
      legacy =
        %{
          cluster_id: "abc",
          epoch: 3,
          logs: [%{id: "log_1", otp_ref: nil, shard_tags: []}],
          coordinators: [%{node: "a@host"}],
          parameters: %{
            desired_logs: 1,
            desired_replication_factor: 1,
            desired_commit_proxies: 1,
            desired_coordinators: 1,
            desired_read_version_proxies: 1,
            ping_rate_in_hz: 10,
            retransmission_rate_in_hz: 20,
            transaction_window_in_ms: 5000,
            empty_transaction_timeout_ms: 1000
          },
          policies: %{allow_volunteer_nodes_to_join: true}
        }
        |> ClusterBootstrap.to_binary()
        |> ClusterBootstrap.read()
        |> then(fn {:ok, decoded} -> decoded end)

      refute Map.has_key?(legacy, :system_materializers)

      attempt =
        %RecoveryAttempt{cluster: nil, epoch: 4, attempt: 1}
        |> Map.put(:seated_materializer_members, %{0 => %{"mat_sys" => "a@host"}})
        |> Map.put(:prior_materializer_members, %{})

      # The config the director holds always came through the
      # coordinator's build_parameters/2, which fills every parameter
      # the record did not carry.
      config = %{
        parameters:
          Map.put(
            legacy.parameters,
            :materializer_idle_timeout_ms,
            Parameters.default_materializer_idle_timeout_ms()
          ),
        policies: legacy.policies
      }

      updated =
        PersistencePhase.build_updated_bootstrap(legacy, attempt, config, %{logs: %{"log_1" => []}})

      assert updated.epoch == 4
      assert updated.system_materializers == [%{id: "mat_sys", node: "a@host"}]
      # Untouched fields survive the merge.
      assert updated.cluster_id == "abc"
      assert updated.coordinators == [%{node: "a@host"}]
    end

    # bedrock-q67.21.8: zero means "never spin down", and a flatbuffer
    # omits a scalar equal to its schema default — so with the implicit
    # default of 0 a disabled cluster would read back as "absent" and be
    # silently re-enabled by the coordinator's fallback on the next
    # restart. The schema's non-zero default is what keeps the two
    # distinguishable.
    test "a cluster with materializer spin-down disabled survives the bootstrap round trip" do
      parameters = %{
        desired_logs: 1,
        desired_replication_factor: 1,
        desired_commit_proxies: 1,
        desired_coordinators: 1,
        desired_read_version_proxies: 1,
        ping_rate_in_hz: 10,
        retransmission_rate_in_hz: 20,
        transaction_window_in_ms: 5000,
        empty_transaction_timeout_ms: 1000,
        materializer_idle_timeout_ms: 0
      }

      assert {:ok, decoded} =
               %{cluster_id: "abc", epoch: 3, parameters: parameters}
               |> ClusterBootstrap.to_binary()
               |> ClusterBootstrap.read()

      assert decoded.parameters.materializer_idle_timeout_ms == 0

      # A record written before the field existed still reads as absent,
      # which is what lets the coordinator supply the default.
      assert {:ok, legacy} =
               %{
                 cluster_id: "abc",
                 epoch: 3,
                 parameters: Map.delete(parameters, :materializer_idle_timeout_ms)
               }
               |> ClusterBootstrap.to_binary()
               |> ClusterBootstrap.read()

      refute Map.has_key?(legacy.parameters, :materializer_idle_timeout_ms)
    end
  end
end
