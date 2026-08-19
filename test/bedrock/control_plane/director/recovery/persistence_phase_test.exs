defmodule Bedrock.ControlPlane.Director.Recovery.PersistencePhaseTest do
  use ExUnit.Case, async: true

  import Bedrock.Test.ControlPlane.RecoveryTestSupport

  alias Bedrock.ControlPlane.Director.Recovery.PersistencePhase
  alias Bedrock.DataPlane.Transaction
  alias Bedrock.SystemKeys
  alias Bedrock.SystemKeys.Values

  # Shared test data setup
  defp mock_transaction_system_layout do
    %{
      id: "test_layout_id",
      epoch: 1,
      director: self(),
      sequencer: self(),
      rate_keeper: nil,
      proxies: [self()],
      resolvers: [{<<0>>, self()}],
      logs: %{"log_1" => [1, 2]},
      services: %{
        "log_1" => %{status: {:up, self()}, kind: :log, last_seen: {:log_1, :node1}}
      },
      shard_layout: %{
        <<0xFF>> => {1, <<>>},
        <<0xFF, 0xFF>> => {0, <<0xFF>>}
      }
    }
  end

  defp base_recovery_attempt do
    layout = mock_transaction_system_layout()

    recovery_attempt()
    |> with_sequencer(self())
    |> with_proxies([self()])
    |> with_resolvers([{<<0>>, self()}])
    |> with_logs(%{"log_1" => [1, 2]})
    |> with_transaction_services(%{
      "log_1" => %{status: {:up, self()}, kind: :log, last_seen: {:log_1, :node1}}
    })
    |> Map.put(:transaction_system_layout, layout)
  end

  describe "execute/2" do
    test "succeeds with existing transaction system layout and transitions to completed" do
      expected_layout = mock_transaction_system_layout()
      recovery_attempt = base_recovery_attempt()

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

    test "every system-key value it writes decodes through Values.decode_for/2" do
      recovery_attempt = base_recovery_attempt()
      test_pid = self()

      commit_fn = fn _proxy, _epoch, encoded_transaction ->
        send(test_pid, {:committed, encoded_transaction})
        {:ok, 1, 0}
      end

      context = Map.put(recovery_context(), :commit_transaction_fn, commit_fn)
      assert {_, :completed} = PersistencePhase.execute(recovery_attempt, context)
      assert_received {:committed, encoded_transaction}

      sets =
        encoded_transaction
        |> Transaction.mutations!()
        |> Enum.map(fn {:set, key, value} -> {key, value} end)

      refute sets == []

      # Every written value must decode via the family dispatched from its key;
      # this is the writer/reader contract that masked the pre-fix shard_key bug.
      decoded =
        Map.new(sets, fn {key, value} ->
          parsed = SystemKeys.parse_key(key)
          refute parsed in [:unknown, :error], "wrote unparseable system key: #{inspect(key)}"
          assert {:ok, decoded} = Values.decode_for(parsed, value), "value for #{inspect(key)} failed to decode"
          {parsed, decoded}
        end)

      # Shard keys must decode to the exact {tag, start_key} the layout holds --
      # the shape the materializer bootstrap cross-epoch read depends on.
      assert decoded[{:shard_key, <<0xFF>>}] == {1, <<>>}
      assert decoded[{:shard_key, <<0xFF, 0xFF>>}] == {0, <<0xFF>>}

      # Services are sanitized: no live pid survives into durable storage.
      assert %{"log_1" => %{status: :unknown}} = decoded[:layout_services]
      assert decoded[{:layout_log, "log_1"}] == [1, 2]
    end
  end
end
