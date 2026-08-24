defmodule Bedrock.ControlPlane.Config.CoreStateTest do
  @moduledoc """
  The durable record recovery recovers FROM — FDB's `DBCoreState`, held
  at recovery as `cstate.prevDBState`. Distinct from the
  `TransactionSystemLayout` broadcast (FDB's `ServerDBInfo`), which is
  transient wiring rebuilt every epoch and never persisted.
  """
  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.Config.CoreState

  describe "from_bootstrap/1 - the durable record, projected" do
    test "carries each log's id and the tags it serves" do
      bootstrap = %{
        logs: [
          %{id: "log_1", shard_tags: [0, 1], otp_ref: %{otp_name: "log_1_name", node: "n1@host"}},
          %{id: "log_2", shard_tags: [2]}
        ]
      }

      assert CoreState.from_bootstrap(bootstrap) == %{logs: %{"log_1" => [0, 1], "log_2" => [2]}}
    end

    test "a log with no tags carries an empty list, not nil" do
      # Downstream does Map.keys/1 and MapSet.new/1 over these; a nil
      # would crash a recovery rather than describe a tagless log.
      assert CoreState.from_bootstrap(%{logs: [%{id: "log_1"}]}) == %{logs: %{"log_1" => []}}
    end

    test "a bootstrap with no logs at all is an empty record, never nil" do
      assert CoreState.from_bootstrap(%{logs: []}) == %{logs: %{}}
      assert CoreState.from_bootstrap(%{}) == %{logs: %{}}
      assert CoreState.from_bootstrap(%{logs: nil}) == %{logs: %{}}
    end
  end

  describe "from_layout/1 - what survives the epoch that just ended" do
    test "keeps the log set and drops everything transient" do
      # A completed recovery's layout becomes the NEXT recovery's prior
      # state. Only the durable half may cross that boundary: pids die
      # with the epoch, so carrying them into a record whose whole job
      # is to outlive the epoch is a category error.
      layout = %{
        epoch: 7,
        sequencer: self(),
        proxies: [self()],
        resolvers: [%{start_key: "", resolver: self()}],
        logs: %{"log_1" => [0, 1]}
      }

      core_state = CoreState.from_layout(layout)

      assert core_state == %{logs: %{"log_1" => [0, 1]}}
      refute core_state |> Map.values() |> Enum.any?(&is_pid/1)
    end

    test "a layout naming no logs projects to a fresh record" do
      assert %{logs: %{}} = CoreState.from_layout(%{logs: %{}})
      assert CoreState.fresh?(CoreState.from_layout(%{logs: %{}}))
    end
  end

  describe "fresh?/1 - FDB's neverCreated" do
    test "no prior record at all is a fresh cluster" do
      assert CoreState.fresh?(nil)
    end

    test "a prior record naming no logs is a fresh cluster" do
      # FDB: `!self->cstate.prevDBState.tLogs.size()` sets neverCreated
      # (ClusterRecovery.actor.cpp:981). A cluster that has never
      # committed a recovery has a record, but it names nothing.
      assert CoreState.fresh?(%{logs: %{}})
    end

    test "a prior record naming any log is NOT fresh - its data must be recovered" do
      refute CoreState.fresh?(%{logs: %{"log_1" => [0]}})
    end
  end

  describe "log_ids/1 - the services recovery must lock" do
    test "names exactly the logs the prior epoch ran" do
      assert CoreState.log_ids(%{logs: %{"log_1" => [0], "log_2" => [1]}}) == MapSet.new(["log_1", "log_2"])
    end

    test "an absent record names nothing - a fresh cluster locks no prior logs" do
      assert CoreState.log_ids(nil) == MapSet.new()
    end
  end
end
