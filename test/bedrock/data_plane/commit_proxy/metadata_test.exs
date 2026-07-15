defmodule Bedrock.DataPlane.CommitProxy.MetadataTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.CommitProxy.Metadata
  alias Bedrock.DataPlane.Version
  alias Bedrock.SystemKeys

  defp v(n), do: Version.from_integer(n)

  defp t2b(term), do: :erlang.term_to_binary(term)

  defp apply!(metadata, updates) do
    {metadata, _stats} = Metadata.apply_updates(metadata, updates)
    metadata
  end

  describe "apply_updates/2 with sets" do
    test "parses every key family PersistencePhase writes into its structured slot" do
      mutations = [
        {:set, SystemKeys.shard_key("m"), t2b(7)},
        {:set, SystemKeys.shard(3), "raw-shard-metadata"},
        {:set, SystemKeys.materializer_key("m"), "raw-materializers"},
        {:set, SystemKeys.layout_log("log-1"), t2b([0, 1])},
        {:set, SystemKeys.layout_services(), t2b(%{"log-1" => %{kind: :log}})},
        {:set, SystemKeys.layout_id(), t2b("layout-abc")},
        {:set, SystemKeys.cluster_coordinators(), t2b([:a@host])},
        {:set, SystemKeys.cluster_epoch(), t2b(4)},
        {:set, SystemKeys.cluster_policies_volunteer_nodes(), t2b(true)},
        {:set, SystemKeys.cluster_parameters_desired_logs(), t2b(2)},
        {:set, SystemKeys.cluster_parameters_desired_replication(), t2b(3)},
        {:set, SystemKeys.cluster_parameters_desired_commit_proxies(), t2b(1)},
        {:set, SystemKeys.cluster_parameters_desired_coordinators(), t2b(1)},
        {:set, SystemKeys.cluster_parameters_desired_read_version_proxies(), t2b(1)},
        {:set, SystemKeys.cluster_parameters_empty_transaction_timeout_ms(), t2b(1_000)},
        {:set, SystemKeys.cluster_parameters_ping_rate_in_hz(), t2b(10)},
        {:set, SystemKeys.cluster_parameters_retransmission_rate_in_hz(), t2b(5)},
        {:set, SystemKeys.cluster_parameters_transaction_window_in_ms(), t2b(5_000)},
        {:set, SystemKeys.recovery_attempt(), t2b(1)},
        {:set, SystemKeys.recovery_state(), t2b(:completed)},
        {:set, SystemKeys.recovery_last_completed(), t2b(123)},
        {:set, SystemKeys.config_monolithic(), t2b({4, %{}})},
        {:set, SystemKeys.epoch_legacy(), t2b(4)},
        {:set, SystemKeys.last_recovery_legacy(), t2b(123)}
      ]

      {metadata, stats} = Metadata.apply_updates(Metadata.new(), [{v(1), mutations}])

      assert metadata.version == v(1)
      assert metadata.shards == %{"m" => 7}
      # shard/materializer values are kept encoded (FlatBuffer / opaque)
      assert metadata.shard_metadata == %{"3" => "raw-shard-metadata"}
      assert metadata.materializers == %{"m" => "raw-materializers"}
      assert metadata.logs == %{"log-1" => [0, 1]}
      assert metadata.services == %{"log-1" => %{kind: :log}}
      assert metadata.layout_id == "layout-abc"
      assert metadata.cluster == %{coordinators: [:a@host], epoch: 4}
      assert metadata.policies == %{volunteer_nodes: true}

      assert metadata.parameters == %{
               desired_logs: 2,
               desired_replication: 3,
               desired_commit_proxies: 1,
               desired_coordinators: 1,
               desired_read_version_proxies: 1,
               empty_transaction_timeout_ms: 1_000,
               ping_rate_in_hz: 10,
               retransmission_rate_in_hz: 5,
               transaction_window_in_ms: 5_000
             }

      assert metadata.recovery == %{attempt: 1, state: :completed, last_completed: 123}
      assert metadata.legacy == %{config_monolithic: {4, %{}}, epoch_legacy: 4, last_recovery_legacy: 123}

      assert stats.applied == length(mutations)
      assert stats.skipped_keys == []

      assert Enum.sort(stats.families) ==
               Enum.sort([
                 :shard_key,
                 :shard,
                 :materializer_key,
                 :layout_log,
                 :layout_services,
                 :layout_id,
                 :cluster,
                 :cluster_policy,
                 :cluster_parameter,
                 :recovery,
                 :legacy
               ])
    end

    test "later version wins and version advances" do
      metadata =
        Metadata.new()
        |> apply!([{v(1), [{:set, SystemKeys.shard_key("m"), t2b(7)}]}])
        |> apply!([{v(2), [{:set, SystemKeys.shard_key("m"), t2b(9)}]}])

      assert metadata.shards == %{"m" => 9}
      assert metadata.version == v(2)
    end

    test "entries at or below the applied version are skipped (idempotent redelivery)" do
      window = [
        {v(1), [{:set, SystemKeys.shard_key("m"), t2b(7)}]},
        {v(2), [{:set, SystemKeys.shard_key("m"), t2b(9)}]}
      ]

      metadata = apply!(Metadata.new(), window)

      # Resolver re-sends the whole window (e.g. to a new finalization task)
      {redelivered, stats} = Metadata.apply_updates(metadata, window)

      assert redelivered == metadata
      assert stats.applied == 0
    end

    test "within a version, later mutation wins" do
      metadata =
        apply!(Metadata.new(), [
          {v(1), [{:set, SystemKeys.shard_key("m"), t2b(7)}, {:set, SystemKeys.shard_key("m"), t2b(8)}]}
        ])

      assert metadata.shards == %{"m" => 8}
    end
  end

  describe "apply_updates/2 with clears" do
    test "clear removes the corresponding entry" do
      metadata =
        apply!(Metadata.new(), [
          {v(1),
           [
             {:set, SystemKeys.shard_key("m"), t2b(7)},
             {:set, SystemKeys.layout_log("log-1"), t2b([0])},
             {:set, SystemKeys.layout_id(), t2b("layout-abc")}
           ]},
          {v(2), [{:clear, SystemKeys.shard_key("m")}, {:clear, SystemKeys.layout_id()}]}
        ])

      assert metadata.shards == %{}
      assert metadata.layout_id == nil
      assert metadata.logs == %{"log-1" => [0]}
    end

    test "clear_range removes every known entry whose full key is in [start, end)" do
      metadata =
        apply!(Metadata.new(), [
          {v(1),
           [
             {:set, SystemKeys.shard_key("a"), t2b(1)},
             {:set, SystemKeys.shard_key("m"), t2b(2)},
             {:set, SystemKeys.shard_key("z"), t2b(3)},
             {:set, SystemKeys.layout_log("log-1"), t2b([0])}
           ]}
        ])

      {metadata, stats} =
        Metadata.apply_updates(metadata, [
          {v(2), [{:clear_range, SystemKeys.shard_key("a"), SystemKeys.shard_key("z")}]}
        ])

      # Range end is exclusive: "a" and "m" cleared, "z" survives
      assert metadata.shards == %{"z" => 3}
      assert metadata.logs == %{"log-1" => [0]}
      assert stats.applied == 1
      assert stats.families == [:shard_key]
    end
  end

  describe "apply_updates/2 forward compatibility" do
    test "unknown system keys are ignored and counted" do
      unknown_key = <<0xFF, "/system/future/feature">>
      non_system_metadata_key = <<0xFF, "not-a-system-key">>

      {metadata, stats} =
        Metadata.apply_updates(Metadata.new(), [
          {v(1),
           [
             {:set, SystemKeys.shard_key("m"), t2b(7)},
             {:set, unknown_key, "opaque"},
             {:clear, non_system_metadata_key}
           ]}
        ])

      assert metadata.shards == %{"m" => 7}
      assert stats.applied == 1
      assert stats.skipped_keys == [unknown_key, non_system_metadata_key]
    end

    test "atomic mutations are not applied to structured metadata" do
      key = SystemKeys.shard_key("m")

      {metadata, stats} = Metadata.apply_updates(Metadata.new(), [{v(1), [{:atomic, :add, key, <<1>>}]}])

      assert metadata.shards == %{}
      assert stats.applied == 0
      assert stats.skipped_keys == [key]
    end

    test "unknown cluster parameters are ignored and counted" do
      key = SystemKeys.system_prefix() <> "/cluster/parameters/future_knob"

      {metadata, stats} = Metadata.apply_updates(Metadata.new(), [{v(1), [{:set, key, t2b(1)}]}])

      assert metadata.parameters == %{}
      assert stats.skipped_keys == [key]
    end
  end
end
