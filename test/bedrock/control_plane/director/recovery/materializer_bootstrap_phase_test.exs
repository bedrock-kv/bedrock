defmodule Bedrock.ControlPlane.Director.Recovery.MaterializerBootstrapPhaseTest do
  use ExUnit.Case, async: true

  import Bedrock.Test.ControlPlane.RecoveryTestSupport
  import ExUnit.CaptureLog

  alias Bedrock.ControlPlane.Director.Recovery.CommitProxyStartupPhase
  alias Bedrock.ControlPlane.Director.Recovery.MaterializerBootstrapPhase

  describe "execute/2" do
    test "for fresh cluster, creates default shard layout" do
      recovery_attempt =
        recovery_attempt()
        |> Map.put(:metadata_materializer, nil)
        |> Map.put(:shard_layout, nil)

      # Fresh cluster context - no old logs
      context = create_test_context(old_transaction_system_layout: %{logs: %{}, storage_teams: []})

      log =
        capture_log(fn ->
          assert {updated_attempt, CommitProxyStartupPhase} =
                   MaterializerBootstrapPhase.execute(recovery_attempt, context)

          # Should have default shard layout for fresh cluster
          assert updated_attempt.shard_layout
          assert is_map(updated_attempt.shard_layout)

          # Default layout has two shards: system and user
          assert map_size(updated_attempt.shard_layout) == 2
        end)

      assert log =~ "Fresh cluster detected"
    end

    test "stalls when materializer not found in existing cluster" do
      recovery_attempt =
        recovery_attempt()
        |> Map.put(:metadata_materializer, nil)
        |> Map.put(:shard_layout, nil)

      # Existing cluster - has old logs but no materializer in available_services
      context =
        create_test_context(
          old_transaction_system_layout: %{
            logs: %{"log_1" => [0, 1]},
            storage_teams: []
          },
          available_services: %{}
        )

      assert {_attempt, {:stalled, {:materializer_unavailable, _}}} =
               MaterializerBootstrapPhase.execute(recovery_attempt, context)
    end

    test "uses existing materializer from available_services" do
      materializer_pid = spawn(fn -> Process.sleep(:infinity) end)

      recovery_attempt =
        recovery_attempt()
        |> Map.put(:metadata_materializer, nil)
        |> Map.put(:shard_layout, nil)

      # Mock context with materializer available and query function
      context =
        [
          old_transaction_system_layout: %{
            logs: %{"log_1" => [0, 1]},
            storage_teams: []
          }
        ]
        |> create_test_context()
        |> Map.put(:available_services, %{
          "metadata_materializer" => {:storage, {:test_materializer, node()}}
        })
        |> Map.put(:get_shard_layout_fn, fn _pid, _version ->
          {:ok, %{<<0xFF>> => {0, <<>>}, Bedrock.end_of_keyspace() => {1, <<0xFF>>}}}
        end)
        |> Map.put(:lock_materializer_fn, fn _service, _epoch -> {:ok, materializer_pid} end)

      assert {updated_attempt, CommitProxyStartupPhase} =
               MaterializerBootstrapPhase.execute(recovery_attempt, context)

      assert updated_attempt.metadata_materializer == materializer_pid
      assert updated_attempt.shard_layout
    end
  end

  describe "default_shard_layout/0" do
    test "returns two shards: system and user" do
      layout = MaterializerBootstrapPhase.default_shard_layout()

      assert is_map(layout)
      assert map_size(layout) == 2

      # System shard: "" to 0xFF (tag 1)
      # User shard: 0xFF to end_of_keyspace (tag 0)
      assert Map.has_key?(layout, <<0xFF>>)
      assert Map.has_key?(layout, Bedrock.end_of_keyspace())
    end
  end
end
