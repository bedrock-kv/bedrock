defmodule Bedrock.ControlPlane.Config.PersistenceTest do
  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.Config.Persistence

  # Mock cluster module for testing
  defmodule TestCluster do
    @moduledoc false
    def otp_name(component) when is_atom(component) do
      :"test_cluster_#{component}"
    end

    def otp_name(component) when is_binary(component) do
      :"test_cluster_#{component}"
    end
  end

  # Helper function to spawn and register a named process
  defp spawn_registered_process(name) do
    test_process = self()

    pid =
      spawn(fn ->
        Process.register(self(), name)
        send(test_process, {:registered, self()})

        receive do
          :stop -> :ok
        end
      end)

    assert_receive {:registered, ^pid}

    # Automatic cleanup on test exit
    on_exit(fn -> send(pid, :stop) end)

    pid
  end

  describe "encode_for_storage/2" do
    test "removes ephemeral state" do
      config = %{
        coordinators: [:node1, :node2],
        epoch: 5,
        parameters: %{desired_logs: 3},
        policies: %{},
        # Should be removed
        recovery_attempt: %{state: :running},
        transaction_system_layout: %{
          director: nil,
          sequencer: nil,
          rate_keeper: nil,
          proxies: [],
          services: %{}
        }
      }

      encoded = Persistence.encode_for_storage(config, TestCluster)

      refute Map.has_key?(encoded, :recovery_attempt)
      assert encoded.coordinators == [:node1, :node2]
      assert encoded.epoch == 5
    end

    test "encodes single PID references to {otp_name, node} tuples" do
      director_pid = spawn(fn -> :ok end)
      sequencer_pid = spawn(fn -> :ok end)

      config = %{
        coordinators: [],
        epoch: 1,
        parameters: %{},
        policies: %{},
        transaction_system_layout: %{
          director: director_pid,
          sequencer: sequencer_pid,
          rate_keeper: nil,
          proxies: [],
          services: %{}
        }
      }

      encoded = Persistence.encode_for_storage(config, TestCluster)

      layout = encoded.transaction_system_layout
      assert layout.director == {:test_cluster_director, node()}
      assert layout.sequencer == {:test_cluster_sequencer, node()}
      assert layout.rate_keeper == nil
    end

    test "encodes proxy list PIDs to {otp_name, node} tuples" do
      proxy1 = spawn(fn -> :ok end)
      proxy2 = spawn(fn -> :ok end)

      config = %{
        coordinators: [],
        epoch: 1,
        parameters: %{},
        policies: %{},
        transaction_system_layout: %{
          director: nil,
          sequencer: nil,
          rate_keeper: nil,
          proxies: [proxy1, proxy2],
          services: %{}
        }
      }

      encoded = Persistence.encode_for_storage(config, TestCluster)

      layout = encoded.transaction_system_layout

      assert layout.proxies == [
               {:test_cluster_commit_proxy_1, node()},
               {:test_cluster_commit_proxy_2, node()}
             ]
    end

    test "encodes service descriptor PIDs to {otp_name, node} tuples" do
      worker_pid = spawn(fn -> :ok end)

      config = %{
        coordinators: [],
        epoch: 1,
        parameters: %{},
        policies: %{},
        transaction_system_layout: %{
          director: nil,
          sequencer: nil,
          rate_keeper: nil,
          proxies: [],
          services: %{
            "log-1" => %{
              kind: :log,
              last_seen: {:some_otp_name, :some_node},
              status: {:up, worker_pid}
            },
            "storage-1" => %{
              kind: :storage,
              status: :down
            }
          }
        }
      }

      encoded = Persistence.encode_for_storage(config, TestCluster)

      services = encoded.transaction_system_layout.services

      # PID should be encoded
      assert services["log-1"].status == {:up, {:"test_cluster_log-1", node()}}

      # Non-PID status should be unchanged
      assert services["storage-1"].status == :down
    end
  end

  describe "decode_from_storage/2" do
    test "decodes {otp_name, node} tuples back to PIDs for running processes" do
      fake_sequencer = spawn_registered_process(:test_cluster_sequencer)

      encoded_config = %{
        coordinators: [],
        epoch: 1,
        parameters: %{},
        policies: %{},
        transaction_system_layout: %{
          director: nil,
          sequencer: {:test_cluster_sequencer, node()},
          rate_keeper: nil,
          proxies: [],
          services: %{}
        }
      }

      decoded = Persistence.decode_from_storage(encoded_config, TestCluster)

      layout = decoded.transaction_system_layout
      assert layout.sequencer == fake_sequencer
    end

    test "handles non-existent processes gracefully" do
      encoded_config = %{
        coordinators: [],
        epoch: 1,
        parameters: %{},
        policies: %{},
        transaction_system_layout: %{
          director: {:non_existent_process, node()},
          sequencer: nil,
          rate_keeper: nil,
          proxies: [],
          services: %{}
        }
      }

      decoded = Persistence.decode_from_storage(encoded_config, TestCluster)

      layout = decoded.transaction_system_layout
      assert layout.director == nil
    end

    test "decodes proxy list references" do
      fake_proxy_1 = spawn_registered_process(:test_cluster_commit_proxy_1)
      fake_proxy_2 = spawn_registered_process(:test_cluster_commit_proxy_2)

      encoded_config = %{
        coordinators: [],
        epoch: 1,
        parameters: %{},
        policies: %{},
        transaction_system_layout: %{
          director: nil,
          sequencer: nil,
          rate_keeper: nil,
          proxies: [
            {:test_cluster_commit_proxy_1, node()},
            {:test_cluster_commit_proxy_2, node()}
          ],
          services: %{}
        }
      }

      decoded = Persistence.decode_from_storage(encoded_config, TestCluster)

      layout = decoded.transaction_system_layout
      assert layout.proxies == [fake_proxy_1, fake_proxy_2]
    end

    test "decodes service descriptor references" do
      fake_log_worker = spawn_registered_process(:"test_cluster_log-1")

      encoded_config = %{
        coordinators: [],
        epoch: 1,
        parameters: %{},
        policies: %{},
        transaction_system_layout: %{
          director: nil,
          sequencer: nil,
          rate_keeper: nil,
          proxies: [],
          services: %{
            "log-1" => %{
              kind: :log,
              last_seen: {:some_otp_name, :some_node},
              status: {:up, {:"test_cluster_log-1", node()}}
            }
          }
        }
      }

      decoded = Persistence.decode_from_storage(encoded_config, TestCluster)

      services = decoded.transaction_system_layout.services
      assert services["log-1"].status == {:up, fake_log_worker}
    end
  end

  describe "round-trip encoding/decoding" do
    test "preserves non-PID data through encode/decode cycle" do
      original_config = %{
        coordinators: [:node1, :node2, :node3],
        epoch: 42,
        parameters: %{
          desired_logs: 5,
          desired_replication_factor: 3,
          desired_commit_proxies: 2
        },
        policies: %{
          allow_volunteer_nodes: true
        },
        transaction_system_layout: %{
          director: nil,
          sequencer: nil,
          rate_keeper: nil,
          proxies: [],
          services: %{
            "storage-1" => %{
              kind: :storage,
              last_seen: {:storage_worker_1, :node1},
              status: :down
            }
          }
        }
      }

      encoded = Persistence.encode_for_storage(original_config, TestCluster)
      decoded = Persistence.decode_from_storage(encoded, TestCluster)

      # Non-PID data should be preserved exactly
      assert decoded.coordinators == original_config.coordinators
      assert decoded.epoch == original_config.epoch
      assert decoded.parameters == original_config.parameters
      assert decoded.policies == original_config.policies

      # Non-PID service status should be preserved
      services = decoded.transaction_system_layout.services
      assert services["storage-1"].status == :down
      assert services["storage-1"].kind == :storage
    end
  end
end
