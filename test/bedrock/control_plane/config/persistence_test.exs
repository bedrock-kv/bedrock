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

  # Helper functions
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

  defp base_config do
    %{
      coordinators: [],
      epoch: 1,
      parameters: %{},
      policies: %{},
      transaction_system_layout: %{
        director: nil,
        sequencer: nil,
        rate_keeper: nil,
        proxies: [],
        services: %{}
      }
    }
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
      assert %{coordinators: [:node1, :node2], epoch: 5} = encoded
    end

    test "encodes single PID references to {otp_name, node} tuples" do
      director_pid = spawn(fn -> :ok end)
      sequencer_pid = spawn(fn -> :ok end)

      config =
        base_config()
        |> put_in([:transaction_system_layout, :director], director_pid)
        |> put_in([:transaction_system_layout, :sequencer], sequencer_pid)

      encoded = Persistence.encode_for_storage(config, TestCluster)

      assert %{
               transaction_system_layout: %{
                 director: {:test_cluster_director, _},
                 sequencer: {:test_cluster_sequencer, _},
                 rate_keeper: nil
               }
             } = encoded
    end

    test "encodes proxy list PIDs to {otp_name, node} tuples" do
      proxy1 = spawn(fn -> :ok end)
      proxy2 = spawn(fn -> :ok end)

      config = put_in(base_config(), [:transaction_system_layout, :proxies], [proxy1, proxy2])

      encoded = Persistence.encode_for_storage(config, TestCluster)

      assert %{
               transaction_system_layout: %{
                 proxies: [
                   {:test_cluster_commit_proxy_1, _},
                   {:test_cluster_commit_proxy_2, _}
                 ]
               }
             } = encoded
    end

    test "encodes service descriptor PIDs to {otp_name, node} tuples" do
      worker_pid = spawn(fn -> :ok end)

      config =
        put_in(base_config(), [:transaction_system_layout, :services], %{
          "log-1" => %{kind: :log, last_seen: {:some_otp_name, :some_node}, status: {:up, worker_pid}},
          "storage-1" => %{kind: :materializer, status: :down}
        })

      encoded = Persistence.encode_for_storage(config, TestCluster)

      assert %{
               transaction_system_layout: %{
                 services: %{
                   "log-1" => %{status: {:up, {:"test_cluster_log-1", _}}},
                   "storage-1" => %{status: :down}
                 }
               }
             } = encoded
    end
  end

  describe "decode_from_storage/2" do
    test "decodes {otp_name, node} tuples back to PIDs for running processes" do
      fake_sequencer = spawn_registered_process(:test_cluster_sequencer)

      encoded_config =
        put_in(base_config(), [:transaction_system_layout, :sequencer], {:test_cluster_sequencer, node()})

      decoded = Persistence.decode_from_storage(encoded_config, TestCluster)

      assert %{
               transaction_system_layout: %{
                 sequencer: ^fake_sequencer
               }
             } = decoded
    end

    test "handles non-existent processes gracefully" do
      encoded_config = put_in(base_config(), [:transaction_system_layout, :director], {:non_existent_process, node()})

      decoded = Persistence.decode_from_storage(encoded_config, TestCluster)

      assert %{
               transaction_system_layout: %{
                 director: nil
               }
             } = decoded
    end

    test "decodes proxy list references" do
      fake_proxy_1 = spawn_registered_process(:test_cluster_commit_proxy_1)
      fake_proxy_2 = spawn_registered_process(:test_cluster_commit_proxy_2)

      encoded_config =
        put_in(base_config(), [:transaction_system_layout, :proxies], [
          {:test_cluster_commit_proxy_1, node()},
          {:test_cluster_commit_proxy_2, node()}
        ])

      decoded = Persistence.decode_from_storage(encoded_config, TestCluster)

      assert %{
               transaction_system_layout: %{
                 proxies: [^fake_proxy_1, ^fake_proxy_2]
               }
             } = decoded
    end

    test "decodes service descriptor references" do
      fake_log_worker = spawn_registered_process(:"test_cluster_log-1")

      encoded_config =
        put_in(base_config(), [:transaction_system_layout, :services], %{
          "log-1" => %{
            kind: :log,
            last_seen: {:some_otp_name, :some_node},
            status: {:up, {:"test_cluster_log-1", node()}}
          }
        })

      decoded = Persistence.decode_from_storage(encoded_config, TestCluster)

      assert %{
               transaction_system_layout: %{
                 services: %{
                   "log-1" => %{status: {:up, ^fake_log_worker}}
                 }
               }
             } = decoded
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
              kind: :materializer,
              last_seen: {:storage_worker_1, :node1},
              status: :down
            }
          }
        }
      }

      encoded = Persistence.encode_for_storage(original_config, TestCluster)
      decoded = Persistence.decode_from_storage(encoded, TestCluster)

      # Non-PID data should be preserved exactly
      assert %{
               coordinators: [:node1, :node2, :node3],
               epoch: 42,
               parameters: %{
                 desired_logs: 5,
                 desired_replication_factor: 3,
                 desired_commit_proxies: 2
               },
               policies: %{allow_volunteer_nodes: true},
               transaction_system_layout: %{
                 services: %{
                   "storage-1" => %{status: :down, kind: :materializer}
                 }
               }
             } = decoded
    end
  end
end
