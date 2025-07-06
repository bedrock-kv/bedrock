defmodule Bedrock.SystemKeysTest do
  use ExUnit.Case, async: true

  alias Bedrock.SystemKeys

  describe "cluster configuration keys" do
    test "cluster_coordinators/0 returns correct key" do
      assert SystemKeys.cluster_coordinators() == "\xff/system/cluster/coordinators"
    end

    test "cluster_epoch/0 returns correct key" do
      assert SystemKeys.cluster_epoch() == "\xff/system/cluster/epoch"
    end

    test "cluster_policies_volunteer_nodes/0 returns correct key" do
      assert SystemKeys.cluster_policies_volunteer_nodes() ==
               "\xff/system/cluster/policies/volunteer_nodes"
    end

    test "cluster parameter keys return correct values" do
      assert SystemKeys.cluster_parameters_desired_logs() ==
               "\xff/system/cluster/parameters/desired_logs"

      assert SystemKeys.cluster_parameters_desired_replication() ==
               "\xff/system/cluster/parameters/desired_replication"

      assert SystemKeys.cluster_parameters_desired_commit_proxies() ==
               "\xff/system/cluster/parameters/desired_commit_proxies"

      assert SystemKeys.cluster_parameters_desired_coordinators() ==
               "\xff/system/cluster/parameters/desired_coordinators"

      assert SystemKeys.cluster_parameters_desired_read_version_proxies() ==
               "\xff/system/cluster/parameters/desired_read_version_proxies"

      assert SystemKeys.cluster_parameters_ping_rate_in_hz() ==
               "\xff/system/cluster/parameters/ping_rate_in_hz"

      assert SystemKeys.cluster_parameters_retransmission_rate_in_hz() ==
               "\xff/system/cluster/parameters/retransmission_rate_in_hz"

      assert SystemKeys.cluster_parameters_transaction_window_in_ms() ==
               "\xff/system/cluster/parameters/transaction_window_in_ms"
    end
  end

  describe "transaction layout keys" do
    test "layout keys return correct values" do
      assert SystemKeys.layout_sequencer() == "\xff/system/layout/sequencer"
      assert SystemKeys.layout_proxies() == "\xff/system/layout/proxies"
      assert SystemKeys.layout_resolvers() == "\xff/system/layout/resolvers"
      assert SystemKeys.layout_services() == "\xff/system/layout/services"
      assert SystemKeys.layout_director() == "\xff/system/layout/director"
      assert SystemKeys.layout_rate_keeper() == "\xff/system/layout/rate_keeper"
      assert SystemKeys.layout_id() == "\xff/system/layout/id"
    end

    test "layout_log/1 returns correct key with ID" do
      assert SystemKeys.layout_log("log_123") == "\xff/system/layout/logs/log_123"
      assert SystemKeys.layout_log("another_log") == "\xff/system/layout/logs/another_log"
    end

    test "layout_storage_team/1 returns correct key with ID" do
      assert SystemKeys.layout_storage_team("team_456") ==
               "\xff/system/layout/storage/team_456"

      assert SystemKeys.layout_storage_team("another_team") ==
               "\xff/system/layout/storage/another_team"
    end

    test "prefix keys for range queries" do
      assert SystemKeys.layout_logs_prefix() == "\xff/system/layout/logs/"
      assert SystemKeys.layout_storage_teams_prefix() == "\xff/system/layout/storage/"
    end
  end

  describe "recovery state keys" do
    test "recovery keys return correct values" do
      assert SystemKeys.recovery_attempt() == "\xff/system/recovery/attempt"
      assert SystemKeys.recovery_state() == "\xff/system/recovery/state"

      assert SystemKeys.recovery_last_completed() ==
               "\xff/system/recovery/last_completed"
    end
  end

  describe "legacy compatibility keys" do
    test "legacy keys return correct values" do
      assert SystemKeys.config_monolithic() == "\xff/system/config"
      assert SystemKeys.layout_monolithic() == "\xff/system/transaction_system_layout"
      assert SystemKeys.epoch_legacy() == "\xff/system/epoch"
      assert SystemKeys.last_recovery_legacy() == "\xff/system/last_recovery"
    end
  end

  describe "utility functions" do
    test "all_cluster_keys/0 returns all cluster configuration keys" do
      keys = SystemKeys.all_cluster_keys()

      assert Enum.member?(keys, SystemKeys.cluster_coordinators())
      assert Enum.member?(keys, SystemKeys.cluster_epoch())
      assert Enum.member?(keys, SystemKeys.cluster_policies_volunteer_nodes())
      assert Enum.member?(keys, SystemKeys.cluster_parameters_desired_logs())
      assert Enum.member?(keys, SystemKeys.cluster_parameters_desired_replication())
      assert Enum.member?(keys, SystemKeys.cluster_parameters_desired_commit_proxies())
      assert Enum.member?(keys, SystemKeys.cluster_parameters_desired_coordinators())

      assert Enum.member?(
               keys,
               SystemKeys.cluster_parameters_desired_read_version_proxies()
             )

      assert Enum.member?(keys, SystemKeys.cluster_parameters_ping_rate_in_hz())
      assert Enum.member?(keys, SystemKeys.cluster_parameters_retransmission_rate_in_hz())
      assert Enum.member?(keys, SystemKeys.cluster_parameters_transaction_window_in_ms())

      # Should have exactly 11 keys
      assert length(keys) == 11
    end

    test "all_layout_keys/0 returns all transaction layout keys" do
      keys = SystemKeys.all_layout_keys()

      assert Enum.member?(keys, SystemKeys.layout_sequencer())
      assert Enum.member?(keys, SystemKeys.layout_proxies())
      assert Enum.member?(keys, SystemKeys.layout_resolvers())
      assert Enum.member?(keys, SystemKeys.layout_services())
      assert Enum.member?(keys, SystemKeys.layout_director())
      assert Enum.member?(keys, SystemKeys.layout_rate_keeper())
      assert Enum.member?(keys, SystemKeys.layout_id())

      # Should have exactly 7 keys
      assert length(keys) == 7
    end

    test "all_legacy_keys/0 returns all legacy compatibility keys" do
      keys = SystemKeys.all_legacy_keys()

      assert Enum.member?(keys, SystemKeys.config_monolithic())
      assert Enum.member?(keys, SystemKeys.layout_monolithic())
      assert Enum.member?(keys, SystemKeys.epoch_legacy())
      assert Enum.member?(keys, SystemKeys.last_recovery_legacy())

      # Should have exactly 4 keys
      assert length(keys) == 4
    end

    test "system_key?/1 correctly identifies system keys" do
      # System keys should return true
      assert SystemKeys.system_key?(SystemKeys.cluster_coordinators())
      assert SystemKeys.system_key?(SystemKeys.layout_sequencer())
      assert SystemKeys.system_key?(SystemKeys.config_monolithic())
      assert SystemKeys.system_key?("\xff/system/custom/key")

      # Non-system keys should return false
      refute SystemKeys.system_key?("user/data/key")
      refute SystemKeys.system_key?("some_other_key")
      refute SystemKeys.system_key?("\xff/user/key")
      refute SystemKeys.system_key?("")

      # Non-binary values should return false
      refute SystemKeys.system_key?(nil)
      refute SystemKeys.system_key?(123)
      refute SystemKeys.system_key?(:atom)
    end

    test "system_prefix/0 returns correct prefix" do
      assert SystemKeys.system_prefix() == "\xff/system"
    end
  end

  describe "key consistency" do
    test "all keys start with system prefix" do
      prefix = SystemKeys.system_prefix()

      # Test all cluster keys
      for key <- SystemKeys.all_cluster_keys() do
        assert String.starts_with?(key, prefix),
               "Cluster key #{key} does not start with #{prefix}"
      end

      # Test all layout keys
      for key <- SystemKeys.all_layout_keys() do
        assert String.starts_with?(key, prefix),
               "Layout key #{key} does not start with #{prefix}"
      end

      # Test all legacy keys
      for key <- SystemKeys.all_legacy_keys() do
        assert String.starts_with?(key, prefix),
               "Legacy key #{key} does not start with #{prefix}"
      end

      # Test dynamic keys
      assert String.starts_with?(SystemKeys.layout_log("test"), prefix)
      assert String.starts_with?(SystemKeys.layout_storage_team("test"), prefix)
    end

    test "no duplicate keys across categories" do
      all_keys =
        SystemKeys.all_cluster_keys() ++
          SystemKeys.all_layout_keys() ++
          SystemKeys.all_legacy_keys()

      unique_keys = Enum.uniq(all_keys)

      assert length(all_keys) == length(unique_keys),
             "Found duplicate keys: #{inspect(all_keys -- unique_keys)}"
    end

    test "dynamic keys work with various ID formats" do
      # Test with different ID formats
      test_ids = ["123", "log_abc", "team-456", "storage_team_789", ""]

      for id <- test_ids do
        log_key = SystemKeys.layout_log(id)
        storage_key = SystemKeys.layout_storage_team(id)

        assert String.starts_with?(log_key, SystemKeys.layout_logs_prefix())
        assert String.starts_with?(storage_key, SystemKeys.layout_storage_teams_prefix())
        assert String.ends_with?(log_key, id)
        assert String.ends_with?(storage_key, id)
      end
    end
  end
end
