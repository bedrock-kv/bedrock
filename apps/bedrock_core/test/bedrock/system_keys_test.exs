defmodule Bedrock.SystemKeysTest do
  use ExUnit.Case, async: true

  alias Bedrock.SystemKeys

  # Helper function to validate key-value pairs in batch
  defp assert_key_values(key_value_pairs) do
    for {actual, expected} <- key_value_pairs do
      assert actual == expected, "Expected #{inspect(actual)} to equal #{inspect(expected)}"
    end
  end

  # Helper function to validate keys are in a collection
  defp assert_keys_present(keys, expected_keys) when is_list(keys) and is_list(expected_keys) do
    for expected_key <- expected_keys do
      assert expected_key in keys, "Expected #{inspect(expected_key)} to be in #{inspect(keys)}"
    end
  end

  describe "cluster configuration keys" do
    test "basic cluster keys return correct values" do
      assert SystemKeys.cluster_coordinators() == "\xff/system/cluster/coordinators"
      assert SystemKeys.cluster_epoch() == "\xff/system/cluster/epoch"

      assert SystemKeys.cluster_policies_volunteer_nodes() ==
               "\xff/system/cluster/policies/volunteer_nodes"
    end

    test "cluster parameter keys return correct values" do
      parameter_keys = [
        {SystemKeys.cluster_parameters_desired_logs(), "\xff/system/cluster/parameters/desired_logs"},
        {SystemKeys.cluster_parameters_desired_replication(), "\xff/system/cluster/parameters/desired_replication"},
        {SystemKeys.cluster_parameters_desired_commit_proxies(),
         "\xff/system/cluster/parameters/desired_commit_proxies"},
        {SystemKeys.cluster_parameters_desired_coordinators(), "\xff/system/cluster/parameters/desired_coordinators"},
        {SystemKeys.cluster_parameters_desired_read_version_proxies(),
         "\xff/system/cluster/parameters/desired_read_version_proxies"},
        {SystemKeys.cluster_parameters_empty_transaction_timeout_ms(),
         "\xff/system/cluster/parameters/empty_transaction_timeout_ms"},
        {SystemKeys.cluster_parameters_ping_rate_in_hz(), "\xff/system/cluster/parameters/ping_rate_in_hz"},
        {SystemKeys.cluster_parameters_retransmission_rate_in_hz(),
         "\xff/system/cluster/parameters/retransmission_rate_in_hz"},
        {SystemKeys.cluster_parameters_transaction_window_in_ms(),
         "\xff/system/cluster/parameters/transaction_window_in_ms"}
      ]

      assert_key_values(parameter_keys)
    end
  end

  describe "transaction layout keys" do
    test "layout keys return correct values" do
      layout_keys = [
        {SystemKeys.layout_sequencer(), "\xff/system/layout/sequencer"},
        {SystemKeys.layout_proxies(), "\xff/system/layout/proxies"},
        {SystemKeys.layout_resolvers(), "\xff/system/layout/resolvers"},
        {SystemKeys.layout_services(), "\xff/system/layout/services"},
        {SystemKeys.layout_director(), "\xff/system/layout/director"},
        {SystemKeys.layout_rate_keeper(), "\xff/system/layout/rate_keeper"},
        {SystemKeys.layout_id(), "\xff/system/layout/id"}
      ]

      assert_key_values(layout_keys)
    end

    test "dynamic layout keys with IDs return correct values" do
      # Test layout_log/1 with different IDs
      assert SystemKeys.layout_log("log_123") == "\xff/system/layout/logs/log_123"
      assert SystemKeys.layout_log("another_log") == "\xff/system/layout/logs/another_log"

      # Test layout_storage_team/1 with different IDs
      assert SystemKeys.layout_storage_team("team_456") == "\xff/system/layout/storage/team_456"
      assert SystemKeys.layout_storage_team("another_team") == "\xff/system/layout/storage/another_team"
    end

    test "prefix keys for range queries" do
      assert SystemKeys.layout_logs_prefix() == "\xff/system/layout/logs/"
      assert SystemKeys.layout_storage_teams_prefix() == "\xff/system/layout/storage/"
    end
  end

  describe "recovery state keys" do
    test "recovery keys return correct values" do
      recovery_keys = [
        {SystemKeys.recovery_attempt(), "\xff/system/recovery/attempt"},
        {SystemKeys.recovery_state(), "\xff/system/recovery/state"},
        {SystemKeys.recovery_last_completed(), "\xff/system/recovery/last_completed"}
      ]

      assert_key_values(recovery_keys)
    end
  end

  describe "legacy compatibility keys" do
    test "legacy keys return correct values" do
      legacy_keys = [
        {SystemKeys.config_monolithic(), "\xff/system/config"},
        {SystemKeys.layout_monolithic(), "\xff/system/transaction_system_layout"},
        {SystemKeys.epoch_legacy(), "\xff/system/epoch"},
        {SystemKeys.last_recovery_legacy(), "\xff/system/last_recovery"}
      ]

      assert_key_values(legacy_keys)
    end
  end

  describe "utility functions" do
    test "all_cluster_keys/0 returns all cluster configuration keys" do
      keys = SystemKeys.all_cluster_keys()

      expected_keys = [
        SystemKeys.cluster_coordinators(),
        SystemKeys.cluster_epoch(),
        SystemKeys.cluster_policies_volunteer_nodes(),
        SystemKeys.cluster_parameters_desired_logs(),
        SystemKeys.cluster_parameters_desired_replication(),
        SystemKeys.cluster_parameters_desired_commit_proxies(),
        SystemKeys.cluster_parameters_desired_coordinators(),
        SystemKeys.cluster_parameters_desired_read_version_proxies(),
        SystemKeys.cluster_parameters_empty_transaction_timeout_ms(),
        SystemKeys.cluster_parameters_ping_rate_in_hz(),
        SystemKeys.cluster_parameters_retransmission_rate_in_hz(),
        SystemKeys.cluster_parameters_transaction_window_in_ms()
      ]

      # Verify all expected keys are present and count is correct
      assert length(keys) == 12
      assert_keys_present(keys, expected_keys)
    end

    test "all_layout_keys/0 returns all transaction layout keys" do
      keys = SystemKeys.all_layout_keys()

      expected_keys = [
        SystemKeys.layout_sequencer(),
        SystemKeys.layout_proxies(),
        SystemKeys.layout_resolvers(),
        SystemKeys.layout_services(),
        SystemKeys.layout_director(),
        SystemKeys.layout_rate_keeper(),
        SystemKeys.layout_id()
      ]

      # Verify all expected keys are present and count is correct
      assert length(keys) == 7
      assert_keys_present(keys, expected_keys)
    end

    test "all_legacy_keys/0 returns all legacy compatibility keys" do
      keys = SystemKeys.all_legacy_keys()

      expected_keys = [
        SystemKeys.config_monolithic(),
        SystemKeys.layout_monolithic(),
        SystemKeys.epoch_legacy(),
        SystemKeys.last_recovery_legacy()
      ]

      # Verify all expected keys are present and count is correct
      assert length(keys) == 4
      assert_keys_present(keys, expected_keys)
    end

    test "system_key?/1 correctly identifies system keys" do
      # Test valid system keys
      valid_system_keys = [
        SystemKeys.cluster_coordinators(),
        SystemKeys.layout_sequencer(),
        SystemKeys.config_monolithic(),
        "\xff/system/custom/key"
      ]

      for key <- valid_system_keys do
        assert SystemKeys.system_key?(key), "Expected #{inspect(key)} to be recognized as system key"
      end

      # Test invalid system keys
      invalid_keys = [
        "user/data/key",
        "some_other_key",
        "\xff/user/key",
        "",
        nil,
        123,
        :atom
      ]

      for key <- invalid_keys do
        refute SystemKeys.system_key?(key), "Expected #{inspect(key)} to NOT be recognized as system key"
      end
    end

    test "system_prefix/0 returns correct prefix" do
      assert SystemKeys.system_prefix() == "\xff/system"
    end
  end

  describe "key consistency" do
    test "all keys start with system prefix" do
      prefix = SystemKeys.system_prefix()

      # Collect all static keys from different categories
      all_static_keys =
        SystemKeys.all_cluster_keys() ++
          SystemKeys.all_layout_keys() ++
          SystemKeys.all_legacy_keys()

      # Test all static keys have correct prefix
      for key <- all_static_keys do
        assert String.starts_with?(key, prefix),
               "Key #{key} does not start with #{prefix}"
      end

      # Test dynamic keys have correct prefix
      dynamic_keys = [
        SystemKeys.layout_log("test"),
        SystemKeys.layout_storage_team("test")
      ]

      for key <- dynamic_keys do
        assert String.starts_with?(key, prefix),
               "Dynamic key #{key} does not start with #{prefix}"
      end
    end

    test "no duplicate keys across categories" do
      all_keys =
        SystemKeys.all_cluster_keys() ++
          SystemKeys.all_layout_keys() ++
          SystemKeys.all_legacy_keys()

      unique_keys = Enum.uniq(all_keys)
      duplicates = all_keys -- unique_keys

      assert length(all_keys) == length(unique_keys),
             "Found duplicate keys: #{inspect(duplicates)}"
    end

    test "dynamic keys work with various ID formats" do
      test_ids = ["123", "log_abc", "team-456", "storage_team_789", ""]

      log_prefix = SystemKeys.layout_logs_prefix()
      storage_prefix = SystemKeys.layout_storage_teams_prefix()

      for id <- test_ids do
        log_key = SystemKeys.layout_log(id)
        storage_key = SystemKeys.layout_storage_team(id)

        # Verify both prefix and suffix in a single assertion pattern
        assert String.starts_with?(log_key, log_prefix) and String.ends_with?(log_key, id),
               "Log key #{log_key} should start with #{log_prefix} and end with #{id}"

        assert String.starts_with?(storage_key, storage_prefix) and String.ends_with?(storage_key, id),
               "Storage key #{storage_key} should start with #{storage_prefix} and end with #{id}"
      end
    end
  end
end
