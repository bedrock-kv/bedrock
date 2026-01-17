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
        {SystemKeys.layout_services(), "\xff/system/layout/services"},
        {SystemKeys.layout_id(), "\xff/system/layout/id"}
      ]

      assert_key_values(layout_keys)
    end

    test "dynamic layout keys with IDs return correct values" do
      # Test layout_log/1 with different IDs
      assert SystemKeys.layout_log("log_123") == "\xff/system/layout/logs/log_123"
      assert SystemKeys.layout_log("another_log") == "\xff/system/layout/logs/another_log"

      # Test shard_key/1 with end_key (ceiling search pattern)
      assert SystemKeys.shard_key("m") == "\xff/system/shard_keys/m"
      assert SystemKeys.shard_key("\xff") == "\xff/system/shard_keys/\xff"

      # Test shard/1 with tag
      assert SystemKeys.shard(0) == "\xff/system/shards/0"
      assert SystemKeys.shard(42) == "\xff/system/shards/42"
    end

    test "prefix keys for range queries" do
      assert SystemKeys.layout_logs_prefix() == "\xff/system/layout/logs/"
      assert SystemKeys.shard_keys_prefix() == "\xff/system/shard_keys/"
      assert SystemKeys.shards_prefix() == "\xff/system/shards/"
      assert SystemKeys.materializer_keys_prefix() == "\xff/system/materializer_keys/"
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

    test "all_legacy_keys/0 returns all legacy compatibility keys" do
      keys = SystemKeys.all_legacy_keys()

      expected_keys = [
        SystemKeys.config_monolithic(),
        SystemKeys.epoch_legacy(),
        SystemKeys.last_recovery_legacy()
      ]

      # Verify all expected keys are present and count is correct
      assert length(keys) == 3
      assert_keys_present(keys, expected_keys)
    end

    test "system_key?/1 correctly identifies system keys" do
      # Test valid system keys
      valid_system_keys = [
        SystemKeys.cluster_coordinators(),
        SystemKeys.layout_services(),
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

  describe "parse_key/1" do
    test "parses layout_log keys" do
      assert SystemKeys.parse_key("\xff/system/layout/logs/log-abc123") == {:layout_log, "log-abc123"}
      assert SystemKeys.parse_key("\xff/system/layout/logs/") == {:layout_log, ""}
    end

    test "parses shard_key keys" do
      assert SystemKeys.parse_key("\xff/system/shard_keys/m") == {:shard_key, "m"}
    end

    test "parses shard keys" do
      assert SystemKeys.parse_key("\xff/system/shards/0") == {:shard, "0"}
      assert SystemKeys.parse_key("\xff/system/shards/42") == {:shard, "42"}
    end

    test "parses materializer_key keys" do
      assert SystemKeys.parse_key("\xff/system/materializer_keys/end") == {:materializer_key, "end"}
    end

    test "returns :unknown for unrecognized system keys" do
      assert SystemKeys.parse_key("\xff/system/unknown/path") == :unknown
    end

    test "returns :error for non-system keys" do
      assert SystemKeys.parse_key("user/data") == :error
      assert SystemKeys.parse_key("") == :error
    end

    test "returns :error for non-binary input" do
      assert SystemKeys.parse_key(nil) == :error
      assert SystemKeys.parse_key(123) == :error
      assert SystemKeys.parse_key(:atom) == :error
    end
  end

  describe "round-trip: generate → parse" do
    test "layout_log round-trips correctly" do
      for id <- ["test-log-123", "log_abc", "", "with/slash"] do
        key = SystemKeys.layout_log(id)
        assert SystemKeys.parse_key(key) == {:layout_log, id}
      end
    end

    test "shard_key round-trips correctly" do
      for end_key <- ["m", "\xff", "", "key-range-end"] do
        key = SystemKeys.shard_key(end_key)
        assert SystemKeys.parse_key(key) == {:shard_key, end_key}
      end
    end

    test "shard round-trips correctly" do
      for tag <- [0, 42, 999] do
        key = SystemKeys.shard(tag)
        assert SystemKeys.parse_key(key) == {:shard, "#{tag}"}
      end
    end

    test "materializer_key round-trips correctly" do
      for end_key <- ["m", "\xff", "", "mat-end-key"] do
        key = SystemKeys.materializer_key(end_key)
        assert SystemKeys.parse_key(key) == {:materializer_key, end_key}
      end
    end
  end

  describe "key consistency" do
    test "all keys start with system prefix" do
      prefix = SystemKeys.system_prefix()

      # Collect all static keys from different categories
      all_static_keys =
        SystemKeys.all_cluster_keys() ++
          [SystemKeys.layout_services(), SystemKeys.layout_id()] ++
          SystemKeys.all_legacy_keys()

      # Test all static keys have correct prefix
      for key <- all_static_keys do
        assert String.starts_with?(key, prefix),
               "Key #{key} does not start with #{prefix}"
      end

      # Test dynamic keys have correct prefix
      dynamic_keys = [
        SystemKeys.layout_log("test"),
        SystemKeys.shard_key("test"),
        SystemKeys.shard(0),
        SystemKeys.materializer_key("test")
      ]

      for key <- dynamic_keys do
        assert String.starts_with?(key, prefix),
               "Dynamic key #{key} does not start with #{prefix}"
      end
    end

    test "no duplicate keys across categories" do
      all_keys =
        SystemKeys.all_cluster_keys() ++
          [SystemKeys.layout_services(), SystemKeys.layout_id()] ++
          SystemKeys.all_legacy_keys()

      unique_keys = Enum.uniq(all_keys)
      duplicates = all_keys -- unique_keys

      assert length(all_keys) == length(unique_keys),
             "Found duplicate keys: #{inspect(duplicates)}"
    end

    test "dynamic keys work with various ID formats" do
      test_ids = ["123", "log_abc", "key-456", "end_key_789", ""]

      log_prefix = SystemKeys.layout_logs_prefix()
      shard_keys_prefix = SystemKeys.shard_keys_prefix()

      for id <- test_ids do
        log_key = SystemKeys.layout_log(id)
        shard_key = SystemKeys.shard_key(id)

        # Verify both prefix and suffix in a single assertion pattern
        assert String.starts_with?(log_key, log_prefix) and String.ends_with?(log_key, id),
               "Log key #{log_key} should start with #{log_prefix} and end with #{id}"

        assert String.starts_with?(shard_key, shard_keys_prefix) and String.ends_with?(shard_key, id),
               "Shard key #{shard_key} should start with #{shard_keys_prefix} and end with #{id}"
      end
    end
  end
end
