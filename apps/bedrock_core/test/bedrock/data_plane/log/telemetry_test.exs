defmodule Bedrock.DataPlane.Log.TelemetryTest do
  use ExUnit.Case, async: false

  alias Bedrock.DataPlane.Log.Telemetry
  alias Bedrock.DataPlane.Version
  alias Bedrock.Test.DataPlane.TransactionTestSupport

  # Named handler function to avoid telemetry warning
  def handle_event(event, measurements, metadata, config) do
    send(config.test_pid, {:telemetry_event, event, measurements, metadata})
  end

  setup do
    # Clear any existing trace metadata
    Process.delete(:trace_metadata)

    # Attach a test handler to capture telemetry events
    test_pid = self()
    handler_id = {:test_handler, make_ref()}

    :telemetry.attach_many(
      handler_id,
      [
        [:bedrock, :log, :started],
        [:bedrock, :log, :lock_for_recovery],
        [:bedrock, :log, :recover_from],
        [:bedrock, :log, :push],
        [:bedrock, :log, :push_out_of_order],
        [:bedrock, :log, :pull]
      ],
      &__MODULE__.handle_event/4,
      %{test_pid: test_pid}
    )

    on_exit(fn ->
      :telemetry.detach(handler_id)
      Process.delete(:trace_metadata)
    end)

    :ok
  end

  describe "trace_metadata/0" do
    test "returns empty map when no metadata is set" do
      assert Telemetry.trace_metadata() == %{}
    end

    test "returns stored metadata" do
      Process.put(:trace_metadata, %{key: :value})
      assert Telemetry.trace_metadata() == %{key: :value}
    end
  end

  describe "trace_metadata/1" do
    test "sets metadata in process dictionary" do
      # Process.put returns the OLD value, which is nil initially
      result = Telemetry.trace_metadata(%{foo: :bar})
      assert result == nil
      assert Telemetry.trace_metadata() == %{foo: :bar}
    end

    test "merges with existing metadata" do
      Telemetry.trace_metadata(%{key1: :value1})
      # Returns the old value (%{key1: :value1})
      result = Telemetry.trace_metadata(%{key2: :value2})

      assert result == %{key1: :value1}
      # But the new value is merged
      assert Telemetry.trace_metadata() == %{key1: :value1, key2: :value2}
    end

    test "overwrites existing keys" do
      Telemetry.trace_metadata(%{key: :old})
      # Returns the old value (%{key: :old})
      result = Telemetry.trace_metadata(%{key: :new})

      assert result == %{key: :old}
      # But the new value overwrites
      assert Telemetry.trace_metadata() == %{key: :new}
    end
  end

  describe "trace_started/0" do
    test "emits telemetry event with trace metadata" do
      Telemetry.trace_metadata(%{request_id: "123"})
      assert :ok = Telemetry.trace_started()

      assert_received {:telemetry_event, [:bedrock, :log, :started], %{}, %{request_id: "123"}}
    end

    test "emits event with empty metadata when none is set" do
      assert :ok = Telemetry.trace_started()

      assert_received {:telemetry_event, [:bedrock, :log, :started], %{}, %{}}
    end
  end

  describe "trace_lock_for_recovery/1" do
    test "emits telemetry event with epoch" do
      epoch = 5
      assert :ok = Telemetry.trace_lock_for_recovery(epoch)

      assert_received {:telemetry_event, [:bedrock, :log, :lock_for_recovery], %{}, %{epoch: 5}}
    end

    test "includes trace metadata in event" do
      Telemetry.trace_metadata(%{session_id: "abc"})
      assert :ok = Telemetry.trace_lock_for_recovery(10)

      assert_received {:telemetry_event, [:bedrock, :log, :lock_for_recovery], %{}, %{epoch: 10, session_id: "abc"}}
    end
  end

  describe "trace_recover_from/3" do
    test "emits telemetry event with recovery details" do
      source_log = :some_log_ref
      first_version = Version.from_integer(0)
      last_version = Version.from_integer(100)

      assert :ok = Telemetry.trace_recover_from(source_log, first_version, last_version)

      assert_received {:telemetry_event, [:bedrock, :log, :recover_from], %{},
                       %{
                         source_log: :some_log_ref,
                         first_version: ^first_version,
                         last_version: ^last_version
                       }}
    end

    test "includes trace metadata" do
      Telemetry.trace_metadata(%{recovery_id: "rec1"})
      source_log = :log
      first_version = Version.zero()
      last_version = Version.from_integer(50)

      assert :ok = Telemetry.trace_recover_from(source_log, first_version, last_version)

      assert_received {:telemetry_event, [:bedrock, :log, :recover_from], %{}, metadata}
      assert metadata.recovery_id == "rec1"
      assert metadata.source_log == :log
      assert metadata.first_version == first_version
      assert metadata.last_version == last_version
    end
  end

  describe "trace_push_transaction/1" do
    test "emits telemetry event with transaction" do
      transaction =
        TransactionTestSupport.new_log_transaction(Version.from_integer(1), %{"key" => "value"})

      assert :ok = Telemetry.trace_push_transaction(transaction)

      assert_received {:telemetry_event, [:bedrock, :log, :push], %{}, %{transaction: ^transaction}}
    end

    test "includes trace metadata" do
      Telemetry.trace_metadata(%{client_id: "client1"})

      transaction =
        TransactionTestSupport.new_log_transaction(Version.from_integer(2), %{"a" => "b"})

      assert :ok = Telemetry.trace_push_transaction(transaction)

      assert_received {:telemetry_event, [:bedrock, :log, :push], %{}, metadata}
      assert metadata.client_id == "client1"
      assert metadata.transaction == transaction
    end
  end

  describe "trace_push_out_of_order/2" do
    test "emits telemetry event with version mismatch details" do
      expected_version = Version.from_integer(10)
      current_version = Version.from_integer(5)

      assert :ok = Telemetry.trace_push_out_of_order(expected_version, current_version)

      assert_received {:telemetry_event, [:bedrock, :log, :push_out_of_order], %{},
                       %{expected_version: ^expected_version, current_version: ^current_version}}
    end

    test "includes trace metadata" do
      Telemetry.trace_metadata(%{transaction_id: "tx123"})
      expected = Version.from_integer(100)
      current = Version.from_integer(99)

      assert :ok = Telemetry.trace_push_out_of_order(expected, current)

      assert_received {:telemetry_event, [:bedrock, :log, :push_out_of_order], %{}, metadata}
      assert metadata.transaction_id == "tx123"
      assert metadata.expected_version == expected
      assert metadata.current_version == current
    end
  end

  describe "trace_pull_transactions/2" do
    test "emits telemetry event with pull details" do
      from_version = Version.from_integer(50)
      opts = [limit: 10, timeout: 5000]

      assert :ok = Telemetry.trace_pull_transactions(from_version, opts)

      assert_received {:telemetry_event, [:bedrock, :log, :pull], %{}, %{from_version: ^from_version, opts: ^opts}}
    end

    test "handles empty opts" do
      from_version = Version.zero()
      opts = []

      assert :ok = Telemetry.trace_pull_transactions(from_version, opts)

      assert_received {:telemetry_event, [:bedrock, :log, :pull], %{}, %{from_version: ^from_version, opts: []}}
    end

    test "includes trace metadata" do
      Telemetry.trace_metadata(%{subscriber_id: "sub1"})
      from_version = Version.from_integer(20)
      opts = [batch_size: 50]

      assert :ok = Telemetry.trace_pull_transactions(from_version, opts)

      assert_received {:telemetry_event, [:bedrock, :log, :pull], %{}, metadata}
      assert metadata.subscriber_id == "sub1"
      assert metadata.from_version == from_version
      assert metadata.opts == opts
    end
  end

  describe "integration" do
    test "all trace functions return :ok" do
      transaction =
        TransactionTestSupport.new_log_transaction(Version.from_integer(1), %{"k" => "v"})

      version = Version.from_integer(1)

      assert :ok = Telemetry.trace_started()
      assert :ok = Telemetry.trace_lock_for_recovery(1)
      assert :ok = Telemetry.trace_recover_from(:log, version, version)
      assert :ok = Telemetry.trace_push_transaction(transaction)
      assert :ok = Telemetry.trace_push_out_of_order(version, version)
      assert :ok = Telemetry.trace_pull_transactions(version, [])
    end

    test "metadata persists across multiple trace calls" do
      Telemetry.trace_metadata(%{session: "s1"})

      Telemetry.trace_started()
      assert_received {:telemetry_event, [:bedrock, :log, :started], %{}, %{session: "s1"}}

      Telemetry.trace_lock_for_recovery(2)

      assert_received {:telemetry_event, [:bedrock, :log, :lock_for_recovery], %{}, %{session: "s1", epoch: 2}}
    end
  end
end
