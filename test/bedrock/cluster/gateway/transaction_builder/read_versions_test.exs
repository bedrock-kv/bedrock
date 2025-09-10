defmodule Bedrock.Cluster.Gateway.TransactionBuilder.ReadVersionsTest do
  use ExUnit.Case, async: true

  alias Bedrock.Cluster.Gateway.TransactionBuilder.ReadVersions
  alias Bedrock.Cluster.Gateway.TransactionBuilder.State

  def create_test_state(opts \\ []) do
    %State{
      state: :valid,
      gateway: Keyword.get(opts, :gateway, :test_gateway),
      transaction_system_layout: %{
        sequencer: Keyword.get(opts, :sequencer, :test_sequencer)
      },
      read_version: Keyword.get(opts, :read_version),
      read_version_lease_expiration: Keyword.get(opts, :read_version_lease_expiration)
    }
  end

  defp success_opts(sequencer_result, gateway_result) do
    [
      sequencer_fn: fn _ -> {:ok, sequencer_result} end,
      gateway_fn: fn _, _ -> {:ok, gateway_result} end
    ]
  end

  defp error_opts(sequencer_result, gateway_result \\ {:ok, 5000}) do
    [
      sequencer_fn: fn _ -> sequencer_result end,
      gateway_fn: fn _, _ -> gateway_result end
    ]
  end

  describe "next_read_version/2" do
    test "successfully gets read version and lease" do
      state = create_test_state()
      opts = success_opts(12_345, 5000)

      assert {:ok, 12_345, 5000} = ReadVersions.next_read_version(state, opts)
    end

    test "handles sequencer error" do
      state = create_test_state()
      opts = error_opts({:error, :unavailable})

      assert {:error, :unavailable} = ReadVersions.next_read_version(state, opts)
    end

    test "handles gateway error" do
      state = create_test_state()
      opts = error_opts({:ok, 12_345}, {:error, :timeout})

      assert {:error, :timeout} = ReadVersions.next_read_version(state, opts)
    end

    test "works with different sequencer and gateway references" do
      state = create_test_state(sequencer: :custom_sequencer, gateway: :custom_gateway)
      opts = success_opts(99_999, 10_000)

      assert {:ok, 99_999, 10_000} = ReadVersions.next_read_version(state, opts)
    end

    test "handles zero read version" do
      state = create_test_state()
      opts = success_opts(0, 1000)

      assert {:ok, 0, 1000} = ReadVersions.next_read_version(state, opts)
    end

    test "handles large read version numbers" do
      state = create_test_state()
      large_version = 999_999_999_999
      opts = success_opts(large_version, 2000)

      assert {:ok, ^large_version, 2000} = ReadVersions.next_read_version(state, opts)
    end

    test "uses default functions when no opts provided" do
      state = create_test_state()

      # This will fail with the real functions since we don't have real infrastructure
      # but it tests that the default functions are being called
      assert {:error, _} = ReadVersions.next_read_version(state)
    end
  end

  describe "renew_read_version_lease/2" do
    test "successfully renews lease" do
      current_time = 50_000
      state = create_test_state(read_version: 12_345, read_version_lease_expiration: current_time + 1000)

      opts = [
        gateway_fn: fn :test_gateway, 12_345 -> {:ok, 5000} end,
        time_fn: fn -> current_time end
      ]

      assert {:ok, %State{read_version_lease_expiration: expected_expiration}} =
               ReadVersions.renew_read_version_lease(state, opts)

      assert expected_expiration == current_time + 5000
    end

    test "handles gateway renewal error" do
      state = create_test_state(read_version: 12_345)

      opts = [
        gateway_fn: fn :test_gateway, 12_345 -> {:error, :read_version_lease_expired} end,
        time_fn: fn -> 50_000 end
      ]

      assert {:error, :read_version_lease_expired} =
               ReadVersions.renew_read_version_lease(state, opts)
    end

    test "preserves other state fields during renewal" do
      current_time = 60_000

      original_state = %State{
        state: :valid,
        gateway: :test_gateway,
        transaction_system_layout: %{test: "data"},
        read_version: 12_345,
        read_version_lease_expiration: current_time - 1000,
        stack: []
      }

      opts = [
        gateway_fn: fn :test_gateway, 12_345 -> {:ok, 3000} end,
        time_fn: fn -> current_time end
      ]

      # Verify that only the lease expiration changed, all other fields preserved
      assert {:ok, new_state} = ReadVersions.renew_read_version_lease(original_state, opts)

      assert %State{
               state: :valid,
               gateway: :test_gateway,
               transaction_system_layout: %{test: "data"},
               read_version: 12_345,
               read_version_lease_expiration: expected_expiration,
               stack: []
             } = new_state

      assert expected_expiration == current_time + 3000
    end

    test "works with different gateway references" do
      current_time = 70_000
      state = create_test_state(gateway: :custom_gateway, read_version: 54_321)

      opts = [
        gateway_fn: fn :custom_gateway, 54_321 -> {:ok, 8000} end,
        time_fn: fn -> current_time end
      ]

      assert {:ok, %State{read_version_lease_expiration: expected_expiration}} =
               ReadVersions.renew_read_version_lease(state, opts)

      assert expected_expiration == current_time + 8000
    end

    test "handles zero lease duration" do
      current_time = 80_000
      state = create_test_state(read_version: 12_345)

      opts = [
        gateway_fn: fn :test_gateway, 12_345 -> {:ok, 0} end,
        time_fn: fn -> current_time end
      ]

      assert {:ok, %State{read_version_lease_expiration: ^current_time}} =
               ReadVersions.renew_read_version_lease(state, opts)
    end

    test "handles large time values" do
      current_time = 999_999_999_999
      state = create_test_state(read_version: 12_345)

      opts = [
        gateway_fn: fn :test_gateway, 12_345 -> {:ok, 10_000} end,
        time_fn: fn -> current_time end
      ]

      assert {:ok, %State{read_version_lease_expiration: expected_expiration}} =
               ReadVersions.renew_read_version_lease(state, opts)

      assert expected_expiration == current_time + 10_000
    end

    test "uses default functions when no opts provided" do
      state = create_test_state(read_version: 12_345)

      # This will fail with the real functions since we don't have real infrastructure
      assert {:error, _} = ReadVersions.renew_read_version_lease(state)
    end
  end

  describe "edge cases and error conditions" do
    test "next_read_version with nil transaction_system_layout sequencer" do
      state = %{create_test_state() | transaction_system_layout: %{sequencer: nil}}
      opts = error_opts({:error, :no_sequencer})

      assert {:error, :no_sequencer} = ReadVersions.next_read_version(state, opts)
    end

    test "renew_read_version_lease with nil read_version" do
      state = create_test_state(read_version: nil)

      opts = [
        gateway_fn: fn :test_gateway, nil -> {:error, :invalid_version} end,
        time_fn: fn -> 50_000 end
      ]

      assert {:error, :invalid_version} = ReadVersions.renew_read_version_lease(state, opts)
    end
  end
end
