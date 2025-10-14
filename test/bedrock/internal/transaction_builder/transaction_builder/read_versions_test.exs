defmodule Bedrock.Internal.TransactionBuilder.ReadVersionsTest do
  use ExUnit.Case, async: true

  alias Bedrock.Internal.TransactionBuilder.ReadVersions
  alias Bedrock.Internal.TransactionBuilder.State

  def create_test_state(opts \\ []) do
    %State{
      state: :valid,
      transaction_system_layout: %{
        sequencer: Keyword.get(opts, :sequencer, :test_sequencer)
      },
      read_version: Keyword.get(opts, :read_version)
    }
  end

  defp success_opts(sequencer_result) do
    [
      sequencer_fn: fn _ -> {:ok, sequencer_result} end
    ]
  end

  defp error_opts(sequencer_result) do
    [
      sequencer_fn: fn _ -> sequencer_result end
    ]
  end

  describe "next_read_version/2" do
    test "successfully gets read version" do
      state = create_test_state()
      opts = success_opts(12_345)

      assert {:ok, 12_345} = ReadVersions.next_read_version(state, opts)
    end

    test "handles sequencer error" do
      state = create_test_state()
      opts = error_opts({:error, :unavailable})

      assert {:error, :unavailable} = ReadVersions.next_read_version(state, opts)
    end

    test "works with different sequencer references" do
      state = create_test_state(sequencer: :custom_sequencer)
      opts = success_opts(99_999)

      assert {:ok, 99_999} = ReadVersions.next_read_version(state, opts)
    end

    test "handles zero read version" do
      state = create_test_state()
      opts = success_opts(0)

      assert {:ok, 0} = ReadVersions.next_read_version(state, opts)
    end

    test "handles large read version numbers" do
      state = create_test_state()
      large_version = 999_999_999_999
      opts = success_opts(large_version)

      assert {:ok, ^large_version} = ReadVersions.next_read_version(state, opts)
    end

    test "uses default functions when no opts provided" do
      state = create_test_state()

      # This will fail with the real functions since we don't have real infrastructure
      # but it tests that the default functions are being called
      assert {:error, _} = ReadVersions.next_read_version(state)
    end
  end

  describe "edge cases and error conditions" do
    test "next_read_version with nil transaction_system_layout sequencer" do
      state = %{create_test_state() | transaction_system_layout: %{sequencer: nil}}
      opts = error_opts({:error, :no_sequencer})

      assert {:error, :no_sequencer} = ReadVersions.next_read_version(state, opts)
    end
  end
end
