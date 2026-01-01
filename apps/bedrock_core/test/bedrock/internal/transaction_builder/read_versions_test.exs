defmodule Bedrock.Internal.TransactionBuilder.ReadVersionsTest do
  use ExUnit.Case, async: true

  alias Bedrock.Internal.TransactionBuilder.ReadVersions
  alias Bedrock.Internal.TransactionBuilder.State

  describe "ensure_read_version!/2" do
    test "returns state with read version when successful" do
      state = %State{
        read_version: nil,
        transaction_system_layout: %{sequencer: self()},
        fetch_timeout_in_ms: 100
      }

      # Mock function that returns a successful read version
      next_read_version_fn = fn _state ->
        {:ok, Bedrock.DataPlane.Version.from_integer(100)}
      end

      result = ReadVersions.ensure_read_version!(state, next_read_version_fn: next_read_version_fn)

      assert result.read_version == Bedrock.DataPlane.Version.from_integer(100)
    end

    test "raises when read version acquisition fails" do
      state = %State{
        read_version: nil,
        transaction_system_layout: %{sequencer: self()},
        fetch_timeout_in_ms: 100
      }

      # Mock function that returns an error
      next_read_version_fn = fn _state ->
        {:error, :unavailable}
      end

      assert_raise RuntimeError, "Failed to acquire read version: :unavailable", fn ->
        ReadVersions.ensure_read_version!(state, next_read_version_fn: next_read_version_fn)
      end
    end

    test "raises with timeout reason when sequencer times out" do
      state = %State{
        read_version: nil,
        transaction_system_layout: %{sequencer: self()},
        fetch_timeout_in_ms: 100
      }

      # Mock function that returns a timeout error
      next_read_version_fn = fn _state ->
        {:error, :timeout}
      end

      assert_raise RuntimeError, "Failed to acquire read version: :timeout", fn ->
        ReadVersions.ensure_read_version!(state, next_read_version_fn: next_read_version_fn)
      end
    end
  end
end
