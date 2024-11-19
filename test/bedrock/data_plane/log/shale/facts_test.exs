defmodule Bedrock.DataPlane.Log.Shale.FactsTest do
  use ExUnit.Case, async: true
  alias Bedrock.DataPlane.Log.Shale.Facts
  alias Bedrock.DataPlane.Log.Shale.State

  describe "info/2" do
    setup do
      state = %State{
        id: "test_id",
        otp_name: :test_otp,
        oldest_version: 1,
        last_version: 10
      }

      {:ok, state: state}
    end

    test "returns info for a single fact", %{state: state} do
      self = self()
      supported_info = Facts.supported_info()
      assert {:ok, "test_id"} = Facts.info(state, :id)
      assert {:ok, :log} = Facts.info(state, :kind)
      assert {:ok, :test_otp} = Facts.info(state, :otp_name)
      assert {:ok, ^self} = Facts.info(state, :pid)
      assert {:ok, ^supported_info} = Facts.info(state, :supported_info)
      assert {:ok, :unavailable} = Facts.info(state, :minimum_durable_version)
      assert {:ok, 1} = Facts.info(state, :oldest_version)
      assert {:ok, 10} = Facts.info(state, :last_version)
    end

    test "returns info for a list of facts", %{state: state} do
      facts = [:id, :kind, :otp_name]

      expected = %{
        id: "test_id",
        kind: :log,
        otp_name: :test_otp
      }

      assert {:ok, ^expected} = Facts.info(state, facts)
    end

    test "returns error for unsupported fact", %{state: state} do
      assert {:error, :unsupported} = Facts.info(state, :unsupported_fact)
    end
  end

  describe "supported_info/0" do
    test "returns the list of supported info" do
      expected = [
        :id,
        :kind,
        :minimum_durable_version,
        :oldest_version,
        :last_version,
        :otp_name,
        :pid,
        :state,
        :supported_info
      ]

      assert expected == Facts.supported_info()
    end
  end
end
