defmodule Bedrock.DataPlane.Log.Shale.FactsTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Log.Shale.Facts
  alias Bedrock.DataPlane.Log.Shale.State

  @test_state %State{
    id: "test_id",
    otp_name: :test_otp,
    oldest_version: 1,
    last_version: 10
  }

  describe "info/2" do
    test "returns info for static facts" do
      assert {:ok, "test_id"} = Facts.info(@test_state, :id)
      assert {:ok, :log} = Facts.info(@test_state, :kind)
      assert {:ok, :test_otp} = Facts.info(@test_state, :otp_name)
      assert {:ok, :unavailable} = Facts.info(@test_state, :minimum_durable_version)
      assert {:ok, 1} = Facts.info(@test_state, :oldest_version)
      assert {:ok, 10} = Facts.info(@test_state, :last_version)
    end

    test "returns info for dynamic facts" do
      current_pid = self()
      supported_info = Facts.supported_info()

      assert {:ok, ^current_pid} = Facts.info(@test_state, :pid)
      assert {:ok, ^supported_info} = Facts.info(@test_state, :supported_info)
    end

    test "returns info for a list of facts" do
      facts = [:id, :kind, :otp_name]

      expected = %{
        id: "test_id",
        kind: :log,
        otp_name: :test_otp
      }

      assert {:ok, ^expected} = Facts.info(@test_state, facts)
    end

    test "returns error for unsupported fact" do
      assert {:error, :unsupported} = Facts.info(@test_state, :unsupported_fact)
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
