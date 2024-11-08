defmodule Bedrock.DataPlane.Log.Shale.FactsTest do
  use ExUnit.Case, async: true
  alias Bedrock.DataPlane.Log.Shale.Facts
  alias Bedrock.DataPlane.Log.Shale.State

  setup do
    state = %State{
      id: "test_id",
      # state: :active,
      otp_name: :test_otp,
      oldest_version: 1,
      last_version: 10
    }

    {:ok, state: state}
  end

  test "info/2 returns correct info for a single fact", %{state: state} do
    me = self()
    supported_info = Facts.supported_info()

    assert {:ok, "test_id"} = Facts.info(state, :id)
    assert {:ok, :log} = Facts.info(state, :kind)
    assert {:ok, :test_otp} = Facts.info(state, :otp_name)
    assert {:ok, ^me} = Facts.info(state, :pid)
    assert {:ok, ^supported_info} = Facts.info(state, :supported_info)
    assert {:ok, :unavailable} = Facts.info(state, :minimum_durable_version)
    assert {:ok, 1} = Facts.info(state, :oldest_version)
    assert {:ok, 10} = Facts.info(state, :last_version)
  end

  test "info/2 returns correct info for a list of facts", %{state: state} do
    facts = [
      :id,
      :kind,
      :otp_name,
      :pid,
      :supported_info,
      :minimum_durable_version,
      :oldest_version,
      :last_version
    ]

    expected_result = %{
      id: "test_id",
      kind: :log,
      otp_name: :test_otp,
      pid: self(),
      supported_info: Facts.supported_info(),
      minimum_durable_version: :unavailable,
      oldest_version: 1,
      last_version: 10
    }

    assert {:ok, ^expected_result} = Facts.info(state, facts)
  end

  test "info/2 returns error for unsupported fact", %{state: state} do
    assert {:error, :unsupported} = Facts.info(state, :unsupported_fact)
  end
end
