defmodule Bedrock.DataPlane.Log.ShaleTest do
  use ExUnit.Case, async: true

  alias Bedrock.DataPlane.Log.Shale
  alias Bedrock.DataPlane.Log.Shale.Server

  describe "child_spec/1" do
    test "delegates to Server.child_spec/1" do
      opts = [
        cluster: %{},
        otp_name: :test_log,
        foreman: self(),
        id: "test_log_1",
        path: "/tmp/test_log",
        object_storage: :mock
      ]

      # Test that child_spec is delegated to Server and has expected structure
      assert %{id: {Server, "test_log_1"}} = Shale.child_spec(opts)
    end
  end
end
