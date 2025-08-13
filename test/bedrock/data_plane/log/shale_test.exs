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
        path: "/tmp/test_log"
      ]

      # Test that child_spec is delegated to Server
      spec = Shale.child_spec(opts)

      # Verify the spec has the expected structure
      assert is_map(spec)
      assert spec.id == {Server, "test_log_1"}
    end
  end
end
