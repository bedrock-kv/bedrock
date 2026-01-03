defmodule Bedrock.ControlPlane.Config.ParametersTest do
  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.Config.Parameters

  describe "put_desired_replication_factor/2" do
    test "updates the desired_replication_factor field" do
      params = Parameters.new([:node1@localhost])
      assert params.desired_replication_factor == 1

      updated = Parameters.put_desired_replication_factor(params, 3)
      assert updated.desired_replication_factor == 3
    end
  end
end
