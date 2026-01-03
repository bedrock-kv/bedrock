defmodule Bedrock.ControlPlane.Director.Recovery.SharedTest do
  use ExUnit.Case, async: true

  alias Bedrock.ControlPlane.Director.Recovery.Shared

  describe "starter_for/1" do
    test "returns a function with arity 2" do
      starter = Shared.starter_for(:test_supervisor)

      assert is_function(starter, 2)
      assert :erlang.fun_info(starter, :arity) == {:arity, 2}
    end

    test "function is created for given supervisor name" do
      # Create a starter function for a specific supervisor
      starter = Shared.starter_for(:my_supervisor)

      # Verify it's the correct function type
      assert is_function(starter, 2)

      # Create another starter for a different supervisor
      another_starter = Shared.starter_for(:different_supervisor)

      # Both should be functions
      assert is_function(another_starter, 2)
    end

    test "returned function accepts child_spec and node parameters" do
      starter = Shared.starter_for(:test_supervisor)

      # Verify the function signature by checking it can be called with 2 args
      # We expect it to fail (supervisor doesn't exist) but we're testing the signature
      child_spec = %{id: :test, start: {Agent, :start_link, [fn -> :ok end]}}

      # This will fail because :test_supervisor doesn't exist on node()
      # but it proves the function accepts the right parameters
      result = starter.(child_spec, node())

      # Should return an error since the supervisor doesn't exist
      assert match?({:error, _}, result)
    end

    test "function handles exit from supervisor operations" do
      starter = Shared.starter_for(:nonexistent_supervisor)
      child_spec = %{id: :test, start: {Agent, :start_link, [fn -> :ok end]}}

      # Should catch the exit and return error tuple
      result = starter.(child_spec, node())

      assert {:error, {:supervisor_exit, _reason}} = result
    end
  end
end
