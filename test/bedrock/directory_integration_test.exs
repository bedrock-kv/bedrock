defmodule Bedrock.DirectoryIntegrationTest do
  use ExUnit.Case, async: true

  import Bedrock.Test.DirectoryHelpers
  import Mox

  alias Bedrock.Directory

  setup do
    stub(MockRepo, :transact, fn callback -> callback.() end)
    :ok
  end

  setup :verify_on_exit!

  # Helper for deterministic prefix allocator
  defp create_prefix_allocator do
    {:ok, agent} = Agent.start_link(fn -> 0 end)

    next_prefix_fn = fn ->
      Agent.get_and_update(agent, fn n ->
        {<<n::32>>, n + 1}
      end)
    end

    {agent, next_prefix_fn}
  end

  test "full directory workflow" do
    {_agent, next_prefix_fn} = create_prefix_allocator()

    # Use proper expectations with directory helpers
    MockRepo
    |> expect_version_initialization()
    |> expect_directory_exists(["users"], nil)
    |> expect_directory_creation(["users"], {<<0, 0, 0, 0>>, "document"})

    # Create directory layer
    root = Directory.root(MockRepo, next_prefix_fn: next_prefix_fn)

    # Execute the workflow with pattern matching assertions
    # Root is already created by Directory.root(), verify its properties
    assert %Directory.Node{path: [], prefix: ""} = root

    assert {:ok, users_dir} = Directory.create(root, ["users"], layer: "document")
    assert %{layer: "document", path: ["users"], prefix: <<0, 0, 0, 0>>} = users_dir

    # Test keyspace generation uses the correct base prefix
    keyspace = users_dir |> Directory.to_keyspace() |> Bedrock.Keyspace.partition("data")
    # Should start with the users_dir prefix
    assert String.starts_with?(keyspace.prefix, <<0, 0, 0, 0>>)
  end
end
