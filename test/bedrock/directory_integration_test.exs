defmodule Bedrock.DirectoryIntegrationTest do
  use ExUnit.Case, async: true

  import Mox

  alias Bedrock.Directory
  alias Bedrock.Directory.Layer

  @version_key <<254, 6, 1, 118, 101, 114, 115, 105, 111, 110, 0, 0>>
  @current_version <<1::little-32, 0::little-32, 0::little-32>>

  setup do
    stub(MockRepo, :transaction, fn callback -> callback.(:mock_txn) end)
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

  # Helper for version management expectations specific to integration tests
  defp expect_version_management(first_create?) do
    if first_create? do
      # First create - initialize version
      expect(MockRepo, :get, fn _txn, key ->
        assert key == @version_key
        nil
      end)

      expect(MockRepo, :get, fn _txn, key ->
        assert key == @version_key
        nil
      end)

      expect(MockRepo, :put, fn _txn, key, value ->
        assert key == @version_key
        assert value == @current_version
        :ok
      end)
    else
      # Subsequent creates - version exists
      expect(MockRepo, :get, fn _txn, key ->
        assert key == @version_key
        @current_version
      end)

      expect(MockRepo, :get, fn _txn, key ->
        assert key == @version_key
        @current_version
      end)
    end
  end

  test "full directory workflow" do
    {_agent, next_prefix_fn} = create_prefix_allocator()

    # === Creating root directory ===
    expect_version_management(true)

    # Check if root directory exists (should be nil)
    expect(MockRepo, :get, fn _txn, key ->
      assert key == <<254>>
      nil
    end)

    # Store root directory metadata
    expect(MockRepo, :put, fn _txn, key, value ->
      assert key == <<254>>
      assert {<<0, 0, 0, 0>>, ""} = Bedrock.Key.unpack(value)
      :ok
    end)

    # === Creating users directory ===
    expect_version_management(false)

    # Check if users directory exists (should be nil)
    expect(MockRepo, :get, fn _txn, key ->
      expected_key = <<254>> <> Bedrock.Key.pack(["users"])
      assert key == expected_key
      nil
    end)

    # Check parent (root) exists
    expect(MockRepo, :get, fn _txn, key ->
      assert key == <<254>>
      Bedrock.Key.pack({<<0, 0, 0, 0>>, ""})
    end)

    # Store users directory metadata
    expect(MockRepo, :put, fn _txn, key, value ->
      expected_key = <<254>> <> Bedrock.Key.pack(["users"])
      assert key == expected_key
      assert {<<0, 0, 0, 1>>, "document"} = Bedrock.Key.unpack(value)
      :ok
    end)

    # Create directory layer
    root = Layer.new(MockRepo, next_prefix_fn: next_prefix_fn)

    # Execute the workflow with pattern matching assertions
    assert {:ok, %{path: [], prefix: <<0, 0, 0, 0>>}} = Directory.create(root, [])

    assert {:ok, users_dir} = Directory.create(root, ["users"], layer: "document")
    assert %{layer: "document", path: ["users"], prefix: <<0, 0, 0, 1>>} = users_dir

    # Test subspace generation uses the correct prefix
    assert %Bedrock.Subspace{prefix: <<0, 0, 0, 1>>} = Directory.get_subspace(users_dir)
  end
end
