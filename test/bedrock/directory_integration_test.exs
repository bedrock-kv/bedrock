defmodule Bedrock.DirectoryIntegrationTest do
  use ExUnit.Case, async: true

  import Mox

  alias Bedrock.Directory
  alias Bedrock.Directory.Layer

  # Version key used by the directory layer
  @version_key <<254, 6, 1, 118, 101, 114, 115, 105, 111, 110, 0, 0>>

  setup do
    # Automatically stub transaction to execute callbacks immediately
    stub(MockRepo, :transaction, fn callback -> callback.(:mock_txn) end)
    :ok
  end

  setup :verify_on_exit!

  test "full directory workflow" do
    # Test specific key/value operations in correct order

    # === Creating root directory ===

    # 1. check_version - checks if version exists
    expect(MockRepo, :get, fn _txn, key ->
      assert key == @version_key
      # No version yet
      nil
    end)

    # 2. ensure_version_initialized - checks again
    expect(MockRepo, :get, fn _txn, key ->
      assert key == @version_key
      # Still no version
      nil
    end)

    # 3. init_version - writes the version
    expect(MockRepo, :put, fn _txn, key, value ->
      assert key == @version_key
      assert value == <<1::little-32, 0::little-32, 0::little-32>>
      :ok
    end)

    # 4. Check if root directory exists (should be nil)
    expect(MockRepo, :get, fn _txn, key ->
      assert key == <<254>>
      # Root doesn't exist
      nil
    end)

    # 5. Store root directory metadata
    expect(MockRepo, :put, fn _txn, key, value ->
      assert key == <<254>>
      # First allocated prefix, no layer for root
      assert {<<0, 0, 0, 0>>, ""} = Bedrock.Key.unpack(value)
      :ok
    end)

    # === Creating users directory ===

    # 6. check_version - version now exists
    expect(MockRepo, :get, fn _txn, key ->
      assert key == @version_key
      <<1::little-32, 0::little-32, 0::little-32>>
    end)

    # 7. ensure_version_initialized - version exists
    expect(MockRepo, :get, fn _txn, key ->
      assert key == @version_key
      <<1::little-32, 0::little-32, 0::little-32>>
    end)

    # 8. Check if users directory exists (should be nil)
    expect(MockRepo, :get, fn _txn, key ->
      expected_key =
        ["users"]
        |> Bedrock.Key.pack()
        |> then(&(<<254>> <> &1))

      assert key == expected_key
      # Users directory doesn't exist
      nil
    end)

    # 9. Check parent (root) exists
    expect(MockRepo, :get, fn _txn, key ->
      assert key == <<254>>
      # Return the root directory metadata we stored earlier
      Bedrock.Key.pack({<<0, 0, 0, 0>>, ""})
    end)

    # 10. Store users directory metadata
    expect(MockRepo, :put, fn _txn, key, value ->
      expected_key =
        ["users"]
        |> Bedrock.Key.pack()
        |> then(&(<<254>> <> &1))

      assert key == expected_key
      # Second allocated prefix with document layer
      assert {<<0, 0, 0, 1>>, "document"} = Bedrock.Key.unpack(value)
      :ok
    end)

    # Create deterministic prefix allocator
    prefix_counter = Agent.start_link(fn -> 0 end)

    next_prefix_fn = fn ->
      Agent.get_and_update(elem(prefix_counter, 1), fn n ->
        {<<n::32>>, n + 1}
      end)
    end

    # Create root directory layer
    root = Layer.new(MockRepo, next_prefix_fn: next_prefix_fn)

    # Execute the workflow
    assert {:ok, %{path: [], prefix: <<0, 0, 0, 0>>}} = Directory.create(root, [])

    assert {:ok, %{layer: "document", path: ["users"], prefix: <<0, 0, 0, 1>>} = users_dir} =
             Directory.create(root, ["users"], layer: "document")

    # Test subspace generation uses the correct prefix
    assert %Bedrock.Subspace{prefix: <<0, 0, 0, 1>>} = Directory.get_subspace(users_dir)
  end
end
