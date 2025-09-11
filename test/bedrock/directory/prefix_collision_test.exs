defmodule Bedrock.Directory.PrefixCollisionTest do
  use ExUnit.Case, async: true

  import Bedrock.Test.DirectoryHelpers
  import Mox

  alias Bedrock.Directory
  alias Bedrock.Directory.Layer

  setup do
    stub(MockRepo, :transaction, fn callback -> callback.(:mock_txn) end)
    :ok
  end

  setup :verify_on_exit!

  # Helper when we expect collision to be detected in range scan
  defp expect_collision_in_range(repo, prefix, collision_data) do
    expected_range = Bedrock.KeyRange.from_prefix(prefix)

    expect(repo, :range, fn :mock_txn, ^expected_range, opts ->
      assert opts[:limit] == 1
      collision_data
    end)

    # No get() call because collision detected early
  end

  # Helper when no collision in range but need ancestor checking
  defp expect_collision_check_with_ancestors(repo, prefix) do
    expected_range = Bedrock.KeyRange.from_prefix(prefix)

    repo
    |> expect(:range, fn :mock_txn, ^expected_range, opts ->
      assert opts[:limit] == 1
      # No collision in range
      []
    end)
    |> expect(:get, fn :mock_txn, key ->
      # This handles both the full prefix check and ancestor checks
      # The implementation checks the full prefix first, then ancestors
      cond do
        key == prefix -> nil
        # Ancestor check
        byte_size(key) < byte_size(prefix) -> nil
        true -> raise "Unexpected key: #{inspect(key)}"
      end
    end)
  end

  describe "prefix collision detection" do
    test "rejects manual prefix that collides with existing data" do
      prefix = <<1, 2>>
      collision_data = [{<<1, 2, 3>>, "some_data"}]

      MockRepo
      |> expect_version_initialization()
      |> expect_directory_exists([], nil)
      |> expect_collision_in_range(prefix, collision_data)

      layer = Layer.new(MockRepo)

      assert {:error, :prefix_collision} =
               Directory.create(layer, [], prefix: prefix)
    end

    test "accepts manual prefix that doesn't collide" do
      prefix = <<10, 20>>
      packed_value = {prefix, ""}

      MockRepo
      |> expect_version_initialization()
      |> expect_directory_exists([], nil)
      |> expect_collision_check_with_ancestors(prefix)
      |> expect_directory_creation([], packed_value)

      layer = Layer.new(MockRepo)

      assert {:ok, %{prefix: ^prefix}} =
               Directory.create(layer, [], prefix: prefix)
    end

    test "rejects reserved system prefixes" do
      reserved_prefixes = [<<0xFE>>, <<0xFF>>]

      for prefix <- reserved_prefixes do
        MockRepo
        |> expect_version_initialization()
        |> expect_directory_exists([], nil)

        layer = Layer.new(MockRepo)

        assert {:error, :prefix_collision} =
                 Directory.create(layer, [], prefix: prefix)
      end
    end

    test "detects when new prefix would be ancestor of existing key" do
      prefix = <<1, 2>>
      collision_data = [{<<1, 2, 3>>, "existing_data"}]

      MockRepo
      |> expect_version_initialization()
      |> expect_directory_exists([], nil)
      |> expect_collision_in_range(prefix, collision_data)

      layer = Layer.new(MockRepo)

      assert {:error, :prefix_collision} =
               Directory.create(layer, [], prefix: prefix)
    end

    test "detects when existing key would be ancestor of new prefix" do
      prefix = <<1, 2, 3>>

      # We need a custom expectation here since an ancestor will return data
      MockRepo
      |> expect_version_initialization()
      |> expect_directory_exists([], nil)
      |> expect(:range, fn :mock_txn, range, opts ->
        assert Bedrock.KeyRange.from_prefix(prefix) == range
        assert opts[:limit] == 1
        []
      end)
      |> expect(:get, fn :mock_txn, key ->
        cond do
          key == prefix -> nil
          # Ancestor has data
          key == <<1>> -> "existing_data"
          true -> raise "Unexpected key: #{inspect(key)}"
        end
      end)

      layer = Layer.new(MockRepo)

      assert {:error, :prefix_collision} =
               Directory.create(layer, [], prefix: prefix)
    end
  end
end
