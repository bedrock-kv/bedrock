defmodule Bedrock.Directory.PrefixCollisionTest do
  use ExUnit.Case, async: true

  import Bedrock.Test.DirectoryHelpers
  import Mox

  alias Bedrock.Directory

  setup do
    stub(MockRepo, :transact, fn callback -> callback.() end)
    :ok
  end

  setup :verify_on_exit!

  # Helper when we expect collision to be detected in range scan
  defp expect_collision_in_range(repo, prefix, collision_data) do
    expected_range = Bedrock.KeyRange.from_prefix(prefix)

    expect(repo, :get_range, fn ^expected_range, opts ->
      assert opts[:limit] == 1
      collision_data
    end)

    # No get() call because collision detected early
  end

  # Helper when no collision in range but need ancestor checking
  defp expect_collision_check_with_ancestors(repo, prefix) do
    repo
    |> expect_collision_check(prefix)
    |> expect(:get, fn key ->
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
      |> expect_directory_exists(["users"], nil)
      |> expect_collision_in_range(prefix, collision_data)

      layer = Directory.root(MockRepo)

      assert {:error, :prefix_collision} =
               Directory.create(layer, ["users"], prefix: prefix)
    end

    test "accepts manual prefix that doesn't collide" do
      prefix = <<10, 20>>
      packed_value = {prefix, ""}

      MockRepo
      |> expect_version_initialization()
      |> expect_directory_exists(["users"], nil)
      |> expect_collision_check_with_ancestors(prefix)
      |> expect_directory_creation(["users"], packed_value)

      layer = Directory.root(MockRepo)

      assert {:ok, %Directory.Node{prefix: ^prefix, path: ["users"]}} =
               Directory.create(layer, ["users"], prefix: prefix)
    end

    test "rejects reserved system prefixes" do
      reserved_prefixes = [<<0xFE>>, <<0xFF>>]

      for prefix <- reserved_prefixes do
        MockRepo
        |> expect_version_initialization()
        |> expect_directory_exists(["users"], nil)

        layer = Directory.root(MockRepo)

        assert {:error, :prefix_collision} =
                 Directory.create(layer, ["users"], prefix: prefix)
      end
    end

    test "detects when new prefix would be ancestor of existing key" do
      prefix = <<1, 2>>
      collision_data = [{<<1, 2, 3>>, "existing_data"}]

      MockRepo
      |> expect_version_initialization()
      |> expect_directory_exists(["users"], nil)
      |> expect_collision_in_range(prefix, collision_data)

      layer = Directory.root(MockRepo)

      assert {:error, :prefix_collision} =
               Directory.create(layer, ["users"], prefix: prefix)
    end

    test "detects when existing key would be ancestor of new prefix" do
      prefix = <<1, 2, 3>>

      # Custom expectation for ancestor collision check
      MockRepo
      |> expect_version_initialization()
      |> expect_directory_exists(["users"], nil)
      |> expect(:get_range, fn range, opts ->
        expected_range = Bedrock.KeyRange.from_prefix(prefix)
        assert expected_range == range
        assert opts[:limit] == 1
        []
      end)
      |> expect(:get, fn key ->
        cond do
          key == prefix -> nil
          # Ancestor has data
          key == <<1>> -> "existing_data"
          true -> raise "Unexpected key: #{inspect(key)}"
        end
      end)

      layer = Directory.root(MockRepo)

      assert {:error, :prefix_collision} =
               Directory.create(layer, ["users"], prefix: prefix)
    end
  end
end
