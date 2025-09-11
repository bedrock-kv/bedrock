defmodule Bedrock.Directory.RootRestrictionsTest do
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

  # Helper function specific to root restrictions tests
  defp expect_range_query(repo, path, results) do
    expected_range = Bedrock.KeyRange.from_prefix(build_directory_key(path))

    expect(repo, :range, fn :mock_txn, ^expected_range -> results end)
  end

  describe "root directory restrictions" do
    test "cannot perform restricted operations on root" do
      layer = Layer.new(MockRepo)

      restricted_operations = [
        {:open, fn -> Directory.open(layer, []) end},
        {:remove, fn -> Directory.remove(layer, []) end},
        {:move_from_root, fn -> Directory.move(layer, [], ["somewhere"]) end},
        {:move_to_root, fn -> Directory.move(layer, ["somewhere"], []) end},
        {:remove_if_exists, fn -> Directory.remove_if_exists(layer, []) end}
      ]

      expected_errors = [
        :cannot_open_root,
        :cannot_remove_root,
        :cannot_move_root,
        :cannot_move_to_root,
        :cannot_remove_root
      ]

      for {{_op, operation}, expected_error} <- Enum.zip(restricted_operations, expected_errors) do
        assert {:error, ^expected_error} = operation.()
      end
    end

    test "can create root directory" do
      prefix = <<0, 1>>

      MockRepo
      |> expect_version_initialization()
      |> expect_directory_exists([], nil)
      |> expect_directory_creation([], {prefix, ""})

      layer = Layer.new(MockRepo, next_prefix_fn: fn -> prefix end)

      assert {:ok, %{path: [], prefix: ^prefix}} = Directory.create(layer, [])
    end

    test "can check if root exists" do
      root_data = Bedrock.Key.pack({<<0, 1>>, ""})

      expect_directory_exists(MockRepo, [], root_data)
      layer = Layer.new(MockRepo)

      assert Directory.exists?(layer, []) == true
    end

    test "can list root directory children" do
      children_results = [
        {build_directory_key(["users"]), Bedrock.Key.pack({<<0, 2>>, ""})},
        {build_directory_key(["docs"]), Bedrock.Key.pack({<<0, 3>>, ""})}
      ]

      MockRepo
      |> expect_version_check_only()
      |> expect_range_query([], children_results)

      layer = Layer.new(MockRepo)

      assert {:ok, children} = Directory.list(layer, [])
      assert "users" in children
      assert "docs" in children
    end
  end

  describe "root? helper" do
    test "identifies root path correctly" do
      test_cases = [
        {[], true},
        {["users"], false},
        {["users", "profiles"], false},
        {nil, false},
        {"", false}
      ]

      for {path, expected} <- test_cases do
        assert Layer.root?(path) == expected
      end
    end
  end
end
